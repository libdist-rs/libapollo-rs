"""Fabric-based AWS provisioning + benchmark harness for libapollo-rs.

Design:
    * Provisioning uses boto3 (clean typed API, same creds as the aws CLI).
    * Remote orchestration uses Fabric 3 (paramiko-backed SSH).
    * State is persisted to `scripts/state/aws.json` across task invocations
      -- every task re-reads it, so instances survive across `fab` calls.
    * NO auto-teardown on error: if `install` or `run` fails, the instances
      stay up so you can SSH in, read logs, re-try. Teardown is explicit
      via `fab teardown`.

Usage:
    source scripts/venv/bin/activate
    cd scripts

    fab provision                  # create VPC, SG, 7 instances; ~3-5 min
    fab install                    # install Rust + build deps
    fab build                      # cargo build --release on node 0,
                                   # then scp binaries to peers
    fab configure                  # generate configs (real private IPs),
                                   # scp to nodes
    fab run --protocol artemis     # run one protocol sweep
    fab run --protocol all         # all four protocols
    fab logs --node 0              # tail a node's log
    fab ssh --node 0               # print the ssh command
    fab status                     # show what's up and what it costs
    fab teardown                   # destroy everything in aws.json

Resources created:
    * 1 VPC            (10.42.0.0/16)
    * 1 Subnet         (10.42.1.0/24) in the first AZ of `region`
    * 1 Internet GW + route
    * 1 Security Group (SSH from your public IP, full TCP intra-SG)
    * 1 Key Pair       (generated fresh; private key stored locally)
    * 7 EC2 instances  (c6g.large, Amazon Linux 2023 ARM, 16GB gp3)

Nothing is tagged beyond a `libapollo-bench` prefix, so `fab teardown`
discovers them from `aws.json`. If you lose `aws.json`, find resources
by the `libapollo-bench` name prefix in the AWS console.
"""

from __future__ import annotations

import datetime as _dt
import io
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError
from fabric import Connection
from invoke import task, Context

# ---------------------------------------------------------------------------
# Paths + constants

HERE = pathlib.Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
STATE_DIR = HERE / "state"
STATE_FILE = STATE_DIR / "aws.json"
KEY_FILE = STATE_DIR / "libapollo-bench.pem"

STATE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_REGION = "us-east-1"
DEFAULT_INSTANCE_TYPE = "c6g.large"   # ARM Graviton2, 2 vCPU, 4 GB RAM
DEFAULT_COUNT = 7
# Glob matching the latest Amazon Linux 2023 ARM AMI name. We used to
# resolve this via the SSM parameter store (`/aws/service/ami-amazon-
# linux-latest/...`), but that requires `ssm:GetParameter` which is
# not in the default EC2-admin IAM profile. `ec2:DescribeImages` is,
# so we sort by CreationDate instead.
DEFAULT_AMI_NAME_GLOB = "al2023-ami-2023.*-kernel-*-arm64"

# Rocksdb + the rest of the Rust build want clang + tool-chain bits.
# Kept in one string so we can curl|bash it on every node.
BOOTSTRAP = r"""
set -euxo pipefail
sudo dnf install -y --allowerasing \
    clang clang-devel cmake gcc gcc-c++ git llvm-devel openssl-devel \
    pkgconfig perl tar zip rsync
# Rustup to a specific version for reproducibility.
if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --default-toolchain stable --profile minimal
fi
source "$HOME/.cargo/env"
rustc --version
"""

# ---------------------------------------------------------------------------
# State helpers


def _load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    return json.loads(STATE_FILE.read_text())


def _save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def _boto(region: str | None = None):
    region = region or _load_state().get("region", DEFAULT_REGION)
    return boto3.Session(region_name=region), region


def _my_public_ip() -> str:
    """IP used for the SSH rule in the security group."""
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=5) as r:
        return r.read().decode().strip() + "/32"


def _tag_spec(prefix: str, *resource_types: str, extra: dict[str, str] | None = None):
    """Build a TagSpecifications list scoped to the given resource types.

    Each EC2 API call only accepts a specific subset of resource types
    in its TagSpecifications, so we can't pass a universal list --
    CreateVpc rejects `subnet`, RunInstances rejects `vpc`, etc.
    """
    tags = [{"Key": "Name", "Value": f"libapollo-bench-{prefix}"}]
    tags += [{"Key": k, "Value": v} for k, v in (extra or {}).items()]
    return [{"ResourceType": rt, "Tags": tags} for rt in resource_types]


# ---------------------------------------------------------------------------
# Fabric connection helpers


def _connections(state: dict[str, Any]) -> list[Connection]:
    """One fabric Connection per running instance, ordered by node id."""
    nodes = state.get("instances", [])
    if not nodes:
        raise RuntimeError("No instances in state. Run `fab provision` first.")
    return [
        Connection(
            host=n["public_ip"],
            user="ec2-user",
            connect_kwargs={"key_filename": str(KEY_FILE)},
        )
        for n in nodes
    ]


def _run_parallel(conns: list[Connection], cmd: str, warn: bool = False) -> list:
    """Run the same command on every connection. Returns list of Results.

    Fabric 3 dropped `ThreadingGroup.run` returning a mapping; emulate with
    a tiny threadpool so we get one Result per node in order.
    """
    import concurrent.futures as cf
    def _one(c):
        return c.run(cmd, warn=warn, hide=False)
    with cf.ThreadPoolExecutor(max_workers=len(conns)) as ex:
        return list(ex.map(_one, conns))


# ---------------------------------------------------------------------------
# Provisioning


@task
def provision(ctx: Context,
              count: int = DEFAULT_COUNT,
              instance_type: str = DEFAULT_INSTANCE_TYPE,
              region: str = DEFAULT_REGION):
    """Create VPC, SG, key pair, and launch N instances. Idempotent-ish: if
    state already lists running instances, refuses rather than doubling up."""
    state = _load_state()
    if state.get("instances"):
        print(f"state file already tracks {len(state['instances'])} instances.")
        print("Run `fab teardown` first if you want a fresh run.")
        return

    session, region = _boto(region)
    ec2 = session.client("ec2")

    # Timestamped prefix keeps multiple parallel benches separable in the
    # console, and scoped by calendar minute in case of retries.
    prefix = _dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    state["prefix"] = prefix
    state["region"] = region

    # --- AMI lookup (latest Amazon Linux 2023 ARM) -----------------------
    # DescribeImages instead of SSM GetParameter so this works under
    # an EC2-only IAM policy (no ssm:GetParameter required).
    print(f"[provision] looking up AMI via describe_images ({DEFAULT_AMI_NAME_GLOB})")
    imgs = ec2.describe_images(
        Owners=["amazon"],
        Filters=[
            {"Name": "name", "Values": [DEFAULT_AMI_NAME_GLOB]},
            {"Name": "state", "Values": ["available"]},
        ],
    )["Images"]
    if not imgs:
        raise RuntimeError(f"No AMI matched {DEFAULT_AMI_NAME_GLOB}")
    imgs.sort(key=lambda i: i["CreationDate"])
    ami_id = imgs[-1]["ImageId"]
    state["ami_id"] = ami_id
    print(f"           AMI = {ami_id}  ({imgs[-1]['Name']})")

    # --- Key pair --------------------------------------------------------
    key_name = f"libapollo-bench-{prefix}"
    print(f"[provision] creating key pair {key_name}")
    kp = ec2.create_key_pair(KeyName=key_name, KeyType="ed25519")
    KEY_FILE.write_text(kp["KeyMaterial"])
    os.chmod(KEY_FILE, 0o600)
    state["key_name"] = key_name
    state["key_file"] = str(KEY_FILE)
    _save_state(state)
    print(f"           private key -> {KEY_FILE}")

    # --- VPC + subnet + IGW + route --------------------------------------
    print("[provision] creating VPC 10.42.0.0/16")
    vpc = ec2.create_vpc(CidrBlock="10.42.0.0/16",
                         TagSpecifications=_tag_spec(prefix, "vpc"))["Vpc"]
    state["vpc_id"] = vpc["VpcId"]
    ec2.modify_vpc_attribute(VpcId=vpc["VpcId"], EnableDnsHostnames={"Value": True})
    _save_state(state)

    az = session.client("ec2").describe_availability_zones()["AvailabilityZones"][0]["ZoneName"]
    print(f"[provision] creating subnet in AZ {az}")
    subnet = ec2.create_subnet(VpcId=vpc["VpcId"], CidrBlock="10.42.1.0/24",
                               AvailabilityZone=az,
                               TagSpecifications=_tag_spec(prefix, "subnet"))["Subnet"]
    state["subnet_id"] = subnet["SubnetId"]
    state["az"] = az
    ec2.modify_subnet_attribute(SubnetId=subnet["SubnetId"],
                                MapPublicIpOnLaunch={"Value": True})

    igw = ec2.create_internet_gateway(
        TagSpecifications=_tag_spec(prefix, "internet-gateway"))["InternetGateway"]
    state["igw_id"] = igw["InternetGatewayId"]
    ec2.attach_internet_gateway(VpcId=vpc["VpcId"], InternetGatewayId=igw["InternetGatewayId"])

    rt = ec2.create_route_table(VpcId=vpc["VpcId"],
                                TagSpecifications=_tag_spec(prefix, "route-table"))["RouteTable"]
    state["rt_id"] = rt["RouteTableId"]
    ec2.create_route(RouteTableId=rt["RouteTableId"],
                     DestinationCidrBlock="0.0.0.0/0",
                     GatewayId=igw["InternetGatewayId"])
    ec2.associate_route_table(RouteTableId=rt["RouteTableId"],
                              SubnetId=subnet["SubnetId"])
    _save_state(state)

    # --- Security group --------------------------------------------------
    my_ip = _my_public_ip()
    print(f"[provision] creating security group; SSH open to {my_ip}")
    sg = ec2.create_security_group(
        GroupName=f"libapollo-bench-{prefix}",
        Description="libapollo-rs benchmark nodes",
        VpcId=vpc["VpcId"],
        TagSpecifications=_tag_spec(prefix, "security-group"))
    state["sg_id"] = sg["GroupId"]

    ec2.authorize_security_group_ingress(
        GroupId=sg["GroupId"],
        IpPermissions=[
            {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
             "IpRanges": [{"CidrIp": my_ip, "Description": "ssh-from-dev"}]},
            {"IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535,
             "UserIdGroupPairs": [{"GroupId": sg["GroupId"],
                                   "Description": "intra-sg"}]}])
    _save_state(state)

    # --- Instances -------------------------------------------------------
    print(f"[provision] launching {count} x {instance_type}")
    resp = ec2.run_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=count, MaxCount=count,
        NetworkInterfaces=[{
            "DeviceIndex": 0,
            "SubnetId": subnet["SubnetId"],
            "Groups": [sg["GroupId"]],
            "AssociatePublicIpAddress": True,
        }],
        BlockDeviceMappings=[{
            "DeviceName": "/dev/xvda",
            "Ebs": {"VolumeSize": 16, "VolumeType": "gp3",
                    "DeleteOnTermination": True},
        }],
        TagSpecifications=_tag_spec(prefix, "instance", "volume"))

    ids = [i["InstanceId"] for i in resp["Instances"]]
    state["instance_ids"] = ids
    _save_state(state)

    print(f"[provision] waiting for {count} instances to be running...")
    ec2.get_waiter("instance_running").wait(InstanceIds=ids)

    descs = ec2.describe_instances(InstanceIds=ids)
    instances = []
    node_id = 0
    for r in descs["Reservations"]:
        for i in r["Instances"]:
            instances.append({
                "node_id": node_id,
                "instance_id": i["InstanceId"],
                "public_ip": i["PublicIpAddress"],
                "private_ip": i["PrivateIpAddress"],
                "az": i["Placement"]["AvailabilityZone"],
            })
            node_id += 1
    instances.sort(key=lambda x: x["instance_id"])
    for n, inst in enumerate(instances):
        inst["node_id"] = n
    state["instances"] = instances
    _save_state(state)

    print("[provision] waiting for SSH on every instance...")
    _wait_for_ssh(instances)
    print("[provision] done.")
    status(ctx)


def _wait_for_ssh(instances, timeout_s: int = 300):
    import socket
    deadline = time.monotonic() + timeout_s
    pending = {i["public_ip"]: i for i in instances}
    while pending and time.monotonic() < deadline:
        for ip in list(pending):
            with socket.socket() as s:
                s.settimeout(2)
                try:
                    s.connect((ip, 22))
                    pending.pop(ip)
                except OSError:
                    pass
        if pending:
            time.sleep(3)
    if pending:
        raise RuntimeError(f"SSH timeout for: {list(pending)}")


# ---------------------------------------------------------------------------
# Deploy: install deps + build + distribute


@task
def install(ctx: Context):
    """Install Rust + build deps on every node in parallel."""
    state = _load_state()
    conns = _connections(state)
    print(f"[install] bootstrapping Rust + build deps on {len(conns)} nodes")
    results = _run_parallel(conns, BOOTSTRAP, warn=True)
    failed = [(c.host, r) for c, r in zip(conns, results) if r.exited != 0]
    if failed:
        print(f"[install] FAILED on {len(failed)} nodes; instances left up for debug.")
        for host, r in failed:
            print(f"  {host}: exit={r.exited}")
    else:
        print("[install] ok on all nodes.")


@task
def sync_src(ctx: Context):
    """rsync the repo to node 0. Excludes target/, scripts/venv/, .git/."""
    state = _load_state()
    conns = _connections(state)
    c0 = conns[0]
    print(f"[sync_src] rsync repo -> {c0.host}:libapollo-rs/")
    # Use the same key that fabric Connection uses for ssh options.
    subprocess.check_call([
        "rsync", "-az", "--delete",
        "-e", f"ssh -i {KEY_FILE} -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null",
        "--exclude", "target/", "--exclude", "scripts/venv/",
        "--exclude", "scripts/state/", "--exclude", ".git/",
        "--exclude", "stress-test/runs/",
        f"{REPO_ROOT}/", f"ec2-user@{c0.host}:libapollo-rs/",
    ])
    print("[sync_src] done")


@task
def build(ctx: Context):
    """Build release binaries on node 0, scp to other nodes."""
    state = _load_state()
    conns = _connections(state)
    c0 = conns[0]

    print("[build] cargo build --release on node 0 (this takes a while on c6g.large)")
    c0.run(
        "source $HOME/.cargo/env && cd libapollo-rs && "
        "cargo build --release --bin node-artemis --bin client-artemis "
        "--bin node-synchs --bin client-synchs "
        "--bin node-optsync --bin client-optsync "
        "--bin node-apollo --bin client-apollo "
        "--bin genconfig",
        hide=False,
    )

    # Fetch binaries from node 0, push to others.
    bin_names = [
        "node-artemis", "client-artemis",
        "node-synchs", "client-synchs",
        "node-optsync", "client-optsync",
        "node-apollo", "client-apollo",
        "genconfig",
    ]
    local_stage = STATE_DIR / "bin"
    local_stage.mkdir(exist_ok=True)
    for b in bin_names:
        c0.get(f"libapollo-rs/target/release/{b}", str(local_stage / b))

    print(f"[build] staged {len(bin_names)} binaries locally. Distributing to peers...")
    peers = conns[1:]

    def _push(c):
        c.run("mkdir -p libapollo-rs/target/release")
        for b in bin_names:
            c.put(str(local_stage / b), f"libapollo-rs/target/release/{b}")
        c.run("chmod +x libapollo-rs/target/release/*")

    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(max_workers=len(peers)) as ex:
        list(ex.map(_push, peers))
    print("[build] binaries distributed.")


# ---------------------------------------------------------------------------
# Configure + run


def _gen_local_configs(state: dict[str, Any], run_dir: pathlib.Path,
                       protocol: str, n: int, f: int, block_size: int,
                       base_port: int) -> dict[str, Any]:
    """Run genconfig locally (on a Mac binary OR on node 0 via ssh) with
    localhost addresses, then rewrite the generated JSONs to point at
    the real private IPs. Returns a dict of {node_id: config_path}.

    For simplicity we run genconfig on node 0 over ssh, using the same
    compiled binary the benchmark will run.
    """
    conns = _connections(state)
    c0 = conns[0]

    cli_base = base_port + 100
    mempool_base = base_port + 200
    client_listen = base_port + 275

    instances = state["instances"]

    # All peers' private IPs go into every node's cert SAN so TLS
    # handshakes between `10.x.y.z` peers pass rustls's SAN check.
    # The stress-test client lives on node 0, so its cert reuses node
    # 0's private IP as the sole client SAN.
    node_ip_csv = ",".join(i["private_ip"] for i in instances)
    client_ip_csv = instances[0]["private_ip"]

    remote_dir = f"bench/{run_dir.name}"
    c0.run(f"rm -rf {remote_dir} && mkdir -p {remote_dir}", hide=True)
    c0.run(
        f"libapollo-rs/target/release/genconfig "
        f"-n {n} -f {f} -d 50 --blocksize {block_size} "
        f"--base_port {base_port} --client_base_port {cli_base} "
        f"--mempool_base_port {mempool_base} "
        f"--client_listen_port {client_listen} "
        f"--node_ips {node_ip_csv} "
        f"--client_ips {client_ip_csv} "
        f"--payload 0 --target {remote_dir}",
        hide=True,
    )

    # Pull node json and cert files; rewrite IPs; push back to each node.
    local_dir = run_dir
    local_dir.mkdir(parents=True, exist_ok=True)

    instances = state["instances"]
    # Node i listens on its own private IP (well, 0.0.0.0 actually, but
    # the net_map advertises the private IP to peers).
    ip_file = "\n".join(f"{inst['private_ip']}:{base_port + i}"
                        for i, inst in enumerate(instances)) + "\n"
    cli_ip_file = "\n".join(f"{inst['private_ip']}:{cli_base + i}"
                            for i, inst in enumerate(instances)) + "\n"

    # We also need client.json to point at node 0's public IP for the
    # client listener. The stress-test's sole client listens on
    # client_listen_port for `ClientMsg` pushes; nodes are told about
    # the client via client_net_map inside nodes-{i}.json. We patch
    # those JSONs below.

    # Pull generated files from node 0 to local staging.
    for i in range(n):
        c0.get(f"{remote_dir}/nodes-{i}.json", str(local_dir / f"nodes-{i}.json"))
        c0.get(f"{remote_dir}/node-{i}.chain.pem", str(local_dir / f"node-{i}.chain.pem"))
        c0.get(f"{remote_dir}/node-{i}.key.pem", str(local_dir / f"node-{i}.key.pem"))
    c0.get(f"{remote_dir}/client.json", str(local_dir / "client.json"))
    c0.get(f"{remote_dir}/client-0.chain.pem", str(local_dir / "client-0.chain.pem"))
    c0.get(f"{remote_dir}/client-0.key.pem", str(local_dir / "client-0.key.pem"))

    # The client always runs on node 0 (so it's reachable by every other
    # node via the same private-IP fabric). Its listener port stays
    # client_listen_port.
    client_host = instances[0]["private_ip"]

    # Rewrite per-node configs:
    #   - net_map / mempool_net_map / client_net_map get real IPs,
    #   - cert paths are rewritten from genconfig's target dir to the
    #     fixed runtime location each node reads at start-up
    #     (`/home/ec2-user/bench/run/{node,root-cert}.*.pem`).
    runtime_cert = "/home/ec2-user/bench/run/node.chain.pem"
    runtime_key = "/home/ec2-user/bench/run/node.key.pem"
    runtime_root = "/home/ec2-user/bench/run/root-cert.pem"
    for i in range(n):
        p = local_dir / f"nodes-{i}.json"
        cfg = json.loads(p.read_text())
        # IPs
        for j in range(n):
            cfg["net_map"][str(j)] = f"{instances[j]['private_ip']}:{base_port + j}"
        if "mempool_net_map" in cfg:
            for j in range(n):
                cfg["mempool_net_map"][str(j)] = \
                    f"{instances[j]['private_ip']}:{mempool_base + j}"
        if "client_net_map" in cfg:
            cfg["client_net_map"] = {"0": f"{client_host}:{client_listen}"}
        # Cert paths
        cfg["my_cert_path"] = runtime_cert
        cfg["my_cert_key_path"] = runtime_key
        cfg["root_cert_path"] = runtime_root
        p.write_text(json.dumps(cfg) + "\n")

    # Rewrite client.json: net_map, my_listen_addr, and cert paths
    # to the runtime locations the remote host will have post-scp.
    p = local_dir / "client.json"
    cli = json.loads(p.read_text())
    for j in range(n):
        cli["net_map"][str(j)] = f"{instances[j]['private_ip']}:{cli_base + j}"
    cli["my_listen_addr"] = f"0.0.0.0:{client_listen}"
    cli["my_cert_path"] = "/home/ec2-user/bench/run/client.chain.pem"
    cli["my_cert_key_path"] = "/home/ec2-user/bench/run/client.key.pem"
    cli["root_cert_path"] = runtime_root
    p.write_text(json.dumps(cli) + "\n")

    # Also write ip_file / cli_ip_file with real IPs for `--ip` arg.
    (local_dir / "ip_file").write_text(ip_file)
    (local_dir / "cli_ip_file").write_text(cli_ip_file)

    return {
        "run_dir": str(local_dir),
        "base_port": base_port,
        "cli_base": cli_base,
        "mempool_base": mempool_base,
        "client_listen": client_listen,
        "client_node": 0,
    }


@task
def configure(ctx: Context,
              protocol: str = "artemis",
              n: int = DEFAULT_COUNT,
              f: int = 3,
              block_size: int = 400,
              base_port: int = 31000):
    """Generate configs with real private IPs and distribute to nodes."""
    state = _load_state()
    conns = _connections(state)
    if len(conns) < n:
        raise RuntimeError(f"Need {n} instances, have {len(conns)}")

    stamp = _dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    run_dir = STATE_DIR / "runs" / f"{protocol}-n{n}-{stamp}"
    info = _gen_local_configs(state, run_dir, protocol, n, f, block_size, base_port)

    # Pull the generated root-cert.pem so we can push it alongside each
    # node's identity cert -- it's the shared trust store.
    c0 = conns[0]
    c0.get(f"bench/{run_dir.name}/root-cert.pem", str(run_dir / "root-cert.pem"))

    # Push each node its own config + certs (bundled into bench/run/).
    import concurrent.futures as cf
    def _push(c, node_id):
        c.run(f"mkdir -p bench/run", hide=True)
        c.put(str(run_dir / f"nodes-{node_id}.json"), "bench/run/nodes.json")
        c.put(str(run_dir / f"node-{node_id}.chain.pem"), "bench/run/node.chain.pem")
        c.put(str(run_dir / f"node-{node_id}.key.pem"), "bench/run/node.key.pem")
        c.put(str(run_dir / "root-cert.pem"), "bench/run/root-cert.pem")
        c.put(str(run_dir / "ip_file"), "bench/run/ip_file")
        c.put(str(run_dir / "cli_ip_file"), "bench/run/cli_ip_file")

    with cf.ThreadPoolExecutor(max_workers=n) as ex:
        list(ex.map(lambda t: _push(*t), [(conns[i], i) for i in range(n)]))

    # Client runs on node 0. Push client config + certs there.
    conns[info["client_node"]].put(str(run_dir / "client.json"), "bench/run/client.json")
    conns[info["client_node"]].put(str(run_dir / "client-0.chain.pem"),
                                   "bench/run/client.chain.pem")
    conns[info["client_node"]].put(str(run_dir / "client-0.key.pem"),
                                   "bench/run/client.key.pem")

    # Patch the chain_path / key_path inside the JSON? The node config
    # refers to paths by absolute location at gen time. Inspect one to
    # decide what to fix.
    # TODO(adhara): verify this is needed by looking at one of the
    # uploaded nodes.json and adjusting my_cert_path / cert_chain_path.
    print(f"[configure] run staged at {run_dir}")
    _save_state({**state, "run": info})


@task
def run(ctx: Context, protocol: str = "artemis",
        total_txs: int = 50000, window: int = 10000,
        bootstrap_secs: int = 12, delta: int = 50):
    """Launch nodes in tmux, then client, collect throughput + latency.

    Each node runs inside a detached tmux session named `node`. The
    session survives SSH disconnect and can be re-attached via
    `ssh ...; tmux attach -t node`. The log tee'd into `node.log`
    gives fab-logs-style access too.
    """
    state = _load_state()
    info = state.get("run")
    if not info:
        raise RuntimeError("Run `fab configure` first.")

    conns = _connections(state)
    n = len(state["instances"])

    # Kill any prior tmux session + stale rocksdb lock before relaunch.
    print("[run] killing any prior node tmux + rocksdb lock...")
    _run_parallel(
        conns,
        "tmux kill-session -t node 2>/dev/null || true; "
        "tmux kill-session -t client 2>/dev/null || true; "
        f"pkill -9 -f node-{protocol} 2>/dev/null || true; "
        "rm -rf bench/run/node-*.rocksdb",
        warn=True,
    )

    # --- Launch nodes ----------------------------------------------------
    print(f"[run] launching node-{protocol} in tmux on {n} hosts...")
    special = "-s" if protocol in ("artemis", "apollo") else ""
    def _launch(ci):
        c, _i = ci
        # `tmux new-session -d` starts detached; the shell-c command runs
        # inside a clean shell scope. Pipes are fine inside tmux so we
        # keep the `2>&1 | tee` to capture the same log fab-logs reads.
        cmd = (
            f'cd $HOME && tmux new-session -d -s node -- bash -c '
            f'"RUST_LOG=info libapollo-rs/target/release/node-{protocol} '
            f'-c bench/run/nodes.json -i bench/run/ip_file '
            f'--sleep {bootstrap_secs} --delta {delta} {special} '
            f'2>&1 | tee bench/run/node.log"'
        )
        c.run(cmd, pty=False, hide=True)
    import concurrent.futures as cf
    t0 = time.monotonic()
    with cf.ThreadPoolExecutor(max_workers=n) as ex:
        list(ex.map(_launch, [(conns[i], i) for i in range(n)]))
    print(f"       all launched in {time.monotonic()-t0:.1f}s")

    # --- Launch client on node 0 -----------------------------------------
    print(f"[run] waiting {bootstrap_secs}s for nodes to bind...")
    time.sleep(bootstrap_secs + 3)       # small margin for tcp accept
    c0 = conns[info["client_node"]]
    print(f"[run] running client on node 0 (tx={total_txs}, window={window})...")
    t0 = time.monotonic()
    r = c0.run(
        f"cd $HOME && RUST_LOG=info "
        f"libapollo-rs/target/release/client-{protocol} "
        f"-c bench/run/client.json -i bench/run/cli_ip_file "
        f"-m {total_txs} -w {window} 2>&1 | tee bench/run/client.log",
        hide=False, warn=True,
    )
    print(f"       client finished in {time.monotonic()-t0:.1f}s (exit={r.exited})")

    # Parse throughput + latency from DP[...] lines.
    throughput = latency = None
    for line in r.stdout.splitlines():
        m = re.search(r"DP\[Throughput\]:\s*([\d.]+)", line)
        if m: throughput = float(m.group(1))
        m = re.search(r"DP\[Latency\]:\s*([\d.]+)", line)
        if m: latency = float(m.group(1))

    print()
    if throughput is not None and latency is not None:
        print(f"[run] {protocol} n={n} | throughput = {throughput:.2f} tx/s "
              f"| latency = {latency:.2f} ms")
    else:
        print("[run] client did not print DP stats. Check bench/run/client.log on node 0.")
    print("[run] nodes still running (tmux session `node`). `fab stop` to kill.")


@task
def bench(ctx: Context,
          protocols: str = "artemis,apollo,synchs,optsync",
          configs: str = "3:1,7:3",
          runs: int = 3,
          block_size: int = 400,
          total_txs: int = 50000,
          window: int = 10000,
          bootstrap_secs: int = 12,
          delta: int = 50,
          tag: str | None = None):
    """Full benchmark sweep: every protocol x every (n,f) x `runs` runs.

    Writes one timestamped tree under `state/results/<stamp>/`:
        manifest.json            -- config (block_size, runs, env, ...)
        <proto>-n<n>-f<f>/
            run-0/client.log     -- raw client stdout/stderr
            run-0/throughput_ms.json  -- parsed {throughput_tx_s, latency_ms}
            run-1/...
            ...

    `fab plot` reads the same directory back and renders PNGs. Keeping
    the raw logs alongside the parsed numbers means anyone can
    re-verify the DP[Throughput]/DP[Latency] we plotted.

    Args:
        protocols: comma-separated protocol names.
        configs:   comma-separated "n:f" pairs. Each config triggers a
                   fresh `fab configure` before its runs.
        runs:      number of runs per (protocol, config) cell.
        tag:       optional label appended to the results dir name.
    """
    state = _load_state()
    conns = _connections(state)

    stamp = _dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    suffix = f"-{tag}" if tag else ""
    results_root = STATE_DIR / "results" / f"{stamp}{suffix}"
    results_root.mkdir(parents=True, exist_ok=True)

    config_pairs = []
    for spec in configs.split(","):
        n_str, f_str = spec.strip().split(":")
        config_pairs.append((int(n_str), int(f_str)))

    protocols_list = [p.strip() for p in protocols.split(",") if p.strip()]

    manifest = {
        "stamp": stamp,
        "tag": tag,
        "block_size": block_size,
        "total_txs": total_txs,
        "window": window,
        "bootstrap_secs": bootstrap_secs,
        "delta": delta,
        "runs_per_cell": runs,
        "protocols": protocols_list,
        "configs": [f"n={n}/f={f}" for n, f in config_pairs],
        "env": {
            "region": state.get("region"),
            "az": state.get("az"),
            "instance_type": "c6g.large",
            "instance_count": len(state.get("instances", [])),
            "ami": state.get("ami_id"),
        },
    }
    (results_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    for (n, f) in config_pairs:
        # One `configure` per (n,f) -- the cert SAN list is the same
        # across protocols, so we reuse the same genconfig output for
        # every protocol at this (n,f).
        print(f"\n[bench] configure n={n} f={f}")
        configure(ctx, protocol="artemis", n=n, f=f, block_size=block_size)

        for proto in protocols_list:
            cell_dir = results_root / f"{proto}-n{n}-f{f}"
            cell_dir.mkdir(parents=True, exist_ok=True)
            print(f"\n[bench] {proto} n={n} f={f}  ({runs} runs)")
            for r in range(runs):
                run_dir = cell_dir / f"run-{r}"
                run_dir.mkdir(exist_ok=True)

                # Launch-via-tmux + run client synchronously: _run_one
                # is a small helper that returns (throughput, latency,
                # raw_client_log). We'd like to keep it close to the
                # existing `run` task's body; call it inline to keep a
                # single shape.
                tp, lat, raw = _run_one(
                    conns, proto, n,
                    total_txs=total_txs, window=window,
                    bootstrap_secs=bootstrap_secs, delta=delta,
                )

                (run_dir / "client.log").write_text(raw)
                (run_dir / "throughput_ms.json").write_text(json.dumps({
                    "protocol": proto, "n": n, "f": f, "run": r,
                    "throughput_tx_s": tp,
                    "latency_ms": lat,
                }, indent=2) + "\n")
                print(f"       run {r}: {tp:.0f} tx/s  |  {lat:.1f} ms"
                      if tp is not None
                      else f"       run {r}: FAILED")

    # Drop a pointer to the latest sweep so `fab plot` can find it
    # without an arg.
    (STATE_DIR / "results" / "latest").write_text(str(results_root.name) + "\n")
    print(f"\n[bench] done. results at {results_root}")


def _run_one(conns, protocol, n, *, total_txs, window, bootstrap_secs, delta):
    """Core launch/run/collect for one measurement. Returns
    (throughput or None, latency or None, raw client.log text)."""
    # Kill stragglers + wipe rocksdb locks (first n of N provisioned
    # instances are the active ones; others are idle at this config).
    _run_parallel(
        conns,
        "tmux kill-session -t node 2>/dev/null || true; "
        "tmux kill-session -t client 2>/dev/null || true; "
        f"pkill -9 -f node-{protocol} 2>/dev/null || true; "
        "rm -rf bench/run/node-*.rocksdb",
        warn=True,
    )

    # Launch node-<protocol> on the active N instances only.
    special = "-s" if protocol in ("artemis", "apollo") else ""
    active = conns[:n]
    def _launch(c):
        cmd = (
            f'cd $HOME && tmux new-session -d -s node -- bash -c '
            f'"RUST_LOG=info libapollo-rs/target/release/node-{protocol} '
            f'-c bench/run/nodes.json -i bench/run/ip_file '
            f'--sleep {bootstrap_secs} --delta {delta} {special} '
            f'2>&1 | tee bench/run/node.log"'
        )
        c.run(cmd, pty=False, hide=True)
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(max_workers=len(active)) as ex:
        list(ex.map(_launch, active))

    time.sleep(bootstrap_secs + 3)

    # Client on node 0; capture full stdout for the log archive.
    c0 = active[0]
    r = c0.run(
        f"cd $HOME && RUST_LOG=info "
        f"libapollo-rs/target/release/client-{protocol} "
        f"-c bench/run/client.json -i bench/run/cli_ip_file "
        f"-m {total_txs} -w {window}",
        hide=True, warn=True,
    )
    raw = (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")

    tp = lat = None
    for line in raw.splitlines():
        m = re.search(r"DP\[Throughput\]:\s*([\d.]+)", line)
        if m: tp = float(m.group(1))
        m = re.search(r"DP\[Latency\]:\s*([\d.]+)", line)
        if m: lat = float(m.group(1))
    return tp, lat, raw


@task
def plot(ctx: Context, results: str | None = None,
         out_dir: str = "../benchmarks"):
    """Parse raw client.logs from a `fab bench` sweep and render PNGs.

    Reads each `throughput_ms.json` under the sweep's cell directories
    (falling back to re-parsing `client.log` if the JSON is missing),
    aggregates across runs, and writes three PNGs plus a CSV summary to
    `out_dir` (default `benchmarks/` in the repo root).

    Args:
        results: specific sweep directory name under `state/results/`;
                 defaults to the contents of `state/results/latest`.
        out_dir: path where plots + summary CSV land.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    if results is None:
        latest_f = STATE_DIR / "results" / "latest"
        if not latest_f.exists():
            raise RuntimeError("No `state/results/latest` pointer. Run `fab bench` first.")
        results = latest_f.read_text().strip()

    results_dir = STATE_DIR / "results" / results
    if not results_dir.is_dir():
        raise RuntimeError(f"Results dir not found: {results_dir}")

    manifest = json.loads((results_dir / "manifest.json").read_text())
    protocols = manifest["protocols"]
    configs = manifest["configs"]       # e.g. ['n=3/f=1', 'n=7/f=3']

    # Aggregate: {(proto, config_label): {tx_s: [runs...], ms: [runs...]}}
    agg: dict[tuple[str, str], dict[str, list[float]]] = {}
    for cell in sorted(results_dir.iterdir()):
        if not cell.is_dir() or "-n" not in cell.name:
            continue
        # cell.name == "<proto>-n<n>-f<f>"
        proto, nf = cell.name.split("-n", 1)
        n_str, f_str = nf.split("-f", 1)
        label = f"n={n_str}/f={f_str}"
        key = (proto, label)
        agg.setdefault(key, {"tx_s": [], "ms": []})
        for run_sub in sorted(cell.iterdir()):
            jp = run_sub / "throughput_ms.json"
            if jp.exists():
                d = json.loads(jp.read_text())
                if d.get("throughput_tx_s") is not None:
                    agg[key]["tx_s"].append(d["throughput_tx_s"])
                if d.get("latency_ms") is not None:
                    agg[key]["ms"].append(d["latency_ms"])

    # `out_dir` is resolved relative to the scripts/ directory -- so the
    # default `../benchmarks` lands in the repo root's `benchmarks/`.
    out_path = pathlib.Path(out_dir)
    if not out_path.is_absolute():
        out_path = (HERE / out_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    # --- CSV summary ------------------------------------------------------
    csv_lines = ["protocol,config,runs,throughput_min,throughput_median,throughput_max,latency_min,latency_median,latency_max"]
    def _stats(xs):
        xs = sorted(xs)
        if not xs: return (0, 0, 0)
        return (xs[0], xs[len(xs)//2], xs[-1])
    for (proto, label), d in sorted(agg.items()):
        tp = _stats(d["tx_s"])
        la = _stats(d["ms"])
        csv_lines.append(f"{proto},{label},{len(d['tx_s'])},"
                         f"{tp[0]:.2f},{tp[1]:.2f},{tp[2]:.2f},"
                         f"{la[0]:.2f},{la[1]:.2f},{la[2]:.2f}")
    (out_path / "summary.csv").write_text("\n".join(csv_lines) + "\n")

    # --- Plots ------------------------------------------------------------
    # Consistent colour mapping per protocol; sorted for legend stability.
    palette = {
        "apollo":  "#4c72b0",
        "artemis": "#dd8452",
        "synchs":  "#55a467",
        "optsync": "#8172b3",
    }
    proto_order = [p for p in ["apollo", "artemis", "synchs", "optsync"] if p in protocols]

    def _grouped_bar(metric: str, ylabel: str, fname: str, logy: bool):
        """Grouped bar chart: x=config, groups=proto, bar=median, errorbars=min/max."""
        fig, ax = plt.subplots(figsize=(8, 5), dpi=160)
        n_groups = len(configs)
        bar_w = 0.8 / max(1, len(proto_order))
        x = np.arange(n_groups)

        for idx, proto in enumerate(proto_order):
            medians = []
            errs_low = []
            errs_high = []
            for cfg in configs:
                vals = agg.get((proto, cfg), {}).get(metric, [])
                if vals:
                    vals_s = sorted(vals)
                    med = vals_s[len(vals_s)//2]
                    medians.append(med)
                    errs_low.append(med - vals_s[0])
                    errs_high.append(vals_s[-1] - med)
                else:
                    medians.append(0)
                    errs_low.append(0)
                    errs_high.append(0)
            offset = (idx - (len(proto_order) - 1) / 2) * bar_w
            ax.bar(x + offset, medians, bar_w, color=palette.get(proto, "gray"),
                   label=proto, yerr=[errs_low, errs_high], capsize=3,
                   error_kw={"alpha": 0.7, "linewidth": 1})

        ax.set_xticks(x)
        ax.set_xticklabels(configs)
        ax.set_xlabel("cluster size")
        ax.set_ylabel(ylabel)
        ax.set_title(
            f"libapollo-rs on AWS c6g.large ({manifest['env'].get('region', '?')})\n"
            f"block_size={manifest['block_size']}, window={manifest['window']}, "
            f"runs={manifest['runs_per_cell']} per cell, error bars = min/max"
        )
        if logy:
            ax.set_yscale("log")
        ax.legend(frameon=False)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        fig.savefig(out_path / fname)
        plt.close(fig)

    _grouped_bar("tx_s", "throughput (tx/s)",   "throughput.png", logy=False)
    _grouped_bar("ms",   "latency (ms)",        "latency.png",    logy=False)

    # Copy the manifest so the benchmarks/ folder is self-documenting.
    import shutil
    shutil.copy(results_dir / "manifest.json", out_path / "manifest.json")

    print(f"[plot] wrote {out_path}/{{throughput,latency}}.png + summary.csv + manifest.json")


@task
def stop(ctx: Context):
    """Kill any running node / client tmux session on every host."""
    state = _load_state()
    conns = _connections(state)
    _run_parallel(
        conns,
        "tmux kill-session -t node 2>/dev/null || true; "
        "tmux kill-session -t client 2>/dev/null || true; "
        "pkill -f libapollo-rs/target/release/ 2>/dev/null || true",
        warn=True,
    )


# ---------------------------------------------------------------------------
# Debug helpers


@task
def logs(ctx: Context, node: int = 0, tail: int = 200):
    """Print the last N lines of a node's log."""
    state = _load_state()
    conns = _connections(state)
    conns[node].run(f"tail -n {tail} bench/run/node.log || tail -n {tail} bench/run/client.log",
                    warn=True)


@task
def ssh(ctx: Context, node: int = 0):
    """Print the ssh command to reach a node."""
    state = _load_state()
    inst = state["instances"][node]
    print(f"ssh -i {KEY_FILE} ec2-user@{inst['public_ip']}")
    print(f"# private ip: {inst['private_ip']}  instance: {inst['instance_id']}")


@task
def status(ctx: Context):
    """Show state + rough hourly cost estimate."""
    state = _load_state()
    if not state.get("instances"):
        print("No instances in state. Nothing running (as far as we know).")
        return
    insts = state["instances"]
    print(f"Region: {state.get('region')}  AZ: {state.get('az')}")
    print(f"Prefix: libapollo-bench-{state.get('prefix')}")
    print(f"Instances: {len(insts)}")
    for i in insts:
        print(f"  node {i['node_id']}: {i['instance_id']}"
              f"  public={i['public_ip']}  private={i['private_ip']}")
    hourly = len(insts) * 0.068  # c6g.large us-east-1 on-demand
    print(f"Estimated on-demand cost: ~${hourly:.3f}/hr "
          f"({len(insts)} x c6g.large @ $0.068/hr)")


# ---------------------------------------------------------------------------
# Teardown


@task
def teardown(ctx: Context, force: bool = False):
    """Destroy all resources tracked in aws.json."""
    state = _load_state()
    if not state:
        print("Nothing to tear down.")
        return
    if not force:
        ans = input(f"Terminate {len(state.get('instances', []))} instances "
                    f"+ VPC + SG in {state.get('region')}? [y/N]: ")
        if ans.lower() not in ("y", "yes"):
            print("abort.")
            return

    session, region = _boto(state.get("region"))
    ec2 = session.client("ec2")

    ids = state.get("instance_ids") or [i["instance_id"] for i in state.get("instances", [])]
    if ids:
        print(f"[teardown] terminating {len(ids)} instances...")
        ec2.terminate_instances(InstanceIds=ids)
        ec2.get_waiter("instance_terminated").wait(InstanceIds=ids)

    # Ordering matters: dependents before their dependencies. AWS
    # refuses to delete:
    #   * a route table that's still associated with a subnet,
    #   * a subnet whose VPC still has an attached IGW with traffic,
    #   * a VPC with any lingering subnet / RT / SG / IGW / ENI.
    # So: SG -> subnet -> IGW (detach+delete) -> RT -> VPC.
    for key, caller in [
        ("sg_id", lambda i: ec2.delete_security_group(GroupId=i)),
        ("subnet_id", lambda i: ec2.delete_subnet(SubnetId=i)),
        ("igw_id", lambda i: (ec2.detach_internet_gateway(InternetGatewayId=i,
                                                          VpcId=state["vpc_id"]),
                              ec2.delete_internet_gateway(InternetGatewayId=i))),
        ("rt_id", lambda i: ec2.delete_route_table(RouteTableId=i)),
        ("vpc_id", lambda i: ec2.delete_vpc(VpcId=i)),
    ]:
        rid = state.get(key)
        if not rid: continue
        try:
            print(f"[teardown] deleting {key} = {rid}")
            caller(rid)
        except ClientError as e:
            print(f"  -> {e}")

    if state.get("key_name"):
        try:
            ec2.delete_key_pair(KeyName=state["key_name"])
            print(f"[teardown] deleted key pair {state['key_name']}")
        except ClientError as e:
            print(f"  -> {e}")

    # Move the state file aside rather than deleting, so we retain a
    # record of what existed.
    if STATE_FILE.exists():
        archive = STATE_DIR / f"aws.{state.get('prefix','unknown')}.json"
        shutil.move(str(STATE_FILE), str(archive))
        print(f"[teardown] archived state -> {archive}")
    if KEY_FILE.exists():
        archive = STATE_DIR / f"libapollo-bench.{state.get('prefix','unknown')}.pem"
        shutil.move(str(KEY_FILE), str(archive))
        print(f"[teardown] archived key -> {archive}")
