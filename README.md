# libapollo-rs

Rust implementations of four BFT consensus protocols:

| Crate               | Protocol         | Reference                                             |
| ------------------- | ---------------- | ----------------------------------------------------- |
| `consensus/apollo`  | Apollo           | [FC 2023 paper](https://github.com/libdist-rs/libchatter-rs/releases/tag/apollo-fc2023-artifact) |
| `consensus/artemis` | Artemis          |                                                       |
| `consensus/synchs`  | Sync HotStuff    |                                                       |
| `consensus/optsync` | Opt Sync         |                                                       |

This repository is the active-development home for these protocols. The
frozen FC 2023 artifact (same protocols, older dependencies) lives at
[`libdist-rs/libchatter-rs`](https://github.com/libdist-rs/libchatter-rs)
tag `apollo-fc2023-artifact`.

## Status

Migration from libchatter-rs onto the modern successor crates is
complete:

- [x] Import from `libchatter-rs@apollo-fc2023-artifact`
- [x] [`libcrypto-rs`](https://github.com/libdist-rs/libcrypto-rs) -- replaces in-tree `crypto/` (hashes, keypairs, typed `Hash<T>`)
- [x] [`libnet-rs`](https://github.com/libdist-rs/libnet-rs) -- replaces in-tree `net/` (TLS + TCP transports)
- [x] In-tree [`mempool/`](mempool/) crate (`libapollo-mempool`) -- replaces libmempool-rs; purpose-built for the leader-inlines-batch pattern, with cached batch hashes, Arc-shared batches, and no redundant rocksdb round-trip on the leader's propose path
- [x] [`libstorage-rs`](https://github.com/libdist-rs/libstorage-rs) -- rocksdb batch store shared between mempool + consensus

Every migration step is a focused commit validated end-to-end by
the stress-test harness. [`baseline_results.txt`](baseline_results.txt)
captures the reference numbers from each milestone.

## Repository layout

```
consensus/
  apollo/      Apollo (FC 2023)   -- chain commit, rotating leader, 1-message-per-round
  artemis/     Artemis            -- UCR chained voting, 1-hop commit, rotating round leader
  synchs/      Sync HotStuff      -- 2Δ timer-based commit, view-based leader
  optsync/     Opt Sync           -- Sync HotStuff + optimistic responsive fast path
mempool/       libapollo-mempool  -- batching, Arc<CachedBatch>, rocksdb persist
types/         wire types: blocks, transactions, proto/client msgs
config/        node + client config (JSON/bincode/yaml)
stress-test/   the canonical benchmark harness + a few ad-hoc perf tools
tools/
  genconfig/   config + TLS cert generator
examples/
  <protocol>/node      node binary for each protocol
  <protocol>/client    stress-test client for each protocol
scripts/       Fabric + boto3 harness for multi-VM AWS runs (see below)
benchmarks/    rendered plots + summary CSV from the latest sweep
```

## Building

```sh
cargo build --release
```

Requires a recent stable Rust toolchain, `clang`, `cmake`, and the usual
openssl/rocksdb build deps. On Amazon Linux 2023 ARM the full list is
in [`scripts/fabfile.py`](scripts/fabfile.py) under `BOOTSTRAP`.

## Running the loopback stress test

```sh
./target/release/stress-test
```

Spawns N-node clusters for each of the four protocols on loopback,
drives a client load (50k transactions, window=10k, block_size=400 by
default), and prints throughput + latency. Filter to a single protocol
via `PROTO=<artemis|apollo|synchs|optsync>`.

[`baseline_results.txt`](baseline_results.txt) records the canonical
loopback numbers per architectural milestone. Note that loopback
throughput is a *poor proxy for real performance* on a shared
machine -- see [Multi-VM benchmarks](#multi-vm-benchmarks-aws) below.

## Local performance tooling

Two small harnesses under `stress-test/` go deeper than the stress-test
harness on one protocol at a time:

### `bench-artemis-metrics.sh`

Runs Artemis `n=7/f=3` locally, SIGTERMs each node when the client
finishes, and prints the node's in-memory metrics snapshot. Each
consensus event (`propose`, `vote`, `round_advance`, `batch_recv`,
`reactor_iter`) is counted atomically with inter-event histograms --
near-zero overhead on the hot path, unlike `log::info!` tracing.

```sh
cargo build --release
stress-test/bench-artemis-metrics.sh
```

Use it to pin down *which* reactor event is stalling when throughput is
below what you expect. Capped at `TOKIO_WORKER_THREADS=2` per node by
default to reduce contention on the local multi-process loopback
benchmark (override with the env var).

### `profile-artemis.sh` (samply)

Runs Artemis `n=7/f=3` with one node wrapped in
[samply](https://github.com/mstange/samply)'s sampling CPU profiler.
`samply load <profile.json.gz>` renders the flamegraph in a local
browser session.

```sh
brew install samply
cargo build --profile profiling --bin node-artemis \
            --bin client-artemis --bin genconfig
stress-test/profile-artemis.sh
samply load stress-test/runs/artemis-profile-*/node-0.samply.json.gz
```

The `profiling` cargo profile preserves debug info so samply's
`--unstable-presymbolicate` emits a sidecar with fully symbolicated
function names.

## Multi-VM benchmarks (AWS)

The loopback stress-test is a poor proxy for real performance: N node
processes contend for one machine's CPU, and the throughput ceiling on
my M1 Pro sits at ~15-40 k tx/s depending on protocol. Moving the same
binaries onto 7 × `c6g.large` (one node per VM) lifts that by 2-8×.
Artemis specifically — which is *latency-pipelined* — benefits the most:

![Throughput, n=3/f=1 vs n=7/f=3 on AWS c6g.large](benchmarks/throughput.png)

![Latency, n=3/f=1 vs n=7/f=3 on AWS c6g.large](benchmarks/latency.png)

| protocol | n=3/f=1 (tx/s) | n=3/f=1 (ms) | n=7/f=3 (tx/s) | n=7/f=3 (ms) |
| --- | ---: | ---: | ---: | ---: |
| **Artemis** | **141 k** | **33** | **115 k** | **64** |
| Opt Sync | 104 k | 77 | 86 k | 92 |
| Apollo | 99 k | 54 | 36 k | 116 |
| Sync HotStuff | 64 k | 130 | 63 k | 136 |

Medians across 3 runs per cell; error bars on the plots show min/max.
Raw DP[Throughput] / DP[Latency] lines from every client run are
preserved under `scripts/state/results/<stamp>/` for audit.

The numbers land where the papers predict: Artemis's UCR chained-voting
has the fewest round-trips per commit, so on a real network it beats
Opt Sync (which was ahead on loopback, because Opt Sync is timer-driven
and less sensitive to per-syscall overhead).

### Reproducing the benchmark

Prerequisites:

* AWS account with EC2 access (`ec2:RunInstances`, `ec2:DescribeImages`,
  SG/VPC/keypair create/delete). No `ssm:GetParameter` needed.
* `aws` CLI authenticated in the target region.
* Python 3.11+, `rsync`, `ssh`.

Steps (from the repo root):

```sh
# One-time setup
python3 -m venv scripts/venv
scripts/venv/bin/pip install -r scripts/requirements.txt
cd scripts
source venv/bin/activate

# Provision 7 × c6g.large in us-east-1a. Writes key + instance IDs
# into `scripts/state/aws.json`. ~3-5 min. $0.48/hr from here.
fab provision

# Install Rust + build deps (clang, cmake, rocksdb, ...) in parallel.
fab install

# Sync the repo to node 0 and build the release binaries there;
# distribute them to the other 6. Takes ~15 min on c6g.large.
fab sync-src
fab build

# Full sweep: every (protocol, n:f) × runs times. Writes client.logs
# + parsed {throughput, latency} JSON under
# `scripts/state/results/<timestamp>-<tag>/`.
fab bench --runs 3 --configs 3:1,7:3 --tag main

# Parse the raw logs and render PNGs + summary CSV into `benchmarks/`.
fab plot

# Tear down all AWS resources tracked in state/aws.json.
fab teardown
```

Each command is idempotent-ish (rerunning `fab build` is a cargo
incremental build, `fab bench` writes to a new timestamped subdir).
Leaving a cell failed mid-sweep doesn't trigger auto-teardown —
the script explicitly preserves state so you can SSH in and debug,
then `fab teardown` when you're done.

Budget for a clean full sweep: ~60-90 minutes wall-clock, ~$0.50-$0.80.

### Adapting to your own workload

* **Different instance type or region:**
  `fab provision --instance-type m7g.medium` (ARM/Graviton only --
  the default AMI is `al2023-ami-*-arm64`; to use an x86 instance,
  swap `DEFAULT_AMI_NAME_GLOB` in [`fabfile.py`](scripts/fabfile.py)
  for an x86_64 glob). `fab provision --region us-west-2` for a
  different region.
* **Different block / workload size:** `fab bench --block-size 1000
  --total-txs 200000 --window 20000 ...`.
* **Different sweep shape:** `fab bench --protocols artemis,optsync
  --configs 3:1,7:3,15:7 --runs 5`.

### Debugging mid-sweep

The harness is designed to hand over control when something goes
sideways:

```sh
fab status          # list instances + estimated hourly cost
fab ssh --node 0    # print the ssh command (manually attach a shell)
fab logs --node 0   # tail `bench/run/node.log` from the remote host
fab stop            # kill the tmux session on every node, leave instances up
fab teardown        # destroy everything when you're done
```

Each task is re-runnable on its own; `fab -l` lists them and
`fab <task> --help` prints per-option flags.

## `benchmarks/` contents

Every successful `fab plot` produces:

* `throughput.png`, `latency.png` -- grouped bar charts, one bar per
  protocol per config, error bars = min/max across runs
* `summary.csv` -- one row per (protocol, config) with min / median /
  max for both metrics
* `manifest.json` -- the sweep parameters (block size, window, number
  of runs, AMI, region, instance count)

Every *raw* run is preserved under
`scripts/state/results/<stamp>-<tag>/<proto>-n<n>-f<f>/run-<r>/`:
`client.log` (full stdout+stderr including DP lines) and a parsed
`throughput_ms.json`. `fab plot` can re-render from any prior sweep
via `fab plot --results <stamp>-<tag>`.
