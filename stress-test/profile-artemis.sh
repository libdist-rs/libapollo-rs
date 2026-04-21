#!/usr/bin/env bash
# Profile Artemis 7/3 under samply.
#
# Mirrors the stress-test harness (same genconfig args, same node args,
# same client args) but launches one of the 7 node processes under
# `samply record`. The other 6 run unprofiled so the network topology
# is complete. The client drives 50,000 tx through the cluster; when
# it finishes, we kill the cluster and samply writes its profile.
#
# Defaults: profile NODE 0. Set PROFILE_NODE=<i> to pick another.
#
# Usage (from repo root):
#   cargo build --profile profiling --bin node-artemis \
#               --bin client-artemis --bin genconfig
#   stress-test/profile-artemis.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

N=${N:-7}
F=${F:-3}
BLOCK_SIZE=${BLOCK_SIZE:-400}
PAYLOAD=${PAYLOAD:-0}
TOTAL_TXS=${TOTAL_TXS:-50000}
WINDOW=${WINDOW:-10000}
DELTA=${DELTA:-50}
BOOTSTRAP_SECS=${BOOTSTRAP_SECS:-12}
PROFILE_NODE=${PROFILE_NODE:-0}

BASE_PORT=${BASE_PORT:-31000}
CLI_BASE_PORT=$((BASE_PORT + 100))
MEMPOOL_BASE_PORT=$((BASE_PORT + 200))
CLIENT_LISTEN_PORT=$((BASE_PORT + 275))

RUN_DIR="stress-test/runs/artemis-profile-n${N}-b${BLOCK_SIZE}-p${PAYLOAD}-${BASE_PORT}"
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"

BIN_DIR="$REPO_ROOT/target/profiling"
if [ ! -x "$BIN_DIR/node-artemis" ] || [ ! -x "$BIN_DIR/client-artemis" ] || [ ! -x "$BIN_DIR/genconfig" ]; then
    echo "error: profiling binaries missing. Run:" >&2
    echo "  cargo build --profile profiling --bin node-artemis --bin client-artemis --bin genconfig" >&2
    exit 1
fi

# 1. Generate configs (certs + node/client JSON).
"$BIN_DIR/genconfig" \
    -n "$N" \
    -f "$F" \
    -d 50 \
    --blocksize "$BLOCK_SIZE" \
    --base_port "$BASE_PORT" \
    --client_base_port "$CLI_BASE_PORT" \
    --mempool_base_port "$MEMPOOL_BASE_PORT" \
    --client_listen_port "$CLIENT_LISTEN_PORT" \
    --payload "$PAYLOAD" \
    --target "$RUN_DIR" \
    >/dev/null

# 2. Write the ip_file / cli_ip_file the stress-test writes out-of-band.
ip_file="$RUN_DIR/ip_file"
cli_ip_file="$RUN_DIR/cli_ip_file"
: >"$ip_file"
: >"$cli_ip_file"
for i in $(seq 0 $((N - 1))); do
    echo "127.0.0.1:$((BASE_PORT + i))" >>"$ip_file"
    echo "127.0.0.1:$((CLI_BASE_PORT + i))" >>"$cli_ip_file"
done

# 3. Launch the non-profiled nodes.
node_pids=()
for i in $(seq 0 $((N - 1))); do
    if [ "$i" = "$PROFILE_NODE" ]; then
        continue
    fi
    "$BIN_DIR/node-artemis" \
        -c "$RUN_DIR/nodes-$i.json" \
        -i "$ip_file" \
        --sleep "$BOOTSTRAP_SECS" \
        --delta "$DELTA" \
        -s \
        >"$RUN_DIR/node-$i.log" 2>&1 &
    node_pids+=($!)
done

# 4. Launch the profiled node under samply.
#
#    * `--save-only` skips the interactive viewer so this script can
#      exit cleanly;
#    * `--duration` tells samply to auto-stop recording after N
#      seconds AND cleanly flush the profile -- sending SIGTERM from
#      a parent script was losing the profile (samply died before
#      writing);
#    * `--no-open` defends against `--save-only` + terminal weirdness.
#
#    The duration budget: bootstrap + client work + a small
#    post-work tail where most interesting steady-state samples
#    land.
PROFILE_DURATION=${PROFILE_DURATION:-$((BOOTSTRAP_SECS + 18))}
SAMPLY_OUTPUT="$RUN_DIR/node-${PROFILE_NODE}.samply.json.gz"
# `--unstable-presymbolicate` embeds a .syms.json sidecar at record
# time so our post-hoc analysis scripts can read symbolicated names
# directly out of the profile, without needing to spin up the
# samply web server.
samply record \
    --save-only \
    --no-open \
    --unstable-presymbolicate \
    --duration "$PROFILE_DURATION" \
    --output "$SAMPLY_OUTPUT" \
    -- \
    "$BIN_DIR/node-artemis" \
    -c "$RUN_DIR/nodes-${PROFILE_NODE}.json" \
    -i "$ip_file" \
    --sleep "$BOOTSTRAP_SECS" \
    --delta "$DELTA" \
    -s \
    >"$RUN_DIR/node-${PROFILE_NODE}.log" 2>&1 &
samply_pid=$!
node_pids+=("$samply_pid")

cleanup() {
    # Only terminate the unprofiled peers. Samply owns its own
    # lifetime via --duration and flushes its profile on the way out.
    for p in "${node_pids[@]}"; do
        if [ "$p" != "$samply_pid" ]; then
            kill "$p" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT

# 5. Wait for nodes to finish booting up, then run the client.
sleep "$BOOTSTRAP_SECS"

"$BIN_DIR/client-artemis" \
    -c "$RUN_DIR/client.json" \
    -i "$cli_ip_file" \
    -m "$TOTAL_TXS" \
    -w "$WINDOW" \
    >"$RUN_DIR/client.log" 2>&1 || true

echo ""
echo "=== Client output (last 20 lines) ==="
tail -20 "$RUN_DIR/client.log"
echo ""

# 6. Shut down the unprofiled peers. `--duration` told samply how
#    long to record, but samply itself waits for its child to exit --
#    the node's consensus loop never does, so we have to SIGTERM the
#    node (samply's child). Killing samply directly loses the flush.
cleanup
trap - EXIT
# Find samply's child (the node-artemis it's profiling) via pgrep and
# signal it; samply sees the child exit, writes the profile, exits.
echo "Signalling profiled node to exit so samply can flush..."
profiled_child=$(pgrep -P "$samply_pid" 2>/dev/null || true)
if [ -n "$profiled_child" ]; then
    kill "$profiled_child" 2>/dev/null || true
fi
echo "Waiting for samply to write profile..."
wait "$samply_pid" 2>/dev/null || true

echo "=== Samply profile saved to: ==="
echo "  $SAMPLY_OUTPUT"
echo ""
echo "Open it with:"
echo "  samply load $SAMPLY_OUTPUT"
