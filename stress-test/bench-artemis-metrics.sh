#!/usr/bin/env bash
# Runs artemis 7/3 locally, then SIGTERMs each node so its in-memory
# metrics snapshot dumps to stderr before the process exits. No
# `log::info!` pollution on the hot path -- every event site is an
# atomic increment + bucket bump.
#
# Usage (from repo root):
#   cargo build --release --bin node-artemis --bin client-artemis \
#               --bin genconfig
#   stress-test/bench-artemis-metrics.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

N=${N:-7}
F=${F:-3}
BLOCK_SIZE=${BLOCK_SIZE:-400}
TOTAL_TXS=${TOTAL_TXS:-50000}
WINDOW=${WINDOW:-10000}
BOOTSTRAP_SECS=${BOOTSTRAP_SECS:-12}
DELTA=${DELTA:-50}

BASE_PORT=${BASE_PORT:-31000}
CLI_BASE=$((BASE_PORT + 100))
MEMPOOL_BASE=$((BASE_PORT + 200))
CLIENT_LISTEN=$((BASE_PORT + 275))

RUN_DIR="stress-test/runs/artemis-metrics-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_DIR"

BIN_DIR="target/release"
for b in node-artemis client-artemis genconfig; do
    [ -x "$BIN_DIR/$b" ] || { echo "missing $BIN_DIR/$b" >&2; exit 1; }
done

"$BIN_DIR/genconfig" \
    -n "$N" -f "$F" -d 50 --blocksize "$BLOCK_SIZE" \
    --base_port "$BASE_PORT" --client_base_port "$CLI_BASE" \
    --mempool_base_port "$MEMPOOL_BASE" --client_listen_port "$CLIENT_LISTEN" \
    --payload 0 --target "$RUN_DIR" >/dev/null

ip_file="$RUN_DIR/ip_file"
cli_ip_file="$RUN_DIR/cli_ip_file"
: >"$ip_file"
: >"$cli_ip_file"
for i in $(seq 0 $((N - 1))); do
    echo "127.0.0.1:$((BASE_PORT + i))" >>"$ip_file"
    echo "127.0.0.1:$((CLI_BASE + i))" >>"$cli_ip_file"
done

pids=()
for i in $(seq 0 $((N - 1))); do
    "$BIN_DIR/node-artemis" \
        -c "$RUN_DIR/nodes-$i.json" -i "$ip_file" \
        --sleep "$BOOTSTRAP_SECS" --delta "$DELTA" -s \
        >"$RUN_DIR/node-$i.log" 2>&1 &
    pids+=($!)
done

# SIGTERM nodes so the reactor's signal handler dumps metrics to
# stderr (captured in node-$i.log), then SIGKILL as fallback.
cleanup() {
    for p in "${pids[@]}"; do
        kill -TERM "$p" 2>/dev/null || true
    done
    # Allow metrics dump to flush. 2s is generous.
    sleep 2
    for p in "${pids[@]}"; do
        kill -KILL "$p" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

sleep "$BOOTSTRAP_SECS"

"$BIN_DIR/client-artemis" \
    -c "$RUN_DIR/client.json" -i "$cli_ip_file" \
    -m "$TOTAL_TXS" -w "$WINDOW" \
    >"$RUN_DIR/client.log" 2>&1 || true

cleanup
trap - EXIT

echo
echo "=== Client ==="
grep -E 'DP\[' "$RUN_DIR/client.log" | tail -4
echo
# Each node flushes its metrics snapshot to its log on SIGTERM. Find
# the line where the summary header begins and print from there to
# the next trailing `====` line.
for log in "$RUN_DIR"/node-*.log; do
    node_name=$(basename "$log" .log)
    echo
    echo "=== $node_name metrics ==="
    start=$(grep -n 'artemis metrics' "$log" | head -1 | cut -d: -f1)
    if [ -z "$start" ]; then
        echo "(no metrics dump)"
        continue
    fi
    start=$((start - 1))          # include the ==== line above
    tail -n +"$start" "$log"
done
echo
echo "Run dir: $RUN_DIR"
