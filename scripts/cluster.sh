#!/usr/bin/env bash
# Starts a local 3-node pmcd cluster from examples/cluster/node1.toml.
#   scripts/cluster.sh start [--release]   # logs and data under $PMCD_RUN (default /tmp/pmcd-cluster)
#   scripts/cluster.sh stop
#   scripts/cluster.sh kill <node>         # SIGKILL one node, e.g. to test failover
#   scripts/cluster.sh restart <node>      # restarts it with the binary `start` used
# Build first with `scripts/cargo.sh build [--release]` (or plain cargo); the binary is looked
# up in the same target directory.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${PMCD_RUN:-/tmp/pmcd-cluster}
mkdir -p "$RUN"

# Sets BIN to the pmcd binary of the given profile, or fails with a hint.
find_bin() {
  BIN="$(bash scripts/target-dir.sh)/$1/pmcd"
  if [[ ! -x "$BIN" ]]; then
    local flag=""
    [[ $1 == release ]] && flag=" --release"
    echo "no pmcd binary at $BIN; build it first: scripts/cargo.sh build$flag" >&2
    exit 1
  fi
}

start_node() {
  local id=$1
  local mc=$((11201 + id * 10)) grpc=$((11202 + id * 10))
  "$BIN" -c examples/cluster/node1.toml --node-id "$id" \
    --listen "127.0.0.1:$mc" --grpc "127.0.0.1:$grpc" --data-dir "$RUN/node$id" \
    >"$RUN/node$id.log" 2>&1 &
  echo $! >"$RUN/node$id.pid"
  echo "node $id: memcache 127.0.0.1:$mc grpc 127.0.0.1:$grpc pid $(cat "$RUN/node$id.pid")"
}

stop_node() {
  local id=$1 sig=${2:-TERM}
  if [[ -f "$RUN/node$id.pid" ]]; then
    kill "-$sig" "$(cat "$RUN/node$id.pid")" 2>/dev/null || true
    rm -f "$RUN/node$id.pid"
  fi
}

case "${1:-}" in
  start)
    if [[ "${2:-}" == "--release" ]]; then find_bin release; else find_bin debug; fi
    echo "$BIN" >"$RUN/bin"
    for id in 1 2 3; do start_node "$id"; done
    ;;
  stop) for id in 1 2 3; do stop_node "$id"; done ;;
  kill) stop_node "$2" KILL ;;
  restart)
    if [[ -f "$RUN/bin" ]]; then BIN=$(cat "$RUN/bin"); else find_bin debug; fi
    stop_node "$2"
    sleep 0.5
    start_node "$2"
    ;;
  *) echo "usage: $0 start [--release] | stop | kill <node> | restart <node>"; exit 1 ;;
esac
