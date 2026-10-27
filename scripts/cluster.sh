#!/usr/bin/env bash
# Starts a local 3-node pmcd cluster from examples/cluster/node1.toml.
#   scripts/cluster.sh start [--release]   # logs and data under $PMCD_RUN (default /tmp/pmcd-cluster)
#   scripts/cluster.sh stop
#   scripts/cluster.sh kill <node>         # SIGKILL one node, e.g. to test failover
#   scripts/cluster.sh restart <node>
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${PMCD_RUN:-/tmp/pmcd-cluster}
TARGET=${CARGO_TARGET_DIR:-$HOME/.cache/pmcd-target}
PROFILE=debug
mkdir -p "$RUN"

bin() { echo "$TARGET/$PROFILE/pmcd"; }

start_node() {
  local id=$1
  local mc=$((11201 + id * 10)) grpc=$((11202 + id * 10))
  "$(bin)" -c examples/cluster/node1.toml --node-id "$id" \
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
    [[ "${2:-}" == "--release" ]] && PROFILE=release
    for id in 1 2 3; do start_node "$id"; done
    ;;
  stop) for id in 1 2 3; do stop_node "$id"; done ;;
  kill) stop_node "$2" KILL ;;
  restart) stop_node "$2"; sleep 0.5; start_node "$2" ;;
  *) echo "usage: $0 start [--release] | stop | kill <node> | restart <node>"; exit 1 ;;
esac
