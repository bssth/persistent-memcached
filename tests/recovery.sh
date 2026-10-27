#!/usr/bin/env bash
# Crash-recovery test for a single node: write keys, SIGKILL the server, restart it and check
# that every acknowledged write survived. A small snapshot interval makes the test cover
# snapshot building, log purging and loading from snapshot + log tail.
#   tests/recovery.sh [path/to/pmcd]
set -euo pipefail
BIN=${1:-$(bash "$(dirname "$0")/../scripts/target-dir.sh")/debug/pmcd}
[[ -x "$BIN" ]] || { echo "no pmcd binary at $BIN; build it first: scripts/cargo.sh build" >&2; exit 1; }
DIR=${PMCD_TEST_DIR:-/dev/shm/pmcd-recovery}
PORT=${PMCD_TEST_PORT:-11411}
rm -rf "$DIR" && mkdir -p "$DIR"
cat >"$DIR/test.toml" <<EOF
data_dir = "$DIR/data"
memcache_listen = "127.0.0.1:$PORT"
grpc_listen = "127.0.0.1:$((PORT + 1))"
[raft]
snapshot_logs = 500
keep_logs = 100
[cluster]
shards = 2
EOF

start() {
  "$BIN" -c "$DIR/test.toml" >>"$DIR/pmcd.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    [[ "$(printf 'set _ready 0 0 1\r\nx\r\n' | timeout 1 nc -q1 127.0.0.1 "$PORT" 2>/dev/null)" == STORED* ]] && return
    sleep 0.1
  done
  echo "server did not become ready"; exit 1
}

python_client() {
  python3 - "$PORT" "$1" <<'PY'
import socket, sys
port, mode = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port)); s.settimeout(10)
f = s.makefile("rb")
def cmd(b):
    s.sendall(b)
    return f.readline()
N = 3000
if mode == "write":
    for i in range(N):
        assert cmd(b"set key%d %d 0 %d\r\nvalue-%d\r\n" % (i, i, len(b"value-%d" % i), i)) == b"STORED\r\n"
    for i in range(0, N, 3):
        assert cmd(b"delete key%d\r\n" % i) == b"DELETED\r\n"
    assert cmd(b"set counter 0 0 1\r\n0\r\n") == b"STORED\r\n"
    for i in range(500):
        assert cmd(b"incr counter 2\r\n") == b"%d\r\n" % (2 * (i + 1))
    print("wrote", N, "keys")
else:
    bad = 0
    for i in range(N):
        s.sendall(b"get key%d\r\n" % i)
        line = f.readline()
        if i % 3 == 0:
            ok = line == b"END\r\n"
        else:
            data = f.readline()
            end = f.readline()
            ok = line == b"VALUE key%d %d %d\r\n" % (i, i, len(b"value-%d" % i)) and data == b"value-%d\r\n" % i and end == b"END\r\n"
        if not ok:
            bad += 1
            if bad < 5:
                print("mismatch for key", i, line)
    s.sendall(b"get counter\r\n"); f.readline(); counter = f.readline().strip(); f.readline()
    assert counter == b"1000", counter
    assert bad == 0, f"{bad} keys wrong after restart"
    print("all keys survived")
PY
}

start
python_client write
echo "snapshots before crash: $(ls "$DIR"/data/shard-*/snapshots/*.snap 2>/dev/null | wc -l)"
kill -9 "$PID"; wait "$PID" 2>/dev/null || true
start
python_client check
grep -c "loaded snapshot" "$DIR/pmcd.log" | xargs echo "snapshot loads on restart:"
kill "$PID"; wait "$PID" 2>/dev/null || true
echo "recovery test passed"
