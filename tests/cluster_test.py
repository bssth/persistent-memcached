#!/usr/bin/env python3
"""End-to-end test of a local 3-node cluster (scripts/cluster.sh): replication, proxying from
any node, leader failover, and catch-up of a restarted node.

    PMCD_RUN=/dev/shm/pmcd-cluster python3 tests/cluster_test.py
"""

import os
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODES = {1: 11211, 2: 11221, 3: 11231}


def sh(*args):
    subprocess.run(["bash", os.path.join(ROOT, "scripts/cluster.sh"), *args], check=True)


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=15)
        self.f = self.s.makefile("rb")

    def line(self, cmd):
        self.s.sendall(cmd)
        return self.f.readline()

    def get(self, key):
        self.s.sendall(b"get " + key + b"\r\n")
        first = self.f.readline()
        if first == b"END\r\n":
            return None
        if not first.startswith(b"VALUE"):
            raise RuntimeError(first)
        data = self.f.readline()[:-2]
        assert self.f.readline() == b"END\r\n"
        return data

    def stats(self):
        self.s.sendall(b"stats\r\n")
        out = {}
        while (l := self.f.readline()) != b"END\r\n":
            _, k, v = l.decode().split(" ", 2)
            out[k] = v.strip()
        return out

    def close(self):
        self.s.close()


def wait_ready(port, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = Conn(port)
            if c.line(b"set _ready 0 0 1\r\nx\r\n") == b"STORED\r\n":
                c.close()
                return
            c.close()
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"node on {port} not ready")


def unavailable_window(port, key):
    """Writes in a loop until one succeeds; returns how long writes failed."""
    start = time.time()
    while True:
        try:
            c = Conn(port)
            r = c.line(b"set " + key + b" 0 0 2\r\nok\r\n")
            c.close()
            if r == b"STORED\r\n":
                return time.time() - start
        except OSError:
            pass
        if time.time() - start > 30:
            raise RuntimeError("cluster did not recover")
        time.sleep(0.05)


def leaders():
    out = {}
    for n, port in NODES.items():
        try:
            c = Conn(port)
            out[n] = int(c.stats()["pmcd_leader_shards"])
            c.close()
        except OSError:
            out[n] = None
    return out


def check(cond, msg):
    print(("ok    " if cond else "FAIL  ") + msg)
    if not cond:
        sys.exit(1)


def main():
    sh("stop")
    subprocess.run(["rm", "-rf", os.environ.get("PMCD_RUN", "/tmp/pmcd-cluster")], check=True)
    sh("start")
    try:
        for port in NODES.values():
            wait_ready(port)
        print("leaders per node:", leaders())

        N = 1000
        c1 = Conn(NODES[1])
        for i in range(N):
            assert c1.line(b"set k%d 0 0 %d\r\nv%d\r\n" % (i, len(b"v%d" % i), i)) == b"STORED\r\n"
        c1.close()
        for n in (2, 3):
            c = Conn(NODES[n])
            bad = sum(c.get(b"k%d" % i) != b"v%d" % i for i in range(N))
            c.close()
            check(bad == 0, f"node {n} serves all {N} keys written via node 1")

        c3 = Conn(NODES[3])
        check(c3.line(b"cas k1 0 0 1 1\r\nx\r\n") == b"EXISTS\r\n", "cas with a stale unique fails cluster-wide")
        check(c3.line(b"incr ctr 1\r\n") == b"NOT_FOUND\r\n", "incr on a missing key")
        c3.line(b"set ctr 0 0 1\r\n0\r\n")
        c3.close()
        for n, port in NODES.items():
            c = Conn(port)
            c.line(b"incr ctr 1\r\n")
            c.close()
        c = Conn(NODES[2])
        check(c.get(b"ctr") == b"3", "increments through three different nodes add up")
        c.close()

        victim = max((n for n in NODES), key=lambda n: leaders().get(n) or -1)
        survivor = next(n for n in NODES if n != victim)
        print(f"killing node {victim} (leads the most shards)")
        sh("kill", str(victim))
        gap = unavailable_window(NODES[survivor], b"after-kill")
        check(gap < 10, f"writes resumed {gap:.2f}s after the leader was killed")
        c = Conn(NODES[survivor])
        bad = sum(c.get(b"k%d" % i) != b"v%d" % i for i in range(N))
        for i in range(N, N + 200):
            assert c.line(b"set k%d 0 0 %d\r\nv%d\r\n" % (i, len(b"v%d" % i), i)) == b"STORED\r\n"
        c.close()
        check(bad == 0, "no acknowledged write was lost with one node down")
        print("leaders per node:", leaders())

        print(f"restarting node {victim}")
        sh("restart", str(victim))
        wait_ready(NODES[victim])
        time.sleep(2)  # let it catch up from the leaders' logs
        other = next(n for n in NODES if n not in (victim, survivor))
        print(f"killing node {other}: the restarted node must now carry the quorum")
        sh("kill", str(other))
        gap = unavailable_window(NODES[victim], b"after-second-kill")
        c = Conn(NODES[victim])
        bad = sum(c.get(b"k%d" % i) != b"v%d" % i for i in range(N + 200))
        c.close()
        check(bad == 0, f"restarted node caught up: all {N + 200} keys readable (writes resumed after {gap:.2f}s)")
        print("all cluster checks passed")
    finally:
        sh("stop")


if __name__ == "__main__":
    main()
