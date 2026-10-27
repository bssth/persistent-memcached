#!/usr/bin/env python3
"""Differential test: send the same raw byte streams to real memcached and to pmcd and
compare the replies.

    python3 tests/compat/diff.py --memcached 127.0.0.1:21211 --pmcd 127.0.0.1:11211

Values that legitimately differ (CAS numbers, version, timing stats) are normalised. Cases
listed in KNOWN_DIFFERENCES document deliberate deviations; they are reported, not failed.
"""

import argparse
import re
import socket
import sys
import time

# Each case is a list of raw request chunks sent in order. Every case starts with a fresh
# connection and a flush_all, so cases are independent.
CASES = {
    "set-get": [b"set k 5 0 3\r\nabc\r\n", b"get k\r\n", b"gets k\r\n"],
    "multi-get-order": [b"set a 0 0 1\r\n1\r\nset b 0 0 1\r\n2\r\n", b"get b missing a b\r\n"],
    "add-replace": [b"add k 0 0 1\r\nx\r\nadd k 0 0 1\r\ny\r\nreplace k 0 0 1\r\nz\r\nreplace nope 0 0 1\r\nz\r\nget k\r\n"],
    "append-prepend": [b"append k 0 0 1\r\nx\r\nset k 9 0 3\r\nmid\r\nappend k 1 0 1\r\n>\r\nprepend k 2 0 1\r\n<\r\nget k\r\n"],
    "cas-flow": [b"cas k 0 0 1 1\r\nx\r\nset k 0 0 1\r\nx\r\ncas k 0 0 1 0\r\ny\r\n"],
    "delete": [b"set k 0 0 1\r\nx\r\ndelete k\r\ndelete k\r\nset k 0 0 1\r\nx\r\ndelete k 0\r\nget k\r\n"],
    "incr-decr": [b"set n 0 0 2\r\n10\r\nincr n 5\r\ndecr n 100\r\nincr nope 1\r\nset s 0 0 2\r\nhi\r\nincr s 1\r\n"],
    "incr-wrap": [b"set n 0 0 20\r\n18446744073709551615\r\nincr n 2\r\n"],
    "incr-bad-delta": [b"incr n abc\r\nincr n -1\r\n"],
    "touch": [b"set k 0 0 1\r\nx\r\ntouch k 100\r\ntouch nope 100\r\nget k\r\n"],
    "gat": [b"set k 3 0 1\r\nx\r\ngat 100 k nope\r\n"],
    "negative-exptime": [b"set k 0 -1 1\r\nx\r\nget k\r\n"],
    "past-absolute-exptime": [b"set k 0 1000000000 1\r\nx\r\nget k\r\n"],
    "noreply": [b"set k 0 0 1 noreply\r\nx\r\nincr k 1 noreply\r\nget k\r\ndelete k noreply\r\nget k\r\n"],
    "flush": [b"set k 0 0 1\r\nx\r\nflush_all\r\nget k\r\nset k 0 0 1\r\ny\r\nget k\r\n"],
    "flush-delayed": [b"set k 0 0 1\r\nx\r\nflush_all 100\r\nget k\r\n"],
    "unknown": [b"bogus\r\nGET k\r\n"],
    "bad-data-chunk": [b"set k 0 0 1\r\nabc\r\nget k\r\n"],
    "bad-length": [b"set k 0 0 -1\r\nversion\r\n"],
    "zero-length": [b"set k 0 0 0\r\n\r\nget k\r\n"],
    "key-too-long": [b"get " + b"k" * 251 + b"\r\n", b"set " + b"k" * 251 + b" 0 0 1\r\nx\r\n"],
    "split-packets": [b"se", b"t k 0 0 5\r\nhel", b"lo\r\nget", b" k\r\n"],
    "pipelined": [b"set a 0 0 1\r\n1\r\nset b 0 0 1\r\n2\r\nget a\r\nget b\r\ndelete a\r\nget a b\r\n"],
    "meta-basic": [b"ms k 2 T0 F7\r\nhi\r\nmg k v f\r\nmg k s k\r\nmg missing v\r\nmg missing v Oxyz k\r\nmn\r\n"],
    "meta-quiet": [b"ms k 1 q\r\nx\r\nmg missing v q\r\nmg k v q\r\nmd nope q\r\nmn\r\n"],
    "meta-set-modes": [b"ms k 1 ME\r\nx\r\nms k 1 ME\r\ny\r\nms k 1 MA\r\nz\r\nms nope 1 MR\r\nz\r\nmg k v\r\n"],
    "meta-delete": [b"ms k 1\r\nx\r\nmd k\r\nmd k\r\nmg k v\r\n"],
    "meta-arith": [b"ma cnt\r\nma cnt N0 J13 v\r\nma cnt v\r\nma cnt MD D20 v\r\nms s 2\r\nhi\r\nma s\r\n"],
    "meta-errors": [b"mg k zz\r\nmg k v v\r\nmg\r\nma k Q\r\n"],
    "meta-base64": [b"ms Zm9v 1 b\r\nx\r\nmg foo v\r\nmg Zm9v b k v\r\n"],
    "meta-stale-lease": [b"ms k 1 T0\r\nx\r\nmd k I T30\r\nmg k v\r\nmg k v\r\nmg lease N30 v\r\nmg lease N30 v\r\n"],
    "verbosity-version": [b"verbosity 1\r\n", b"verbosity\r\n"],
    "cas-wrong-arity": [b"cas k 0 0 1\r\nx\r\nversion\r\n"],
}

# Deliberate deviations from memcached 1.6.45 (pmcd targets the 1.6.40+ parser; against
# 1.6.39 and older expect more differences: ERROR instead of CLIENT_ERROR for wrong argument
# counts, "HD " with a trailing space, no O/k flags on EN).
KNOWN_DIFFERENCES = {
    # memcached 1.6.45 sends a stray empty line after the error for `get <too long key>`.
    "key-too-long": "upstream stray CRLF",
}


def connect(addr):
    host, port = addr.rsplit(":", 1)
    s = socket.create_connection((host, int(port)), timeout=3)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def drain(s, quiet=0.15):
    """Reads until the server stays silent for `quiet` seconds or closes."""
    out = b""
    s.settimeout(quiet)
    while True:
        try:
            chunk = s.recv(65536)
        except socket.timeout:
            return out
        if not chunk:
            return out + b"<closed>"
        out += chunk


def run(addr, chunks):
    s = connect(addr)
    s.sendall(b"flush_all\r\n")
    got = drain(s, 1.0)
    assert got == b"OK\r\n", f"flush_all on {addr}: {got!r}"
    out = b""
    for c in chunks:
        s.sendall(c)
        time.sleep(0.02)
    out += drain(s, 0.3)
    s.close()
    return out


def normalise(b):
    b = re.sub(rb"(VALUE \S+ \d+ \d+) \d+", rb"\1 <cas>", b)
    b = re.sub(rb" c\d+", rb" c<cas>", b)
    b = re.sub(rb"VERSION \S+", rb"VERSION <v>", b)
    b = re.sub(rb" t(\d+)", rb" t<ttl>", b)
    return b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--memcached", required=True)
    ap.add_argument("--pmcd", required=True)
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("only", nargs="*")
    args = ap.parse_args()
    failed = known = 0
    for name, chunks in CASES.items():
        if args.only and name not in args.only:
            continue
        a = normalise(run(args.memcached, chunks))
        b = normalise(run(args.pmcd, chunks))
        if a == b:
            print(f"ok    {name}")
            if args.verbose:
                print("      " + repr(a))
            continue
        tag = "known" if name in KNOWN_DIFFERENCES else "FAIL "
        if name in KNOWN_DIFFERENCES:
            known += 1
        else:
            failed += 1
        print(f"{tag} {name}")
        print(f"      memcached: {a!r}")
        print(f"      pmcd:      {b!r}")
    print(f"\n{len(CASES) if not args.only else len(args.only)} cases, {failed} failed, {known} known differences")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
