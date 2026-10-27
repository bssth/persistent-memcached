# pmcd — persistent memcached in Rust

[![CI](https://github.com/bssth/persistent-memcached/actions/workflows/ci.yml/badge.svg)](https://github.com/bssth/persistent-memcached/actions/workflows/ci.yml)
![MIU](https://img.shields.io/badge/made_in-Ukraine-ffd700?labelColor=0057b7)

pmcd is a memcached-compatible server that does not lose data on restart. Every shard is
replicated with Raft, and keys are spread over shards with hash slots. It speaks the vanilla
memcached text protocol (classic and meta commands) and gRPC.

* **Persistent.** Every write goes to a write-ahead log (the Raft log) before the reply.
  Snapshots keep restarts fast. A node killed with `kill -9` comes back with all acknowledged writes.
* **Replicated.** One Raft group per shard (openraft). A shard keeps serving while a majority
  of its replicas is up.
* **Sharded.** 16384 hash slots (CRC16, `{hash tags}`), like Redis Cluster. Any node accepts any
  key and forwards the request to the shard leader.
* **Compatible.** Checked byte for byte against real memcached 1.6.45 with a differential test.

> **Status: experimental.** The cluster topology is fixed at bootstrap: there is no slot migration
> or membership change yet. See [Limitations and roadmap](https://github.com/bssth/persistent-memcached/wiki/Limitations-and-Roadmap).

Documentation lives in the [wiki](https://github.com/bssth/persistent-memcached/wiki):
[architecture](https://github.com/bssth/persistent-memcached/wiki/Architecture),
[configuration](https://github.com/bssth/persistent-memcached/wiki/Configuration),
[text protocol](https://github.com/bssth/persistent-memcached/wiki/Text-Protocol),
[operations](https://github.com/bssth/persistent-memcached/wiki/Operations).

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/V7V51ZQ1RH)

## Install

Prebuilt binaries for Linux x86_64 and aarch64 (glibc 2.17+) are on the
[releases page](https://github.com/bssth/persistent-memcached/releases).

Docker image (amd64, arm64):

```bash
docker run -d --name pmcd -p 11211:11211 -p 11212:11212 -v pmcd-data:/data \
  ghcr.io/bssth/persistent-memcached:latest
```

Data lives in the `/data` volume. To use your own config, mount it and pass the arguments again:
`-v ./pmcd.toml:/etc/pmcd.toml:ro ghcr.io/bssth/persistent-memcached -c /etc/pmcd.toml --data-dir /data`.
To build the image from source, run `docker build -t pmcd .`.

A release is published when a tag `v<version from Cargo.toml>` is pushed. See
[Build and releases](https://github.com/bssth/persistent-memcached/wiki/Build-and-Releases).

## Build

Rust 1.85+ is required. `protoc` is not: the proto files are compiled by `protox`.

```bash
cargo build --release
```

When the sources are on a Windows drive and you build from WSL, use `scripts/cargo.sh`.
It keeps the target directory on the WSL filesystem, which is much faster.

## Run

A single node (one Raft group per shard, 4 shards by default):

```bash
pmcd --data-dir ./data -l 0.0.0.0:11211 --grpc 0.0.0.0:11212
```

A three-node cluster on one machine:

```bash
scripts/cluster.sh start
printf 'set k 0 0 5\r\nhello\r\n' | nc -q1 127.0.0.1 11211
printf 'get k\r\n' | nc -q1 127.0.0.1 11231     # any node serves any key
scripts/cluster.sh kill 1                        # shard leaders are re-elected in a second or two
scripts/cluster.sh stop
```

The config is TOML. See [examples/cluster/node1.toml](examples/cluster/node1.toml) and the
[configuration reference](https://github.com/bssth/persistent-memcached/wiki/Configuration).

| Key | Default | Meaning |
|---|---|---|
| `fsync` | `true` | fsync the log before replying to the client |
| `read_consistency` | `leader` | `linearizable` runs a read-index round before every read |
| `max_item_size` | 1 MiB | like memcached's `-I` |
| `memory_limit_mb` | 0 (none) | above the limit new writes get `SERVER_ERROR out of memory storing object` |
| `cluster.shards` | 4 | number of shards (Raft groups) |
| `cluster.replication` | 3 | replicas per shard |

## Compatibility

The reference is memcached master (1.6.40+), plus harmless forms that 1.6.38 accepted
(leading spaces, `delete <key> 0`). A differential test sends the same bytes to real memcached
and to pmcd and compares the replies:

```bash
python3 tests/compat/diff.py --memcached 127.0.0.1:21211 --pmcd 127.0.0.1:11211
```

Deliberate differences:

* **No eviction (LRU).** pmcd is a store, not a cache: when `memory_limit_mb` is reached, new data is rejected.
* **CAS is the Raft log index** of the last write. It is unique and monotonic within a shard and
  survives restarts and leader changes.
* `decr` stores the exact digits (memcached sometimes pads the number with spaces).
* The meta flag `E` (explicit CAS) is not supported and returns `CLIENT_ERROR invalid flag`.
* `stats slabs` and `stats items` are empty. There is no binary protocol and no UDP.

## gRPC

The public API is [proto/pmc/v1/memcache.proto](proto/pmc/v1/memcache.proto): `Get`, `Set`
(all store modes and CAS), `Delete`, `Incr`, `Decr`, `Touch`, `FlushAll`, `Stats`. The internal
`pmc.cluster.v1.Cluster` service carries Raft traffic, forwards operations between nodes, and serves
`Status` (JSON with the state of every shard). See the [gRPC API](https://github.com/bssth/persistent-memcached/wiki/gRPC-API) page.

## License

MIT OR Apache-2.0.
