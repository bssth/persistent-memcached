//! Cluster topology: nodes, shards and the hash-slot map.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::config::ClusterConfig;
use crate::raft::NodeInfo;

/// Number of hash slots, as in Redis Cluster.
pub const SLOTS: u16 = 16384;

const fn crc16_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC16: [u16; 256] = crc16_table();

/// CRC16-CCITT (XMODEM), the hash Redis Cluster uses for slots.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &b in data {
        crc = (crc << 8) ^ CRC16[(((crc >> 8) as u8) ^ b) as usize];
    }
    crc
}

/// Slot of a key. A non-empty `{tag}` makes only the tag count, so related keys can share a
/// shard: `user:{42}:name` and `user:{42}:age` land in the same slot.
pub fn key_slot(key: &[u8]) -> u16 {
    let hashed = match memchr_pos(key, b'{') {
        Some(open) => match memchr_pos(&key[open + 1..], b'}') {
            Some(len) if len > 0 => &key[open + 1..open + 1 + len],
            _ => key,
        },
        None => key,
    };
    crc16(hashed) % SLOTS
}

/// FNV-1a: stable across builds and platforms, unlike `std`'s hasher.
struct Fnv(u64);

impl Default for Fnv {
    fn default() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

fn memchr_pos(s: &[u8], b: u8) -> Option<usize> {
    s.iter().position(|&c| c == b)
}

#[derive(Clone, Debug, Serialize)]
pub struct ShardSpec {
    pub id: u32,
    /// Replica node ids; the first one bootstraps the shard's Raft group.
    pub replicas: Vec<u64>,
    /// Slot range `[first, last]` served by the shard.
    pub slots: (u16, u16),
}

#[derive(Clone, Debug)]
pub struct Topology {
    /// Stable hash of the configuration, carried by every node-to-node request so that two
    /// clusters sharing addresses (or a stray node) can never mix their Raft logs.
    pub cluster_id: u64,
    pub nodes: BTreeMap<u64, NodeInfo>,
    pub shards: Vec<ShardSpec>,
    slot_to_shard: Vec<u32>,
}

impl Topology {
    /// Static placement: shard `s` lives on `replication` consecutive nodes starting at `s`,
    /// and owns a contiguous slot range.
    ///
    /// `max_item_size` goes into `cluster_id` too: applying a log entry checks the item size,
    /// so nodes with different limits would apply the same entry differently.
    pub fn from_config(cfg: &ClusterConfig, max_item_size: usize) -> Self {
        let nodes: BTreeMap<u64, NodeInfo> =
            cfg.nodes.iter().map(|n| (n.id, NodeInfo { grpc: n.grpc.clone(), memcache: n.memcache.clone() })).collect();
        let ids: Vec<u64> = nodes.keys().copied().collect();
        let r = cfg.replication.min(ids.len());
        let n = cfg.shards;
        let mut slot_to_shard = vec![0u32; SLOTS as usize];
        let mut shards = Vec::with_capacity(n as usize);
        for s in 0..n {
            let first = (s as u64 * SLOTS as u64 / n as u64) as u16;
            let last = ((s as u64 + 1) * SLOTS as u64 / n as u64 - 1) as u16;
            for slot in first..=last {
                slot_to_shard[slot as usize] = s;
            }
            let replicas = (0..r).map(|j| ids[(s as usize + j) % ids.len()]).collect();
            shards.push(ShardSpec { id: s, replicas, slots: (first, last) });
        }
        let mut id = Fnv::default();
        id.write(cfg.name.as_bytes());
        id.write(&cfg.shards.to_le_bytes());
        id.write(&(cfg.replication as u64).to_le_bytes());
        id.write(&(max_item_size as u64).to_le_bytes());
        for (nid, info) in &nodes {
            id.write(&nid.to_le_bytes());
            id.write(info.grpc.as_bytes());
            id.write(&[0]);
        }
        Self { cluster_id: id.0, nodes, shards, slot_to_shard }
    }

    pub fn shard_of(&self, key: &[u8]) -> u32 {
        self.slot_to_shard[key_slot(key) as usize]
    }

    pub fn shard(&self, id: u32) -> Option<&ShardSpec> {
        self.shards.get(id as usize)
    }

    pub fn node(&self, id: u64) -> Option<&NodeInfo> {
        self.nodes.get(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;

    #[test]
    fn crc16_matches_redis() {
        // Reference values from the Redis Cluster specification.
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"{user1000}.following"), key_slot(b"{user1000}.followers"));
        assert_eq!(key_slot(b"foo{}{bar}"), crc16(b"foo{}{bar}") % SLOTS);
    }

    #[test]
    fn placement() {
        let cfg = ClusterConfig {
            name: "pmcd".into(),
            shards: 4,
            replication: 3,
            nodes: (1..=4).map(|id| NodeConfig { id, grpc: format!("n{id}"), memcache: String::new() }).collect(),
        };
        let t = Topology::from_config(&cfg, 1 << 20);
        assert_eq!(t.shards[0].replicas, vec![1, 2, 3]);
        assert_eq!(t.shards[3].replicas, vec![4, 1, 2]);
        assert_eq!(t.shards[0].slots, (0, 4095));
        assert_eq!(t.shards[3].slots, (12288, 16383));
        assert_eq!(t.shard_of(b"foo"), 2);
        let mut other = cfg.clone();
        other.nodes[0].grpc = "elsewhere".into();
        assert_ne!(t.cluster_id, Topology::from_config(&other, 1 << 20).cluster_id);
        assert_eq!(t.cluster_id, Topology::from_config(&cfg, 1 << 20).cluster_id);
    }

    /// Nodes with different item size limits must not form one cluster: the limit changes how
    /// a log entry is applied.
    #[test]
    fn max_item_size_is_part_of_cluster_id() {
        let cfg = ClusterConfig {
            name: "pmcd".into(),
            shards: 2,
            replication: 3,
            nodes: (1..=3).map(|id| NodeConfig { id, grpc: format!("n{id}"), memcache: String::new() }).collect(),
        };
        let a = Topology::from_config(&cfg, 1 << 20);
        let b = Topology::from_config(&cfg, 2 << 20);
        assert_ne!(a.cluster_id, b.cluster_id);
        assert_eq!(a.shards[0].replicas, b.shards[0].replicas);
    }
}
