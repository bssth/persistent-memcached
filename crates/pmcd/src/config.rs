use std::path::PathBuf;

use anyhow::{Context, bail};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    /// Read from the shard leader's memory. Fast; may be stale for a moment after a leader
    /// is deposed by a network partition.
    #[default]
    Leader,
    /// Confirm leadership with a quorum before every read (Raft read-index).
    Linearizable,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub node_id: u64,
    pub data_dir: PathBuf,
    /// memcached text protocol listener.
    pub memcache_listen: String,
    /// gRPC listener (public API + node-to-node traffic).
    pub grpc_listen: String,
    pub max_item_size: usize,
    /// Refuse new data above this much item memory (0 = unlimited).
    pub memory_limit_mb: u64,
    /// fsync the log before acknowledging writes.
    pub fsync: bool,
    pub read_consistency: ReadConsistency,
    pub raft: RaftConfig,
    pub cluster: ClusterConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RaftConfig {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    /// Build a snapshot after this many log entries.
    pub snapshot_logs: u64,
    /// Log entries kept after a snapshot, so lagging followers can catch up from the log.
    pub keep_logs: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    /// Part of the cluster identity; change it to keep two clusters apart.
    pub name: String,
    pub shards: u32,
    pub replication: usize,
    /// All nodes of the cluster. Empty means a single-node cluster of this node.
    pub nodes: Vec<NodeConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub id: u64,
    /// Address other nodes use to reach this node's gRPC listener.
    pub grpc: String,
    /// Address clients use for the text protocol (informational).
    #[serde(default)]
    pub memcache: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 1,
            data_dir: PathBuf::from("data"),
            memcache_listen: "0.0.0.0:11211".into(),
            grpc_listen: "0.0.0.0:11212".into(),
            max_item_size: 1 << 20,
            memory_limit_mb: 0,
            fsync: true,
            read_consistency: ReadConsistency::Leader,
            raft: RaftConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 100,
            election_min_ms: 500,
            election_max_ms: 1000,
            snapshot_logs: 100_000,
            keep_logs: 5_000,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self { name: "pmcd".into(), shards: 4, replication: 3, nodes: Vec::new() }
    }
}

/// Replaces an unspecified listen host with loopback, for the single-node default.
fn connectable(addr: &str) -> String {
    match addr.strip_prefix("0.0.0.0:") {
        Some(port) => format!("127.0.0.1:{port}"),
        None => addr.to_string(),
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    /// Fills defaults that depend on other fields and checks consistency.
    pub fn finish(mut self) -> anyhow::Result<Self> {
        if self.cluster.nodes.is_empty() {
            self.cluster.nodes.push(NodeConfig {
                id: self.node_id,
                grpc: connectable(&self.grpc_listen),
                memcache: connectable(&self.memcache_listen),
            });
        }
        self.cluster.nodes.sort_by_key(|n| n.id);
        if self.cluster.nodes.windows(2).any(|w| w[0].id == w[1].id) {
            bail!("duplicate node id in cluster.nodes");
        }
        if !self.cluster.nodes.iter().any(|n| n.id == self.node_id) {
            bail!("node_id {} is not listed in cluster.nodes", self.node_id);
        }
        if self.node_id == 0 {
            bail!("node_id must be positive");
        }
        if self.cluster.shards == 0 || self.cluster.shards > crate::cluster::SLOTS as u32 {
            bail!("cluster.shards must be between 1 and {}", crate::cluster::SLOTS);
        }
        if self.cluster.replication == 0 {
            bail!("cluster.replication must be positive");
        }
        if self.max_item_size < 1024 {
            bail!("max_item_size must be at least 1024");
        }
        Ok(self)
    }
}
