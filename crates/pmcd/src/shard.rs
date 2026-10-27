//! One shard replica on this node: a Raft group plus its in-memory store.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use openraft::error::{CheckIsLeaderError, ClientWriteError, InitializeError, RaftError};
use openraft::{RaftMetrics, ServerState, SnapshotPolicy};
use pmc_store::store::{Hit, StoreStats};
use pmc_store::wal::WalOptions;
use pmc_store::{Item, Op, Reply, Store, StoreConfig, Write};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

use crate::config::{Config, ReadConsistency};
use crate::raft::log_store::LogStore;
use crate::raft::network::{Network, Peers};
use crate::raft::state_machine::StateMachine;
use crate::raft::{NodeId, NodeInfo, Raft};

pub fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// An operation on one shard, executed by the node that can serve it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ShardOp {
    Write(Op),
    Get { keys: Vec<Bytes>, bump: bool },
    MetaGet { key: Bytes, touch: Option<i64>, vivify: Option<i64>, recache: Option<u64>, bump: bool },
    Stats,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaHit {
    pub item: Item,
    pub fetched_before: bool,
    pub last_access: u64,
    pub won: bool,
    pub already_won: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ShardResult {
    Reply(Reply),
    Hits(Vec<Option<Hit>>),
    Meta(Option<MetaHit>),
    Stats(StoreStats),
}

#[derive(Clone, Debug, Error, Serialize, Deserialize)]
pub enum ExecError {
    #[error("not the shard leader")]
    NotLeader { leader: Option<NodeId> },
    #[error("out of memory storing object")]
    OutOfMemory,
    #[error("cluster unavailable: {0}")]
    Unavailable(String),
    #[error("{0}")]
    Internal(String),
}

/// The node-wide item memory limit (`memory_limit_mb`), shared by every shard replica on the
/// node: what counts is the sum of their stores, followers included, since they all use this
/// node's RAM.
pub struct MemoryBudget {
    /// Bytes; 0 means no limit.
    limit: usize,
    stores: parking_lot::RwLock<Vec<Arc<Store>>>,
}

impl MemoryBudget {
    pub fn new(limit: usize) -> Self {
        Self { limit, stores: Default::default() }
    }

    pub fn register(&self, store: Arc<Store>) {
        self.stores.write().push(store);
    }

    pub fn used(&self) -> usize {
        self.stores.read().iter().map(|s| s.used_bytes()).sum()
    }

    /// Whether writes that add data must be refused.
    pub fn exhausted(&self) -> bool {
        self.limit > 0 && self.used() > self.limit
    }
}

pub struct Shard {
    pub id: u32,
    pub raft: Raft,
    pub store: Arc<Store>,
    pub log: LogStore,
    metrics: watch::Receiver<RaftMetrics<NodeId, NodeInfo>>,
    memory: Arc<MemoryBudget>,
}

impl Shard {
    pub async fn open(
        id: u32,
        cfg: &Config,
        replicas: &[NodeId],
        nodes: &BTreeMap<NodeId, NodeInfo>,
        cluster_id: u64,
        peers: Arc<Peers>,
        memory: Arc<MemoryBudget>,
    ) -> anyhow::Result<Arc<Self>> {
        let dir = cfg.data_dir.join(format!("shard-{id:05}"));
        let store = Arc::new(Store::new(StoreConfig { stripes: 256, max_item_size: cfg.max_item_size }));
        memory.register(store.clone());
        let log = LogStore::open(&dir.join("log"), WalOptions { fsync: cfg.fsync, ..WalOptions::default() })?;
        let sm = StateMachine::open(&dir.join("snapshots"), store.clone())?;
        let raft_cfg = openraft::Config {
            cluster_name: format!("pmcd-shard-{id}"),
            heartbeat_interval: cfg.raft.heartbeat_ms,
            election_timeout_min: cfg.raft.election_min_ms,
            election_timeout_max: cfg.raft.election_max_ms,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(cfg.raft.snapshot_logs),
            max_in_snapshot_log_to_keep: cfg.raft.keep_logs,
            max_payload_entries: 64,
            ..Default::default()
        }
        .validate()?;
        let net = Network { shard: id, cluster_id, peers };
        let raft = Raft::new(cfg.node_id, Arc::new(raft_cfg), net, log.clone(), sm).await?;
        if replicas.first() == Some(&cfg.node_id) && !raft.is_initialized().await? {
            let members: BTreeMap<NodeId, NodeInfo> =
                replicas.iter().map(|r| (*r, nodes.get(r).cloned().unwrap_or_default())).collect();
            match raft.initialize(members).await {
                Ok(()) => tracing::info!(shard = id, ?replicas, "initialized raft group"),
                Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {}
                Err(e) => return Err(e.into()),
            }
        }
        let metrics = raft.metrics();
        Ok(Arc::new(Self { id, raft, store, log, metrics, memory }))
    }

    pub fn is_leader(&self) -> bool {
        self.metrics.borrow().state == ServerState::Leader
    }

    pub fn metrics(&self) -> RaftMetrics<NodeId, NodeInfo> {
        self.metrics.borrow().clone()
    }

    async fn check_read(&self, consistency: ReadConsistency) -> Result<(), ExecError> {
        match consistency {
            ReadConsistency::Leader => {
                let m = self.metrics.borrow();
                if m.state == ServerState::Leader {
                    Ok(())
                } else {
                    Err(ExecError::NotLeader { leader: m.current_leader })
                }
            }
            ReadConsistency::Linearizable => match self.raft.ensure_linearizable().await {
                Ok(_) => Ok(()),
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                    Err(ExecError::NotLeader { leader: f.leader_id })
                }
                Err(e) => Err(ExecError::Unavailable(e.to_string())),
            },
        }
    }

    fn grows(op: &Op) -> bool {
        matches!(op, Op::Store { .. } | Op::Arith { vivify: Some(_), .. } | Op::MetaGet { vivify: Some(_), .. })
    }

    pub async fn write(&self, op: Op) -> Result<Reply, ExecError> {
        if Self::grows(&op) && self.memory.exhausted() {
            return Err(ExecError::OutOfMemory);
        }
        match self.raft.client_write(Write { now: unix_now(), op }).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                Err(ExecError::NotLeader { leader: f.leader_id })
            }
            Err(e) => Err(ExecError::Unavailable(e.to_string())),
        }
    }

    pub async fn execute(&self, op: ShardOp, consistency: ReadConsistency) -> Result<ShardResult, ExecError> {
        match op {
            ShardOp::Write(op) => self.write(op).await.map(ShardResult::Reply),
            ShardOp::Get { keys, bump } => {
                self.check_read(consistency).await?;
                let now = unix_now();
                Ok(ShardResult::Hits(keys.iter().map(|k| self.store.get(k, now, bump)).collect()))
            }
            ShardOp::MetaGet { key, touch, vivify, recache, bump } => {
                self.check_read(consistency).await?;
                let now = unix_now();
                let before = self.store.get(&key, now, bump);
                let needs_write = touch.is_some()
                    || (vivify.is_some() && before.is_none())
                    || self.store.meta_get_needs_write(&key, now, recache);
                if !needs_write {
                    return Ok(ShardResult::Meta(before.map(|h| MetaHit {
                        already_won: h.item.is_won(),
                        item: h.item,
                        fetched_before: h.fetched_before,
                        last_access: h.last_access,
                        won: false,
                    })));
                }
                match self.write(Op::MetaGet { key, touch, vivify, recache }).await? {
                    Reply::Item { item, won, already_won } => Ok(ShardResult::Meta(Some(MetaHit {
                        item,
                        fetched_before: before.as_ref().is_some_and(|h| h.fetched_before),
                        last_access: before.as_ref().map_or(now, |h| h.last_access),
                        won,
                        already_won,
                    }))),
                    Reply::NotFound => Ok(ShardResult::Meta(None)),
                    other => Err(ExecError::Internal(format!("unexpected reply {other:?}"))),
                }
            }
            ShardOp::Stats => Ok(ShardResult::Stats(self.store.stats())),
        }
    }

    /// Background upkeep: advance the replicated clock when idle and reclaim dead items.
    pub fn spawn_maintenance(self: &Arc<Self>) {
        let shard = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            let mut cursor = 0usize;
            let mut seconds = 0u64;
            loop {
                tick.tick().await;
                let Some(s) = shard.upgrade() else { return };
                seconds += 1;
                if seconds % 5 == 0 && s.is_leader() && s.store.apply_time() + 5 <= unix_now() {
                    let _ = s.raft.client_write(Write { now: unix_now(), op: Op::Tick }).await;
                }
                let n = s.store.stripe_count();
                let step = (n / 16).max(1);
                let store = s.store.clone();
                let start = cursor;
                let _ = tokio::task::spawn_blocking(move || store.sweep(start, step)).await;
                cursor = (cursor + step) % n;
            }
        });
    }
}

pub fn unexpected(r: impl std::fmt::Debug) -> ExecError {
    ExecError::Internal(format!("unexpected result {r:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(store: &Store, index: u64, key: &str, bytes: usize) {
        let op = Op::Store {
            mode: pmc_store::StoreMode::Set,
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::from(vec![b'x'; bytes]),
            flags: 0,
            exptime: 0,
            cas: None,
            invalidate: false,
        };
        assert!(matches!(store.apply(index, &Write { now: 1, op }), Reply::Stored { .. }));
    }

    /// The limit covers the whole node: two shards, each under the limit on its own, together
    /// exhaust it.
    #[test]
    fn memory_limit_is_per_node() {
        let budget = MemoryBudget::new(100_000);
        let a = Arc::new(Store::new(StoreConfig::default()));
        let b = Arc::new(Store::new(StoreConfig::default()));
        budget.register(a.clone());
        budget.register(b.clone());

        fill(&a, 1, "a", 60_000);
        assert!(a.used_bytes() < 100_000);
        assert!(!budget.exhausted());
        fill(&b, 1, "b", 60_000);
        assert!(b.used_bytes() < 100_000);
        assert!(budget.exhausted(), "used {} of 100000", budget.used());

        // Freeing memory in either shard lifts the limit again.
        let del = Op::Delete {
            key: Bytes::from_static(b"a"),
            cas: None,
            invalidate: false,
            exptime: None,
            remove_value: false,
        };
        a.apply(2, &Write { now: 1, op: del });
        assert!(!budget.exhausted());
    }

    #[test]
    fn zero_means_unlimited() {
        let budget = MemoryBudget::new(0);
        let a = Arc::new(Store::new(StoreConfig::default()));
        budget.register(a.clone());
        fill(&a, 1, "a", 200_000);
        assert!(!budget.exhausted());
    }
}
