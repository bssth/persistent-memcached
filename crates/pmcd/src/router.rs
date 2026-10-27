//! Routes operations to shards: executes locally on a shard leader, otherwise forwards the
//! operation to the node that leads (or hosts) the shard.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use parking_lot::RwLock;
use pmc_store::store::{Hit, StoreStats};
use pmc_store::{Op, Reply};

use crate::cluster::Topology;
use crate::config::ReadConsistency;
use crate::pb::cluster::ExecuteRequest;
use crate::raft::NodeId;
use crate::raft::network::Peers;
use crate::shard::{ExecError, MetaHit, Shard, ShardOp, ShardResult, unexpected};

const MAX_HOPS: u32 = 3;
const MAX_ATTEMPTS: u32 = 30;

pub struct Router {
    pub node_id: NodeId,
    pub topo: Arc<Topology>,
    local: RwLock<HashMap<u32, Arc<Shard>>>,
    peers: Arc<Peers>,
    /// Last known leader per shard (0 = unknown).
    hints: Vec<AtomicU64>,
    consistency: ReadConsistency,
}

impl Router {
    pub fn new(node_id: NodeId, topo: Arc<Topology>, peers: Arc<Peers>, consistency: ReadConsistency) -> Self {
        let hints = (0..topo.shards.len()).map(|_| AtomicU64::new(0)).collect();
        Self { node_id, topo, local: RwLock::new(HashMap::new()), peers, hints, consistency }
    }

    pub fn add_local(&self, shard: Arc<Shard>) {
        self.local.write().insert(shard.id, shard);
    }

    pub fn local(&self, id: u32) -> Option<Arc<Shard>> {
        self.local.read().get(&id).cloned()
    }

    pub fn local_shards(&self) -> Vec<Arc<Shard>> {
        let mut v: Vec<_> = self.local.read().values().cloned().collect();
        v.sort_by_key(|s| s.id);
        v
    }

    fn hint(&self, shard: u32) -> Option<NodeId> {
        match self.hints.get(shard as usize)?.load(Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    fn set_hint(&self, shard: u32, node: NodeId) {
        if let Some(h) = self.hints.get(shard as usize) {
            h.store(node, Relaxed);
        }
    }

    /// Executes `op` on `shard`, wherever its leader is. `hops` counts forwards so far.
    pub async fn execute(&self, shard: u32, op: ShardOp, hops: u32) -> Result<ShardResult, ExecError> {
        let spec = self.topo.shard(shard).ok_or_else(|| ExecError::Internal(format!("no shard {shard}")))?;
        let mut attempt = 0u32;
        loop {
            let leader = match self.local(shard) {
                Some(s) => match s.execute(op.clone(), self.consistency).await {
                    Err(ExecError::NotLeader { leader }) => leader,
                    other => return other,
                },
                None => self.hint(shard),
            };
            // Somebody else leads (or might): forward unless we are going in circles.
            let target = leader.filter(|l| *l != self.node_id).or_else(|| {
                let others: Vec<_> = spec.replicas.iter().copied().filter(|r| *r != self.node_id).collect();
                (!others.is_empty() && self.local(shard).is_none()).then(|| others[attempt as usize % others.len()])
            });
            if let Some(target) = target {
                if hops < MAX_HOPS {
                    match self.forward(target, shard, &op, hops + 1).await {
                        Ok(r) => {
                            self.set_hint(shard, target);
                            return Ok(r);
                        }
                        Err(ExecError::NotLeader { leader: Some(l) }) => self.set_hint(shard, l),
                        Err(ExecError::NotLeader { leader: None }) | Err(ExecError::Unavailable(_)) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
            attempt += 1;
            if attempt >= MAX_ATTEMPTS || hops > 0 {
                // A forwarded request gives up quickly; the origin node retries.
                return Err(ExecError::NotLeader { leader: self.hint(shard) });
            }
            // Election in progress: wait a little and retry.
            tokio::time::sleep(Duration::from_millis(20 * attempt.min(10) as u64)).await;
        }
    }

    async fn forward(&self, node: NodeId, shard: u32, op: &ShardOp, hops: u32) -> Result<ShardResult, ExecError> {
        let info = self.topo.node(node).ok_or_else(|| ExecError::Internal(format!("unknown node {node}")))?;
        let payload = postcard::to_stdvec(op).map_err(|e| ExecError::Internal(e.to_string()))?;
        let mut client = self.peers.client(&info.grpc);
        let mut req = tonic::Request::new(ExecuteRequest {
            shard,
            payload: payload.into(),
            hops,
            cluster_id: self.topo.cluster_id,
        });
        req.set_timeout(Duration::from_secs(10));
        let reply =
            client.execute(req).await.map_err(|s| ExecError::Unavailable(format!("node {node}: {}", s.message())))?;
        postcard::from_bytes::<Result<ShardResult, ExecError>>(&reply.into_inner().payload)
            .map_err(|e| ExecError::Internal(e.to_string()))?
    }

    pub fn shard_of(&self, key: &[u8]) -> u32 {
        self.topo.shard_of(key)
    }

    pub async fn write(&self, op: Op) -> Result<Reply, ExecError> {
        let key = op.key().ok_or_else(|| ExecError::Internal("keyless op".into()))?;
        let shard = self.shard_of(key);
        match self.execute(shard, ShardOp::Write(op), 0).await? {
            ShardResult::Reply(r) => Ok(r),
            other => Err(unexpected(other)),
        }
    }

    /// Multi-key get, split by shard and run in parallel. Results keep the order of `keys`.
    pub async fn get_many(&self, keys: &[Bytes], bump: bool) -> Result<Vec<Option<Hit>>, ExecError> {
        let mut groups: BTreeMap<u32, (Vec<usize>, Vec<Bytes>)> = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            let g = groups.entry(self.shard_of(k)).or_default();
            g.0.push(i);
            g.1.push(k.clone());
        }
        let mut out = vec![None; keys.len()];
        let runs = groups.into_iter().map(|(shard, (idx, keys))| async move {
            (idx, self.execute(shard, ShardOp::Get { keys, bump }, 0).await)
        });
        for (idx, res) in join_all(runs).await {
            match res? {
                ShardResult::Hits(hits) => {
                    for (i, h) in idx.into_iter().zip(hits) {
                        out[i] = h;
                    }
                }
                other => return Err(unexpected(other)),
            }
        }
        Ok(out)
    }

    pub async fn meta_get(
        &self,
        key: Bytes,
        touch: Option<i64>,
        vivify: Option<i64>,
        recache: Option<u64>,
        bump: bool,
    ) -> Result<Option<MetaHit>, ExecError> {
        let shard = self.shard_of(&key);
        match self.execute(shard, ShardOp::MetaGet { key, touch, vivify, recache, bump }, 0).await? {
            ShardResult::Meta(m) => Ok(m),
            other => Err(unexpected(other)),
        }
    }

    /// `flush_all` goes to every shard.
    pub async fn flush_all(&self, delay: u64) -> Result<(), ExecError> {
        let runs = (0..self.topo.shards.len() as u32).map(|s| self.execute(s, ShardOp::Write(Op::Flush { delay }), 0));
        for r in join_all(runs).await {
            r?;
        }
        Ok(())
    }

    /// Item counters summed over all shards (as seen by their leaders).
    pub async fn store_stats(&self) -> StoreStats {
        let runs = (0..self.topo.shards.len() as u32).map(|s| self.execute(s, ShardOp::Stats, 0));
        let mut total = StoreStats::default();
        for r in join_all(runs).await {
            if let Ok(ShardResult::Stats(s)) = r {
                total.curr_items += s.curr_items;
                total.bytes += s.bytes;
                total.total_items += s.total_items;
            }
        }
        total
    }
}
