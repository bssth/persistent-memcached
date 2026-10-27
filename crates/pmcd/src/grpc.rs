//! gRPC services: the public `pmc.v1.Memcache` API and the internal `pmc.cluster.v1.Cluster`
//! node-to-node API (Raft traffic and forwarded shard operations).

use std::sync::Arc;

use bytes::Bytes;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use pmc_proto::MAX_KEY_LEN;
use pmc_store::{Op, Reply, StoreMode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::{Request, Response, Status};

use crate::mc::Ctx;
use crate::pb::cluster::cluster_server::Cluster;
use crate::pb::cluster::{ExecuteReply, ExecuteRequest, RaftKind, RaftReply, RaftRequest, StatusReply, StatusRequest};
use crate::pb::v1 as pb;
use crate::pb::v1::memcache_server::Memcache;
use crate::raft::network::{WireResult, wire_infallible};
use crate::raft::{NodeId, TypeConfig};
use crate::router::Router;
use crate::shard::{ExecError, ShardOp, unix_now};

fn status_of(e: ExecError) -> Status {
    match e {
        ExecError::OutOfMemory => Status::resource_exhausted(e.to_string()),
        ExecError::NotLeader { .. } | ExecError::Unavailable(_) => Status::unavailable(e.to_string()),
        ExecError::Internal(_) => Status::internal(e.to_string()),
    }
}

fn check_key(key: &[u8]) -> Result<(), Status> {
    if key.is_empty() || key.len() > MAX_KEY_LEN {
        return Err(Status::invalid_argument(format!("key must be 1..={MAX_KEY_LEN} bytes")));
    }
    Ok(())
}

fn ttl_of(exptime: u64, now: u64) -> i64 {
    if exptime == 0 { -1 } else { exptime.saturating_sub(now) as i64 }
}

pub struct MemcacheService {
    pub ctx: Arc<Ctx>,
}

impl MemcacheService {
    async fn arith(&self, req: pb::ArithRequest, incr: bool) -> Result<Response<pb::ArithResponse>, Status> {
        check_key(&req.key)?;
        let op = Op::Arith {
            key: req.key,
            incr,
            delta: req.delta,
            cas: None,
            vivify: req.initial.map(|v| (v, req.initial_exptime)),
            exptime: None,
        };
        match self.ctx.router.write(op).await.map_err(status_of)? {
            Reply::Arith { value, item } => Ok(Response::new(pb::ArithResponse { found: true, value, cas: item.cas })),
            Reply::NotFound => Ok(Response::new(pb::ArithResponse { found: false, value: 0, cas: 0 })),
            Reply::NonNumeric => Err(Status::failed_precondition("cannot increment or decrement non-numeric value")),
            other => Err(Status::internal(format!("unexpected reply {other:?}"))),
        }
    }
}

#[tonic::async_trait]
impl Memcache for MemcacheService {
    async fn get(&self, req: Request<pb::GetRequest>) -> Result<Response<pb::GetResponse>, Status> {
        let req = req.into_inner();
        for k in &req.keys {
            check_key(k)?;
        }
        let now = unix_now();
        let mut items = Vec::new();
        match req.touch_exptime {
            Some(exptime) => {
                for key in req.keys {
                    let reply =
                        self.ctx.router.write(Op::Touch { key: key.clone(), exptime }).await.map_err(status_of)?;
                    if let Reply::Item { item, .. } = reply {
                        items.push(pb::Item {
                            key,
                            ttl: ttl_of(item.exptime, now),
                            value: item.value,
                            flags: item.flags,
                            cas: item.cas,
                        });
                    }
                }
            }
            None => {
                let hits = self.ctx.router.get_many(&req.keys, true).await.map_err(status_of)?;
                for (key, h) in req.keys.into_iter().zip(hits) {
                    if let Some(h) = h {
                        items.push(pb::Item {
                            key,
                            ttl: ttl_of(h.item.exptime, now),
                            value: h.item.value,
                            flags: h.item.flags,
                            cas: h.item.cas,
                        });
                    }
                }
            }
        }
        Ok(Response::new(pb::GetResponse { items }))
    }

    async fn set(&self, req: Request<pb::SetRequest>) -> Result<Response<pb::SetResponse>, Status> {
        let req = req.into_inner();
        check_key(&req.key)?;
        if !self.ctx.limits.fits(req.key.len(), req.value.len()) {
            return Err(Status::resource_exhausted("object too large for cache"));
        }
        let mode = match pb::StoreMode::try_from(req.mode).map_err(|_| Status::invalid_argument("bad mode"))? {
            pb::StoreMode::Set => StoreMode::Set,
            pb::StoreMode::Add => StoreMode::Add,
            pb::StoreMode::Replace => StoreMode::Replace,
            pb::StoreMode::Append => StoreMode::Append,
            pb::StoreMode::Prepend => StoreMode::Prepend,
        };
        let op = Op::Store {
            mode,
            key: req.key,
            value: req.value,
            flags: req.flags,
            exptime: req.exptime,
            cas: req.cas,
            invalidate: false,
        };
        let (status, cas) = match self.ctx.router.write(op).await.map_err(status_of)? {
            Reply::Stored { cas, .. } => (pb::StoreStatus::Stored, cas),
            Reply::NotStored => (pb::StoreStatus::NotStored, 0),
            Reply::Exists => (pb::StoreStatus::Exists, 0),
            Reply::NotFound => (pb::StoreStatus::NotFound, 0),
            Reply::TooLarge => return Err(Status::resource_exhausted("object too large for cache")),
            other => return Err(Status::internal(format!("unexpected reply {other:?}"))),
        };
        Ok(Response::new(pb::SetResponse { status: status as i32, cas }))
    }

    async fn delete(&self, req: Request<pb::DeleteRequest>) -> Result<Response<pb::DeleteResponse>, Status> {
        let req = req.into_inner();
        check_key(&req.key)?;
        let op = Op::Delete { key: req.key, cas: req.cas, invalidate: false, exptime: None, remove_value: false };
        let status = match self.ctx.router.write(op).await.map_err(status_of)? {
            Reply::Deleted => pb::DeleteStatus::Deleted,
            Reply::Exists => pb::DeleteStatus::DeleteExists,
            _ => pb::DeleteStatus::DeleteNotFound,
        };
        Ok(Response::new(pb::DeleteResponse { status: status as i32 }))
    }

    async fn incr(&self, req: Request<pb::ArithRequest>) -> Result<Response<pb::ArithResponse>, Status> {
        self.arith(req.into_inner(), true).await
    }

    async fn decr(&self, req: Request<pb::ArithRequest>) -> Result<Response<pb::ArithResponse>, Status> {
        self.arith(req.into_inner(), false).await
    }

    async fn touch(&self, req: Request<pb::TouchRequest>) -> Result<Response<pb::TouchResponse>, Status> {
        let req = req.into_inner();
        check_key(&req.key)?;
        let found = matches!(
            self.ctx.router.write(Op::Touch { key: req.key, exptime: req.exptime }).await.map_err(status_of)?,
            Reply::Item { .. }
        );
        Ok(Response::new(pb::TouchResponse { found }))
    }

    async fn flush_all(&self, req: Request<pb::FlushAllRequest>) -> Result<Response<pb::FlushAllResponse>, Status> {
        let delay = req.into_inner().delay.max(0) as u64;
        self.ctx.router.flush_all(delay).await.map_err(status_of)?;
        Ok(Response::new(pb::FlushAllResponse {}))
    }

    async fn stats(&self, _: Request<pb::StatsRequest>) -> Result<Response<pb::StatsResponse>, Status> {
        let mut stats: std::collections::HashMap<String, String> =
            self.ctx.stats.counters.snapshot().into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let items = self.ctx.router.store_stats().await;
        stats.insert("curr_items".into(), items.curr_items.to_string());
        stats.insert("total_items".into(), items.total_items.to_string());
        stats.insert("bytes".into(), items.bytes.to_string());
        stats.insert("version".into(), crate::mc::VERSION.into());
        Ok(Response::new(pb::StatsResponse { stats }))
    }
}

pub struct ClusterService {
    pub router: Arc<Router>,
}

impl ClusterService {
    fn check_cluster(&self, id: u64) -> Result<(), Status> {
        if id == self.router.topo.cluster_id {
            Ok(())
        } else {
            Err(Status::failed_precondition(format!(
                "request from a different cluster ({id:016x}, this node is in {:016x}); \
                 check that [cluster] and max_item_size are the same on all nodes",
                self.router.topo.cluster_id
            )))
        }
    }
}

fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, Status> {
    postcard::from_bytes(b).map_err(|e| Status::invalid_argument(format!("bad payload: {e}")))
}

fn encode<T: Serialize>(v: &T) -> Result<Bytes, Status> {
    postcard::to_stdvec(v).map(Bytes::from).map_err(|e| Status::internal(e.to_string()))
}

#[derive(Serialize)]
struct ShardStatus {
    id: u32,
    replicas: Vec<NodeId>,
    slots: (u16, u16),
    local: bool,
    state: Option<String>,
    leader: Option<NodeId>,
    term: Option<u64>,
    last_applied: Option<u64>,
    log_first: Option<u64>,
    log_last: Option<u64>,
    log_bytes: Option<u64>,
    items: Option<u64>,
}

#[tonic::async_trait]
impl Cluster for ClusterService {
    async fn raft(&self, req: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let req = req.into_inner();
        self.check_cluster(req.cluster_id)?;
        let shard = self
            .router
            .local(req.shard)
            .ok_or_else(|| Status::unavailable(format!("shard {} is not open on this node", req.shard)))?;
        let kind = RaftKind::try_from(req.kind).map_err(|_| Status::invalid_argument("bad raft rpc kind"))?;
        let payload = match kind {
            RaftKind::Append => {
                let rpc: AppendEntriesRequest<TypeConfig> = decode(&req.payload)?;
                encode(&wire_infallible(shard.raft.append_entries(rpc).await))?
            }
            RaftKind::Vote => {
                let rpc: VoteRequest<NodeId> = decode(&req.payload)?;
                encode(&wire_infallible(shard.raft.vote(rpc).await))?
            }
            RaftKind::Snapshot => {
                let rpc: InstallSnapshotRequest<TypeConfig> = decode(&req.payload)?;
                encode(&WireResult::from(shard.raft.install_snapshot(rpc).await))?
            }
        };
        Ok(Response::new(RaftReply { payload }))
    }

    async fn execute(&self, req: Request<ExecuteRequest>) -> Result<Response<ExecuteReply>, Status> {
        let req = req.into_inner();
        self.check_cluster(req.cluster_id)?;
        let op: ShardOp = decode(&req.payload)?;
        let res = self.router.execute(req.shard, op, req.hops).await;
        Ok(Response::new(ExecuteReply { payload: encode(&res)? }))
    }

    async fn status(&self, _: Request<StatusRequest>) -> Result<Response<StatusReply>, Status> {
        let shards: Vec<ShardStatus> = self
            .router
            .topo
            .shards
            .iter()
            .map(|spec| {
                let local = self.router.local(spec.id);
                let m = local.as_ref().map(|s| s.metrics());
                let log = local.as_ref().map(|s| s.log.stats());
                ShardStatus {
                    id: spec.id,
                    replicas: spec.replicas.clone(),
                    slots: spec.slots,
                    local: local.is_some(),
                    state: m.as_ref().map(|m| format!("{:?}", m.state)),
                    leader: m.as_ref().and_then(|m| m.current_leader),
                    term: m.as_ref().map(|m| m.current_term),
                    last_applied: m.as_ref().and_then(|m| m.last_applied.map(|l| l.index)),
                    log_first: log.and_then(|l| l.0),
                    log_last: log.and_then(|l| l.1),
                    log_bytes: log.map(|l| l.2),
                    items: local.as_ref().map(|s| s.store.stats().curr_items),
                }
            })
            .collect();
        let json = serde_json::json!({ "node_id": self.router.node_id, "shards": shards }).to_string();
        Ok(Response::new(StatusReply { json }))
    }
}
