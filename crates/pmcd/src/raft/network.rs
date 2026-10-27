//! Raft transport over the internal gRPC `Cluster` service. Requests and replies are
//! postcard-encoded openraft types.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{Fatal, InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, Endpoint};

use super::{NodeId, NodeInfo, TypeConfig};
use crate::pb::cluster::cluster_client::ClusterClient;
use crate::pb::cluster::{RaftKind, RaftRequest};

/// Max gRPC message size between nodes (Raft batches can carry many 1 MiB values).
pub const MAX_MESSAGE: usize = 256 << 20;

/// Lazily connected gRPC channels to other nodes, shared by all shards.
#[derive(Default)]
pub struct Peers {
    channels: Mutex<HashMap<String, Channel>>,
}

impl Peers {
    pub fn client(&self, addr: &str) -> ClusterClient<Channel> {
        let mut map = self.channels.lock();
        let channel = map
            .entry(addr.to_string())
            .or_insert_with(|| {
                Endpoint::from_shared(format!("http://{addr}"))
                    .expect("valid peer address")
                    .connect_timeout(Duration::from_secs(2))
                    .tcp_nodelay(true)
                    .http2_keep_alive_interval(Duration::from_secs(10))
                    .connect_lazy()
            })
            .clone();
        ClusterClient::new(channel).max_decoding_message_size(MAX_MESSAGE).max_encoding_message_size(MAX_MESSAGE)
    }
}

pub struct Network {
    pub shard: u32,
    pub cluster_id: u64,
    pub peers: Arc<Peers>,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetClient;

    async fn new_client(&mut self, target: NodeId, node: &NodeInfo) -> NetClient {
        NetClient { shard: self.shard, cluster_id: self.cluster_id, target, client: self.peers.client(&node.grpc) }
    }
}

pub struct NetClient {
    shard: u32,
    cluster_id: u64,
    target: NodeId,
    client: ClusterClient<Channel>,
}

type RpcResult<T, E = openraft::error::Infallible> = Result<T, RPCError<NodeId, NodeInfo, RaftError<NodeId, E>>>;

/// Uninhabited stand-in for `Infallible`, which serde cannot deserialize.
#[derive(Debug, Serialize, Deserialize)]
pub enum Never {}

/// A Raft RPC result as sent over the wire.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
pub enum WireResult<T, E> {
    Ok(T),
    Fatal(Fatal<NodeId>),
    Api(E),
}

impl<T, E> From<Result<T, RaftError<NodeId, E>>> for WireResult<T, E>
where
    E: std::error::Error,
{
    fn from(r: Result<T, RaftError<NodeId, E>>) -> Self {
        match r {
            Ok(v) => WireResult::Ok(v),
            Err(RaftError::Fatal(f)) => WireResult::Fatal(f),
            Err(RaftError::APIError(e)) => WireResult::Api(e),
        }
    }
}

/// For RPCs whose API error is `Infallible` (append entries, vote).
pub fn wire_infallible<T>(r: Result<T, RaftError<NodeId>>) -> WireResult<T, Never> {
    match r {
        Ok(v) => WireResult::Ok(v),
        Err(RaftError::Fatal(f)) => WireResult::Fatal(f),
        Err(RaftError::APIError(never)) => match never {},
    }
}

enum Transport {
    Unreachable(Unreachable),
    Network(NetworkError),
}

fn lift<E: std::error::Error>(t: Transport) -> RPCError<NodeId, NodeInfo, RaftError<NodeId, E>> {
    match t {
        Transport::Unreachable(u) => RPCError::Unreachable(u),
        Transport::Network(n) => RPCError::Network(n),
    }
}

impl NetClient {
    async fn call<Req, Resp, W>(
        &mut self,
        kind: RaftKind,
        req: &Req,
        timeout: Duration,
    ) -> Result<WireResult<Resp, W>, Transport>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        W: DeserializeOwned,
    {
        let payload = postcard::to_stdvec(req).map_err(|e| Transport::Network(NetworkError::new(&e)))?;
        let mut request = tonic::Request::new(RaftRequest {
            shard: self.shard,
            kind: kind as i32,
            payload: payload.into(),
            cluster_id: self.cluster_id,
        });
        request.set_timeout(timeout);
        let reply = self.client.raft(request).await.map_err(|s| Transport::Unreachable(Unreachable::new(&s)))?;
        postcard::from_bytes(&reply.into_inner().payload).map_err(|e| Transport::Network(NetworkError::new(&e)))
    }

    fn remote<E: std::error::Error>(
        &self,
        e: RaftError<NodeId, E>,
    ) -> RPCError<NodeId, NodeInfo, RaftError<NodeId, E>> {
        RPCError::RemoteError(RemoteError::new(self.target, e))
    }
}

impl RaftNetwork<TypeConfig> for NetClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<NodeId>> {
        match self.call::<_, _, Never>(RaftKind::Append, &rpc, option.hard_ttl()).await.map_err(lift)? {
            WireResult::Ok(v) => Ok(v),
            WireResult::Fatal(f) => Err(self.remote(RaftError::Fatal(f))),
            WireResult::Api(never) => match never {},
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<NodeId>, InstallSnapshotError> {
        match self.call(RaftKind::Snapshot, &rpc, option.hard_ttl()).await.map_err(lift)? {
            WireResult::Ok(v) => Ok(v),
            WireResult::Fatal(f) => Err(self.remote(RaftError::Fatal(f))),
            WireResult::Api(e) => Err(self.remote(RaftError::APIError(e))),
        }
    }

    async fn vote(&mut self, rpc: VoteRequest<NodeId>, option: RPCOption) -> RpcResult<VoteResponse<NodeId>> {
        match self.call::<_, _, Never>(RaftKind::Vote, &rpc, option.hard_ttl()).await.map_err(lift)? {
            WireResult::Ok(v) => Ok(v),
            WireResult::Fatal(f) => Err(self.remote(RaftError::Fatal(f))),
            WireResult::Api(never) => match never {},
        }
    }
}
