//! pmcd — persistent, replicated, sharded memcached-compatible server.

// openraft's storage traits fix `StorageError` (~224 bytes) as the error type.
#![allow(clippy::result_large_err)]

mod cluster;
mod config;
mod grpc;
mod mc;
mod raft;
mod router;
mod shard;
mod stats;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser as _;
use futures::future::join_all;
use pmc_proto::Limits;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use crate::cluster::Topology;
use crate::config::Config;
use crate::raft::network::{MAX_MESSAGE, Peers};
use crate::router::Router;
use crate::shard::{MemoryBudget, Shard};

pub mod pb {
    pub mod v1 {
        tonic::include_proto!("pmc.v1");
    }
    pub mod cluster {
        tonic::include_proto!("pmc.cluster.v1");
    }
}

#[derive(clap::Parser, Debug)]
#[command(version, about = "Persistent, replicated, sharded memcached")]
struct Args {
    /// TOML config file. Without one, pmcd runs as a single-node cluster.
    #[arg(short, long)]
    config: Option<PathBuf>,
    #[arg(long)]
    node_id: Option<u64>,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// memcached text protocol listen address.
    #[arg(short = 'l', long)]
    listen: Option<String>,
    /// gRPC listen address.
    #[arg(long)]
    grpc: Option<String>,
    /// Number of shards (single-node mode or bootstrap).
    #[arg(long)]
    shards: Option<u32>,
    /// Do not fsync the log (faster, loses recent writes on power failure).
    #[arg(long)]
    no_fsync: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,openraft=warn".into()))
        .init();
    let args = Args::parse();
    let mut cfg = match &args.config {
        Some(p) => Config::load(p)?,
        None => Config::default(),
    };
    if let Some(v) = args.node_id {
        cfg.node_id = v;
    }
    if let Some(v) = args.data_dir {
        cfg.data_dir = v;
    }
    if let Some(v) = args.listen {
        cfg.memcache_listen = v;
    }
    if let Some(v) = args.grpc {
        cfg.grpc_listen = v;
    }
    if let Some(v) = args.shards {
        cfg.cluster.shards = v;
    }
    if args.no_fsync {
        cfg.fsync = false;
    }
    run(cfg.finish()?).await
}

async fn run(cfg: Config) -> anyhow::Result<()> {
    let topo = Arc::new(Topology::from_config(&cfg.cluster, cfg.max_item_size));
    let peers = Arc::new(Peers::default());
    let router = Arc::new(Router::new(cfg.node_id, topo.clone(), peers.clone(), cfg.read_consistency));
    let ctx = Arc::new(mc::Ctx {
        router: router.clone(),
        stats: Arc::default(),
        limits: Limits { max_item_size: cfg.max_item_size, ..Limits::default() },
        memory_limit: cfg.memory_limit_mb << 20,
    });

    // Bind both ports before touching any data, so a busy port fails fast.
    let grpc_addr: SocketAddr = cfg.grpc_listen.parse().with_context(|| format!("grpc_listen {}", cfg.grpc_listen))?;
    let grpc_listener = TcpListener::bind(grpc_addr).await.with_context(|| format!("binding {grpc_addr}"))?;
    let listener =
        TcpListener::bind(&cfg.memcache_listen).await.with_context(|| format!("binding {}", cfg.memcache_listen))?;

    // gRPC goes up first: peers need it to elect leaders while our shards start.
    let grpc = tonic::transport::Server::builder()
        .add_service(
            pb::v1::memcache_server::MemcacheServer::new(grpc::MemcacheService { ctx: ctx.clone() })
                .max_decoding_message_size(cfg.max_item_size + (64 << 10)),
        )
        .add_service(
            pb::cluster::cluster_server::ClusterServer::new(grpc::ClusterService { router: router.clone() })
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
        )
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(grpc_listener));
    let mut grpc_task = tokio::spawn(grpc);

    let mine: Vec<_> = topo.shards.iter().filter(|s| s.replicas.contains(&cfg.node_id)).collect();
    let memory = Arc::new(MemoryBudget::new((cfg.memory_limit_mb as usize) << 20));
    let opened =
        join_all(mine.iter().map(|s| {
            Shard::open(s.id, &cfg, &s.replicas, &topo.nodes, topo.cluster_id, peers.clone(), memory.clone())
        }))
        .await;
    for s in opened {
        let s = s?;
        s.spawn_maintenance();
        router.add_local(s);
    }

    tracing::info!(
        cluster_id = format!("{:016x}", topo.cluster_id),
        node = cfg.node_id,
        memcache = %cfg.memcache_listen,
        grpc = %grpc_addr,
        shards = topo.shards.len(),
        local_shards = mine.len(),
        data = %cfg.data_dir.display(),
        "pmcd started"
    );
    tokio::spawn(mc::serve(listener, ctx));

    let terminate = async {
        #[cfg(unix)]
        {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal");
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::select! {
        _ = terminate => tracing::info!("shutting down"),
        r = &mut grpc_task => { r??; }
    }
    for s in router.local_shards() {
        let _ = s.raft.shutdown().await;
    }
    Ok(())
}
