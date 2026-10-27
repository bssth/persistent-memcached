//! Raft glue: openraft types, log storage on top of the WAL, the state machine with snapshots,
//! and the gRPC transport.

pub mod log_store;
pub mod network;
pub mod state_machine;

#[allow(unused_imports)]
use std::io::Cursor;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    /// gRPC address other nodes connect to.
    pub grpc: String,
    /// memcached text protocol address (informational).
    pub memcache: String,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = pmc_store::Write,
        R = pmc_store::Reply,
        NodeId = u64,
        Node = NodeInfo,
        SnapshotData = tokio::fs::File,
);

pub type NodeId = u64;
pub type Raft = openraft::Raft<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type StorageError = openraft::StorageError<NodeId>;

fn io_error(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

/// Small metadata files (vote, purge point): `postcard payload | crc32c`, replaced atomically.
pub(crate) mod meta_file {
    use std::fs;
    use std::io::{self, Write};
    use std::path::Path;

    use serde::Serialize;
    use serde::de::DeserializeOwned;

    pub fn write<T: Serialize>(path: &Path, value: &T, sync: bool) -> io::Result<()> {
        let mut buf = postcard::to_stdvec(value).map_err(super::io_error)?;
        let crc = crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        let tmp = path.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&buf)?;
            if sync {
                f.sync_all()?;
            }
        }
        fs::rename(&tmp, path)?;
        if sync {
            #[cfg(unix)]
            if let Some(dir) = path.parent() {
                fs::File::open(dir)?.sync_all()?;
            }
        }
        Ok(())
    }

    /// Returns `None` if the file is missing or damaged.
    pub fn read<T: DeserializeOwned>(path: &Path) -> Option<T> {
        let buf = fs::read(path).ok()?;
        if buf.len() < 4 {
            return None;
        }
        let (body, crc) = buf.split_at(buf.len() - 4);
        if crc32c(body).to_le_bytes() != crc {
            tracing::warn!(path = %path.display(), "ignoring damaged metadata file");
            return None;
        }
        postcard::from_bytes(body).ok()
    }

    fn crc32c(b: &[u8]) -> u32 {
        // Same CRC as the WAL, without pulling the crate into pmcd directly.
        let mut crc = !0u32;
        for &x in b {
            crc ^= x as u32;
            for _ in 0..8 {
                crc = if crc & 1 != 0 { (crc >> 1) ^ 0x82F6_3B78 } else { crc >> 1 };
            }
        }
        !crc
    }
}
