//! Raft state machine: applies log entries to a [`Store`] and manages snapshot files.
//!
//! Snapshots are `<snapshot_id>.snap` files in pmc-store's snapshot format; the openraft
//! `SnapshotMeta` is kept in the snapshot header. The newest file is the current snapshot.

use std::fs;
use std::io::{BufReader, BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::storage::RaftStateMachine;
use openraft::{
    AnyError, EntryPayload, OptionalSend, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageIOError, StoredMembership,
};
use pmc_store::store::Image;
use pmc_store::{Reply, Store, snapshot};
use tokio::io::AsyncWriteExt;

use super::{Entry, LogId, NodeId, NodeInfo, StorageError, TypeConfig};

type Meta = SnapshotMeta<NodeId, NodeInfo>;

fn snap_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::write_snapshot(None, AnyError::new(&e)).into()
}

fn read_snap_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::read_snapshot(None, AnyError::new(&e)).into()
}

pub struct StateMachine {
    store: Arc<Store>,
    dir: PathBuf,
    last_applied: Option<LogId>,
    membership: StoredMembership<NodeId, NodeInfo>,
    receiving: Option<PathBuf>,
    seq: u64,
}

/// Snapshot files sorted oldest first. Ids are `<last index, 20 digits>-<seq>`.
fn snapshot_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "snap"))
        .collect();
    v.sort();
    Ok(v)
}

fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(f) = fs::File::open(dir) {
        let _ = f.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

fn safe_id(id: &str) -> bool {
    !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

/// Loads a snapshot file into `store`, returning its metadata.
fn load(store: &Store, path: &Path) -> std::io::Result<Meta> {
    let header = snapshot::read_header(BufReader::new(fs::File::open(path)?))?;
    store.reset(header.clocks);
    let header = snapshot::read(BufReader::with_capacity(1 << 20, fs::File::open(path)?), |k, it| store.load(k, it))?;
    postcard::from_bytes(&header.meta).map_err(std::io::Error::other)
}

fn read_meta(path: &Path) -> std::io::Result<Meta> {
    let header = snapshot::read_header(BufReader::new(fs::File::open(path)?))?;
    postcard::from_bytes(&header.meta).map_err(std::io::Error::other)
}

/// Removes all snapshots but the newest.
fn prune(dir: &Path) {
    if let Ok(files) = snapshot_files(dir) {
        for old in files.iter().rev().skip(1) {
            let _ = fs::remove_file(old);
        }
    }
}

impl StateMachine {
    pub fn open(dir: &Path, store: Arc<Store>) -> Result<Self, StorageError> {
        fs::create_dir_all(dir).map_err(snap_err)?;
        for e in fs::read_dir(dir).map_err(read_snap_err)?.flatten() {
            if e.path().extension().is_some_and(|x| x == "tmp") {
                let _ = fs::remove_file(e.path());
            }
        }
        let mut sm = Self {
            store,
            dir: dir.to_path_buf(),
            last_applied: None,
            membership: StoredMembership::default(),
            receiving: None,
            seq: 0,
        };
        if let Some(path) = snapshot_files(dir).map_err(read_snap_err)?.pop() {
            let meta = load(&sm.store, &path).map_err(read_snap_err)?;
            tracing::info!(path = %path.display(), last = ?meta.last_log_id, items = sm.store.stats().curr_items, "loaded snapshot");
            sm.last_applied = meta.last_log_id;
            sm.membership = meta.last_membership;
        }
        Ok(sm)
    }
}

pub struct SnapshotBuilder {
    image: Option<Image>,
    last_applied: Option<LogId>,
    membership: StoredMembership<NodeId, NodeInfo>,
    dir: PathBuf,
    seq: u64,
}

fn write_file(dir: &Path, path: &Path, image: &Image, meta: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut w = BufWriter::with_capacity(1 << 20, fs::File::create(&tmp)?);
        snapshot::write(&mut w, image, meta)?;
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    fs::rename(&tmp, path)?;
    sync_dir(dir);
    Ok(())
}

impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let image = self.image.take().expect("build_snapshot is called once");
        let index = self.last_applied.map(|l| l.index).unwrap_or(0);
        let meta = Meta {
            last_log_id: self.last_applied,
            last_membership: self.membership.clone(),
            snapshot_id: format!("{index:020}-{:06}", self.seq),
        };
        let meta_bytes = postcard::to_stdvec(&meta).map_err(snap_err)?;
        let dir = self.dir.clone();
        let path = dir.join(format!("{}.snap", meta.snapshot_id));
        let items = image.items.len();
        let p = path.clone();
        tokio::task::spawn_blocking(move || {
            write_file(&dir, &p, &image, &meta_bytes)?;
            prune(&dir);
            Ok::<_, std::io::Error>(())
        })
        .await
        .map_err(|e| snap_err(std::io::Error::other(e)))?
        .map_err(snap_err)?;
        tracing::info!(id = %meta.snapshot_id, items, "built snapshot");
        let f = tokio::fs::File::open(&path).await.map_err(read_snap_err)?;
        Ok(Snapshot { meta, snapshot: Box::new(f) })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership<NodeId, NodeInfo>), StorageError> {
        Ok((self.last_applied, self.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Reply>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut out = Vec::new();
        for e in entries {
            self.last_applied = Some(e.log_id);
            out.push(match e.payload {
                EntryPayload::Blank => Reply::Ok,
                EntryPayload::Normal(w) => self.store.apply(e.log_id.index, &w),
                EntryPayload::Membership(m) => {
                    self.membership = StoredMembership::new(Some(e.log_id), m);
                    Reply::Ok
                }
            });
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Copy the items here, between two applies, so the snapshot is point-in-time;
        // writing the file happens later in a separate task.
        self.seq += 1;
        SnapshotBuilder {
            image: Some(self.store.image()),
            last_applied: self.last_applied,
            membership: self.membership.clone(),
            dir: self.dir.clone(),
            seq: self.seq,
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<tokio::fs::File>, StorageError> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
        let path = self.dir.join(format!("recv-{nanos}.tmp"));
        let f = tokio::fs::File::create(&path).await.map_err(snap_err)?;
        self.receiving = Some(path);
        Ok(Box::new(f))
    }

    async fn install_snapshot(&mut self, meta: &Meta, snapshot: Box<tokio::fs::File>) -> Result<(), StorageError> {
        let mut f = *snapshot;
        f.flush().await.map_err(snap_err)?;
        f.sync_all().await.map_err(snap_err)?;
        drop(f);
        let recv =
            self.receiving.take().ok_or_else(|| snap_err(std::io::Error::other("no snapshot being received")))?;
        if !safe_id(&meta.snapshot_id) {
            return Err(snap_err(std::io::Error::other("bad snapshot id")));
        }
        let path = self.dir.join(format!("{}.snap", meta.snapshot_id));
        let store = self.store.clone();
        let dir = self.dir.clone();
        let loaded = tokio::task::block_in_place(|| {
            fs::rename(&recv, &path)?;
            sync_dir(&dir);
            let loaded = load(&store, &path)?;
            prune(&dir);
            Ok::<_, std::io::Error>(loaded)
        })
        .map_err(snap_err)?;
        tracing::info!(id = %meta.snapshot_id, last = ?loaded.last_log_id, items = self.store.stats().curr_items, "installed snapshot");
        self.last_applied = meta.last_log_id;
        self.membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        let Some(path) = snapshot_files(&self.dir).map_err(read_snap_err)?.pop() else { return Ok(None) };
        let meta = read_meta(&path).map_err(read_snap_err)?;
        let f = tokio::fs::File::open(&path).await.map_err(read_snap_err)?;
        Ok(Some(Snapshot { meta, snapshot: Box::new(f) }))
    }
}
