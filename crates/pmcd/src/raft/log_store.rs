//! Raft log storage on top of the pmc-store WAL.
//!
//! Directory layout:
//! * `wal/` — log entries (postcard-encoded `openraft::Entry`), one WAL record per entry;
//! * `vote` — the current vote (fsynced: Raft safety depends on it);
//! * `purged` — the last purged log id (fsynced before segments are deleted);
//! * `committed` — a recent committed id (not fsynced, written at most once a second; only
//!   speeds up restarts).

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{AnyError, LogState, OptionalSend, RaftLogReader, StorageIOError, Vote};
use parking_lot::Mutex;
use pmc_store::wal::{Wal, WalOptions};

use super::{Entry, LogId, NodeId, StorageError, TypeConfig, meta_file};

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    wal: Wal,
    dir: PathBuf,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId>,
    purged: Option<LogId>,
    /// Log id of the last entry in the WAL.
    last: Option<LogId>,
    committed_saved_at: Option<Instant>,
}

fn write_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::write_logs(AnyError::new(&e)).into()
}

fn read_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::read_logs(AnyError::new(&e)).into()
}

fn vote_err(e: impl std::error::Error + 'static) -> StorageError {
    StorageIOError::write_vote(AnyError::new(&e)).into()
}

fn decode(bytes: &[u8]) -> Result<Entry, StorageError> {
    postcard::from_bytes(bytes).map_err(read_err)
}

impl Inner {
    fn entry_at(&self, index: u64) -> Result<Option<Entry>, StorageError> {
        match self.wal.read(index).map_err(read_err)? {
            Some(b) => Ok(Some(decode(&b)?)),
            None => Ok(None),
        }
    }

    fn refresh_last(&mut self) -> Result<(), StorageError> {
        self.last = match self.wal.last_index() {
            Some(i) => self.entry_at(i)?.map(|e| e.log_id),
            None => None,
        };
        Ok(())
    }
}

impl LogStore {
    pub fn open(dir: &Path, opts: WalOptions) -> Result<Self, StorageError> {
        std::fs::create_dir_all(dir).map_err(write_err)?;
        let mut wal = Wal::open(dir.join("wal"), opts).map_err(write_err)?;
        let vote = meta_file::read(&dir.join("vote"));
        let committed = meta_file::read::<Option<LogId>>(&dir.join("committed")).flatten();
        let purged: Option<LogId> = meta_file::read(&dir.join("purged"));
        if let Some(p) = purged {
            // Purging deletes whole segments only, so redo the logical part after a restart.
            wal.purge_to(p.index).map_err(write_err)?;
        }
        let mut inner =
            Inner { wal, dir: dir.to_path_buf(), vote, committed, purged, last: None, committed_saved_at: None };
        inner.refresh_last()?;
        Ok(Self { inner: Arc::new(Mutex::new(inner)) })
    }

    /// (first index, last index, bytes on disk) for status pages.
    pub fn stats(&self) -> (Option<u64>, Option<u64>, u64) {
        let i = self.inner.lock();
        (i.wal.first_index(), i.wal.last_index(), i.wal.disk_size())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        let inner = self.inner.lock();
        let (Some(first), Some(last)) = (inner.wal.first_index(), inner.wal.last_index()) else {
            return Ok(Vec::new());
        };
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => first,
        }
        .max(first);
        let end = match range.end_bound() {
            Bound::Included(&e) => e.saturating_add(1),
            Bound::Excluded(&e) => e,
            Bound::Unbounded => last + 1,
        }
        .min(last + 1);
        let mut out = Vec::with_capacity(end.saturating_sub(start) as usize);
        for i in start..end {
            match inner.entry_at(i)? {
                Some(e) => out.push(e),
                None => break,
            }
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let i = self.inner.lock();
        Ok(LogState { last_purged_log_id: i.purged, last_log_id: i.last.or(i.purged) })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError> {
        let inner = self.inner.clone();
        let vote = *vote;
        tokio::task::block_in_place(|| {
            let mut i = inner.lock();
            meta_file::write(&i.dir.join("vote"), &vote, true).map_err(vote_err)?;
            i.vote = Some(vote);
            Ok(())
        })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError> {
        Ok(self.inner.lock().vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        let mut i = self.inner.lock();
        i.committed = committed;
        // Only a restart hint: an older value is safe, so write it at most once a second.
        // (Replacing a file on ext4 forces a data flush, which is slow on every commit.)
        if i.committed_saved_at.is_none_or(|t| t.elapsed() >= Duration::from_secs(1)) {
            i.committed_saved_at = Some(Instant::now());
            if let Err(e) = meta_file::write(&i.dir.join("committed"), &committed, false) {
                tracing::warn!(error = %e, "failed to save committed index");
            }
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        Ok(self.inner.lock().committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut encoded = Vec::new();
        let mut last = None;
        for e in entries {
            encoded.push((e.log_id.index, postcard::to_stdvec(&e).map_err(write_err)?));
            last = Some(e.log_id);
        }
        let inner = self.inner.clone();
        let res = tokio::task::block_in_place(|| {
            let mut i = inner.lock();
            i.wal.append(encoded.iter().map(|(idx, b)| (*idx, b.as_slice())))?;
            i.wal.sync()?;
            if last.is_some() {
                i.last = last;
            }
            Ok::<_, pmc_store::wal::WalError>(())
        });
        match res {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                callback.log_io_completed(Err(std::io::Error::other(e.to_string())));
                Err(write_err(e))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let inner = self.inner.clone();
        tokio::task::block_in_place(|| {
            let mut i = inner.lock();
            i.wal.truncate_from(log_id.index).map_err(write_err)?;
            i.refresh_last()
        })
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let inner = self.inner.clone();
        tokio::task::block_in_place(|| {
            let mut i = inner.lock();
            meta_file::write(&i.dir.join("purged"), &log_id, true).map_err(write_err)?;
            i.purged = Some(log_id);
            i.wal.purge_to(log_id.index).map_err(write_err)?;
            i.refresh_last()
        })
    }
}
