//! Segmented write-ahead log — the "binlog".
//!
//! Files are named `<first index, 20 digits>.wal`. Each record:
//!
//! ```text
//! u32 payload_len | u32 crc32c(index_le ++ payload) | u64 index | payload
//! ```
//!
//! Records have consecutive indexes. A torn record at the end of the last segment (a crash in
//! the middle of a write) is cut off on open; damage anywhere else is reported as an error.
//! Purging only deletes whole segments, so after `open` the caller must purge again up to its
//! own persisted purge point.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

const HEADER: usize = 16;
pub const MAX_RECORD: usize = 256 << 20;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("wal io: {0}")]
    Io(#[from] io::Error),
    #[error("wal corrupted in {path}: {msg}")]
    Corrupt { path: PathBuf, msg: String },
    #[error("wal append out of order: expected index {expected}, got {got}")]
    OutOfOrder { expected: u64, got: u64 },
}

#[derive(Clone, Debug)]
pub struct WalOptions {
    /// Start a new segment once the active one grows beyond this size.
    pub segment_size: u64,
    /// fsync on `sync()`. Turning it off trades durability on power loss for speed.
    pub fsync: bool,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self { segment_size: 64 << 20, fsync: true }
    }
}

struct Segment {
    first: u64,
    path: PathBuf,
    file: File,
    len: u64,
}

pub struct Wal {
    dir: PathBuf,
    opts: WalOptions,
    segments: VecDeque<Segment>,
    /// Offset of every live record; `offsets[i]` belongs to index `first + i`.
    offsets: VecDeque<u64>,
    first: u64,
    dirty: bool,
}

fn segment_name(first: u64) -> String {
    format!("{first:020}.wal")
}

#[cfg(unix)]
fn read_at(f: &File, buf: &mut [u8], off: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.read_exact_at(buf, off)
}

#[cfg(windows)]
fn read_at(f: &File, mut buf: &mut [u8], mut off: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match f.seek_read(buf, off)? {
            0 => return Err(io::ErrorKind::UnexpectedEof.into()),
            n => {
                buf = &mut buf[n..];
                off += n as u64;
            }
        }
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

fn record_crc(index: u64, payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&index.to_le_bytes()), payload)
}

/// Scans one segment and returns record offsets plus the length of the valid prefix.
fn scan(path: &Path, first: u64) -> io::Result<(Vec<u64>, u64, Option<String>)> {
    let mut f = io::BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut offsets = Vec::new();
    let mut off = 0u64;
    let mut expected = first;
    let mut payload = Vec::new();
    loop {
        let mut h = [0u8; HEADER];
        match f.read_exact(&mut h) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Either a clean end or a torn header; the valid prefix ends at `off` either way.
                let total = fs::metadata(path)?.len();
                let tail = (total > off).then(|| "torn record header".to_string());
                return Ok((offsets, off, tail));
            }
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(h[4..8].try_into().unwrap());
        let index = u64::from_le_bytes(h[8..16].try_into().unwrap());
        if len > MAX_RECORD {
            return Ok((offsets, off, Some(format!("record length {len} at offset {off}"))));
        }
        payload.resize(len, 0);
        if let Err(e) = f.read_exact(&mut payload) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok((offsets, off, Some("torn record payload".into())));
            }
            return Err(e);
        }
        if record_crc(index, &payload) != crc {
            return Ok((offsets, off, Some(format!("checksum mismatch at offset {off}"))));
        }
        if index != expected {
            return Ok((offsets, off, Some(format!("index {index} at offset {off}, expected {expected}"))));
        }
        offsets.push(off);
        off += (HEADER + len) as u64;
        expected += 1;
    }
}

impl Wal {
    pub fn open(dir: impl AsRef<Path>, opts: WalOptions) -> Result<Self, WalError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let mut names: Vec<(u64, PathBuf)> = Vec::new();
        for e in fs::read_dir(&dir)? {
            let e = e?;
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(stem) = name.strip_suffix(".wal") else { continue };
            let Ok(first) = stem.parse::<u64>() else { continue };
            names.push((first, e.path()));
        }
        names.sort();

        let mut wal = Wal { dir, opts, segments: VecDeque::new(), offsets: VecDeque::new(), first: 0, dirty: false };
        let count = names.len();
        for (i, (first, path)) in names.into_iter().enumerate() {
            let last = i + 1 == count;
            let (offsets, valid_len, problem) = scan(&path, first)?;
            if let Some(msg) = problem {
                if !last {
                    return Err(WalError::Corrupt { path, msg });
                }
                tracing::warn!(path = %path.display(), %msg, "cutting off damaged wal tail");
                OpenOptions::new().write(true).open(&path)?.set_len(valid_len)?;
            }
            if let Some(prev_last) = wal.last_index() {
                if !offsets.is_empty() && first != prev_last + 1 {
                    return Err(WalError::Corrupt {
                        path,
                        msg: format!("segment starts at {first}, previous ends at {prev_last}"),
                    });
                }
            }
            if wal.offsets.is_empty() {
                wal.first = first;
            }
            wal.offsets.extend(offsets);
            let file = OpenOptions::new().read(true).append(true).open(&path)?;
            wal.segments.push_back(Segment { first, path, file, len: valid_len });
        }
        Ok(wal)
    }

    pub fn first_index(&self) -> Option<u64> {
        (!self.offsets.is_empty()).then_some(self.first)
    }

    pub fn last_index(&self) -> Option<u64> {
        (!self.offsets.is_empty()).then(|| self.first + self.offsets.len() as u64 - 1)
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    fn sync_dir(&self) -> io::Result<()> {
        if self.opts.fsync { sync_dir(&self.dir) } else { Ok(()) }
    }

    fn create_segment(&mut self, first: u64) -> io::Result<()> {
        let path = self.dir.join(segment_name(first));
        let file = OpenOptions::new().read(true).append(true).create(true).truncate(false).open(&path)?;
        file.set_len(0)?;
        self.segments.push_back(Segment { first, path, file, len: 0 });
        self.sync_dir()
    }

    /// Appends records with consecutive indexes. When the log is empty the first index is free.
    /// Data is written to the OS but not fsynced; call [`Wal::sync`].
    pub fn append<'a>(&mut self, records: impl IntoIterator<Item = (u64, &'a [u8])>) -> Result<(), WalError> {
        let mut buf = Vec::new();
        for (index, payload) in records {
            if let Some(last) = self.last_index() {
                if index != last + 1 {
                    return Err(WalError::OutOfOrder { expected: last + 1, got: index });
                }
            } else {
                // Empty log: drop leftover empty segments and start where the caller says.
                self.flush_buf(&mut buf)?;
                while let Some(seg) = self.segments.pop_back() {
                    drop(seg.file);
                    fs::remove_file(&seg.path)?;
                }
                self.first = index;
            }
            let need_new = match self.segments.back() {
                None => true,
                Some(seg) => seg.len + buf.len() as u64 >= self.opts.segment_size,
            };
            if need_new {
                self.flush_buf(&mut buf)?;
                if let Some(seg) = self.segments.back() {
                    if self.opts.fsync {
                        seg.file.sync_data()?;
                    }
                }
                self.create_segment(index)?;
            }
            let seg_len = self.segments.back().map(|s| s.len).unwrap_or(0);
            self.offsets.push_back(seg_len + buf.len() as u64);
            buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&record_crc(index, payload).to_le_bytes());
            buf.extend_from_slice(&index.to_le_bytes());
            buf.extend_from_slice(payload);
        }
        self.flush_buf(&mut buf)?;
        Ok(())
    }

    fn flush_buf(&mut self, buf: &mut Vec<u8>) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let seg = self.segments.back_mut().expect("active segment exists when buffer is not empty");
        seg.file.write_all(buf)?;
        seg.len += buf.len() as u64;
        buf.clear();
        self.dirty = true;
        Ok(())
    }

    /// Makes appended records durable.
    pub fn sync(&mut self) -> io::Result<()> {
        if self.dirty && self.opts.fsync {
            if let Some(seg) = self.segments.back() {
                seg.file.sync_data()?;
            }
        }
        self.dirty = false;
        Ok(())
    }

    fn segment_of(&self, index: u64) -> &Segment {
        let k = self.segments.partition_point(|s| s.first <= index);
        &self.segments[k - 1]
    }

    pub fn read(&self, index: u64) -> Result<Option<Vec<u8>>, WalError> {
        let Some(last) = self.last_index() else { return Ok(None) };
        if index < self.first || index > last {
            return Ok(None);
        }
        let off = self.offsets[(index - self.first) as usize];
        let seg = self.segment_of(index);
        let mut h = [0u8; HEADER];
        read_at(&seg.file, &mut h, off)?;
        let len = u32::from_le_bytes(h[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(h[4..8].try_into().unwrap());
        let got = u64::from_le_bytes(h[8..16].try_into().unwrap());
        let mut payload = vec![0u8; len];
        read_at(&seg.file, &mut payload, off + HEADER as u64)?;
        if got != index || record_crc(index, &payload) != crc {
            return Err(WalError::Corrupt { path: seg.path.clone(), msg: format!("bad record {index}") });
        }
        Ok(Some(payload))
    }

    /// Removes records with index >= `index` (Raft log conflict resolution).
    pub fn truncate_from(&mut self, index: u64) -> Result<(), WalError> {
        let Some(last) = self.last_index() else { return Ok(()) };
        if index > last {
            return Ok(());
        }
        let index = index.max(self.first);
        let off = self.offsets[(index - self.first) as usize];
        let containing = self.segment_of(index).first;
        // Segments that start at or after `index` go entirely; the one holding `index` is cut.
        while self.segments.back().is_some_and(|s| s.first >= index) {
            let seg = self.segments.pop_back().unwrap();
            drop(seg.file);
            fs::remove_file(&seg.path)?;
        }
        if let Some(seg) = self.segments.back_mut().filter(|s| s.first == containing) {
            if seg.len > off {
                seg.file.set_len(off)?;
                seg.len = off;
                if self.opts.fsync {
                    seg.file.sync_data()?;
                }
            }
        }
        self.offsets.truncate((index - self.first) as usize);
        self.sync_dir()?;
        Ok(())
    }

    /// Logically removes records with index <= `index`, deleting segments that became unused.
    pub fn purge_to(&mut self, index: u64) -> Result<(), WalError> {
        match self.last_index() {
            None => {}
            Some(last) if index >= last => {
                self.offsets.clear();
                self.first = index + 1;
            }
            Some(_) if index >= self.first => {
                let n = (index + 1 - self.first) as usize;
                self.offsets.drain(..n);
                self.first = index + 1;
            }
            Some(_) => {}
        }
        // A segment can go once the next one starts at or before the new first index.
        let keep_from = self.first;
        let mut removed = false;
        while self.segments.len() > 1 && self.segments[1].first <= keep_from {
            let seg = self.segments.pop_front().unwrap();
            drop(seg.file);
            fs::remove_file(&seg.path)?;
            removed = true;
        }
        if self.offsets.is_empty() {
            while let Some(seg) = self.segments.pop_front() {
                drop(seg.file);
                fs::remove_file(&seg.path)?;
                removed = true;
            }
        }
        if removed {
            self.sync_dir()?;
        }
        Ok(())
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Bytes on disk in all segments.
    pub fn disk_size(&self) -> u64 {
        self.segments.iter().map(|s| s.len).sum()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(segment_size: u64) -> WalOptions {
        WalOptions { segment_size, fsync: false }
    }

    fn rec(i: u64) -> Vec<u8> {
        format!("payload-{i}").into_bytes()
    }

    fn append_range(w: &mut Wal, from: u64, to: u64) {
        let recs: Vec<(u64, Vec<u8>)> = (from..=to).map(|i| (i, rec(i))).collect();
        w.append(recs.iter().map(|(i, p)| (*i, p.as_slice()))).unwrap();
    }

    #[test]
    fn append_read_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::open(dir.path(), opts(100)).unwrap();
        append_range(&mut w, 1, 50);
        assert!(w.segment_count() > 1);
        assert_eq!(w.read(17).unwrap().unwrap(), rec(17));
        drop(w);
        let w = Wal::open(dir.path(), opts(100)).unwrap();
        assert_eq!(w.first_index(), Some(1));
        assert_eq!(w.last_index(), Some(50));
        for i in 1..=50 {
            assert_eq!(w.read(i).unwrap().unwrap(), rec(i));
        }
        assert!(w.read(51).unwrap().is_none());
    }

    #[test]
    fn torn_tail_is_cut() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::open(dir.path(), opts(1 << 20)).unwrap();
        append_range(&mut w, 1, 10);
        let path = w.segments.back().unwrap().path.clone();
        drop(w);
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new().write(true).open(&path).unwrap().set_len(len - 3).unwrap();
        let mut w = Wal::open(dir.path(), opts(1 << 20)).unwrap();
        assert_eq!(w.last_index(), Some(9));
        append_range(&mut w, 10, 12);
        drop(w);
        let w = Wal::open(dir.path(), opts(1 << 20)).unwrap();
        assert_eq!(w.last_index(), Some(12));
        assert_eq!(w.read(10).unwrap().unwrap(), rec(10));
    }

    #[test]
    fn truncate_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::open(dir.path(), opts(100)).unwrap();
        append_range(&mut w, 1, 40);
        w.truncate_from(25).unwrap();
        assert_eq!(w.last_index(), Some(24));
        append_range(&mut w, 25, 30);
        w.purge_to(10).unwrap();
        assert_eq!(w.first_index(), Some(11));
        assert!(w.read(10).unwrap().is_none());
        assert_eq!(w.read(11).unwrap().unwrap(), rec(11));
        drop(w);
        let mut w = Wal::open(dir.path(), opts(100)).unwrap();
        w.purge_to(10).unwrap();
        assert_eq!(w.first_index(), Some(11));
        assert_eq!(w.last_index(), Some(30));
        // Purge past the end (snapshot installed beyond the log): the log restarts anywhere.
        w.purge_to(100).unwrap();
        assert!(w.is_empty());
        assert_eq!(w.segment_count(), 0);
        append_range(&mut w, 101, 105);
        drop(w);
        let w = Wal::open(dir.path(), opts(100)).unwrap();
        assert_eq!(w.first_index(), Some(101));
        assert_eq!(w.last_index(), Some(105));
    }

    #[test]
    fn truncate_whole_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::open(dir.path(), opts(100)).unwrap();
        append_range(&mut w, 5, 20);
        w.truncate_from(5).unwrap();
        assert!(w.is_empty());
        append_range(&mut w, 5, 6);
        assert_eq!(w.read(6).unwrap().unwrap(), rec(6));
    }

    #[test]
    fn out_of_order_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = Wal::open(dir.path(), opts(100)).unwrap();
        append_range(&mut w, 1, 3);
        assert!(matches!(w.append([(5u64, &b"x"[..])]), Err(WalError::OutOfOrder { .. })));
    }
}
