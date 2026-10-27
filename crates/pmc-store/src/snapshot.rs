//! Streaming snapshot format.
//!
//! ```text
//! "PMCSNAP1" | u32 header_len | header (postcard) | item* | u32 crc32c(all previous bytes)
//! item = u16 key_len | key | u32 flags | u64 exptime | u64 cas | u64 stored_at | u8 bits
//!        | u32 value_len | value
//! ```
//! All integers are little endian. `header.meta` is opaque to this crate (pmcd keeps the Raft
//! snapshot metadata there).

use std::io::{self, Read, Write};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::item::Item;
use crate::store::{Clocks, Image};

pub const MAGIC: &[u8; 8] = b"PMCSNAP1";
const MAX_HEADER: u32 = 16 << 20;
const MAX_VALUE: u32 = 256 << 20;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotHeader {
    pub clocks: Clocks,
    pub items: u64,
    pub meta: Vec<u8>,
}

struct CrcWriter<W> {
    inner: W,
    crc: u32,
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc = crc32c::crc32c_append(self.crc, &buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct CrcReader<R> {
    inner: R,
    crc: u32,
}

impl<R: Read> Read for CrcReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.crc = crc32c::crc32c_append(self.crc, &buf[..n]);
        Ok(n)
    }
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("snapshot: {msg}"))
}

/// Writes `image` with `meta` into `w`. The caller is responsible for buffering and fsync.
pub fn write<W: Write>(w: W, image: &Image, meta: &[u8]) -> io::Result<()> {
    let mut w = CrcWriter { inner: w, crc: 0 };
    let header = SnapshotHeader { clocks: image.clocks, items: image.items.len() as u64, meta: meta.to_vec() };
    let hb = postcard::to_stdvec(&header).map_err(|e| invalid(&e.to_string()))?;
    w.write_all(MAGIC)?;
    w.write_all(&(hb.len() as u32).to_le_bytes())?;
    w.write_all(&hb)?;
    for (key, it) in &image.items {
        w.write_all(&(key.len() as u16).to_le_bytes())?;
        w.write_all(key)?;
        w.write_all(&it.flags.to_le_bytes())?;
        w.write_all(&it.exptime.to_le_bytes())?;
        w.write_all(&it.cas.to_le_bytes())?;
        w.write_all(&it.stored_at.to_le_bytes())?;
        w.write_all(&[it.bits])?;
        w.write_all(&(it.value.len() as u32).to_le_bytes())?;
        w.write_all(&it.value)?;
    }
    let crc = w.crc;
    w.inner.write_all(&crc.to_le_bytes())?;
    w.flush()
}

fn read_exact<const N: usize, R: Read>(r: &mut R) -> io::Result<[u8; N]> {
    let mut b = [0u8; N];
    r.read_exact(&mut b)?;
    Ok(b)
}

fn read_header_from<R: Read>(r: &mut R) -> io::Result<SnapshotHeader> {
    let magic: [u8; 8] = read_exact(r)?;
    if &magic != MAGIC {
        return Err(invalid("bad magic"));
    }
    let len = u32::from_le_bytes(read_exact(r)?);
    if len > MAX_HEADER {
        return Err(invalid("header too large"));
    }
    let mut hb = vec![0u8; len as usize];
    r.read_exact(&mut hb)?;
    postcard::from_bytes(&hb).map_err(|e| invalid(&e.to_string()))
}

/// Reads only the header (no checksum verification).
pub fn read_header<R: Read>(mut r: R) -> io::Result<SnapshotHeader> {
    read_header_from(&mut r)
}

/// Reads a whole snapshot, verifying the checksum. Items are passed to `sink` as they are read;
/// if this returns an error, whatever `sink` received must be discarded.
pub fn read<R: Read>(r: R, mut sink: impl FnMut(Bytes, Item)) -> io::Result<SnapshotHeader> {
    let mut r = CrcReader { inner: r, crc: 0 };
    let header = read_header_from(&mut r)?;
    for _ in 0..header.items {
        let klen = u16::from_le_bytes(read_exact(&mut r)?) as usize;
        let mut key = vec![0u8; klen];
        r.read_exact(&mut key)?;
        let flags = u32::from_le_bytes(read_exact(&mut r)?);
        let exptime = u64::from_le_bytes(read_exact(&mut r)?);
        let cas = u64::from_le_bytes(read_exact(&mut r)?);
        let stored_at = u64::from_le_bytes(read_exact(&mut r)?);
        let [bits] = read_exact::<1, _>(&mut r)?;
        let vlen = u32::from_le_bytes(read_exact(&mut r)?);
        if vlen > MAX_VALUE {
            return Err(invalid("value too large"));
        }
        let mut value = vec![0u8; vlen as usize];
        r.read_exact(&mut value)?;
        sink(Bytes::from(key), Item { value: Bytes::from(value), flags, exptime, cas, stored_at, bits });
    }
    let expected = r.crc;
    let crc = u32::from_le_bytes(read_exact(&mut r.inner)?);
    if crc != expected {
        return Err(invalid("checksum mismatch"));
    }
    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Clocks;

    fn image() -> Image {
        let item = |v: &'static str, cas| Item {
            value: Bytes::from_static(v.as_bytes()),
            flags: 3,
            exptime: 0,
            cas,
            stored_at: 10,
            bits: 0,
        };
        Image {
            clocks: Clocks { applied_index: 9, apply_time: 10, flush_index: 0, flush_floor: 0, flush_at: 0 },
            items: vec![(Bytes::from_static(b"a"), item("1", 1)), (Bytes::from_static(b"b"), item("22", 2))],
        }
    }

    #[test]
    fn roundtrip() {
        let mut buf = Vec::new();
        write(&mut buf, &image(), b"meta").unwrap();
        let mut got = Vec::new();
        let header = read(&buf[..], |k, it| got.push((k, it))).unwrap();
        assert_eq!(header.meta, b"meta");
        assert_eq!(header.items, 2);
        assert_eq!(header.clocks.applied_index, 9);
        assert_eq!(got.len(), 2);
        assert_eq!(&got[1].1.value[..], b"22");
        assert_eq!(read_header(&buf[..]).unwrap(), header);
    }

    #[test]
    fn detects_corruption() {
        let mut buf = Vec::new();
        write(&mut buf, &image(), b"").unwrap();
        let n = buf.len();
        buf[n - 6] ^= 0xff;
        assert!(read(&buf[..], |_, _| {}).is_err());
        assert!(read(&buf[..n - 2], |_, _| {}).is_err());
    }
}
