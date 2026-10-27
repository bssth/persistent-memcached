//! Reply encoding.

use base64::Engine;
use bytes::BytesMut;

use crate::MetaOpts;

pub fn line(out: &mut BytesMut, s: &[u8]) {
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
}

pub fn u64(out: &mut BytesMut, mut v: u64) {
    let mut b = [0u8; 20];
    let mut i = b.len();
    loop {
        i -= 1;
        b[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&b[i..]);
}

pub fn i64(out: &mut BytesMut, v: i64) {
    if v < 0 {
        out.extend_from_slice(b"-");
    }
    u64(out, v.unsigned_abs());
}

/// `VALUE <key> <flags> <bytes>[ <cas>]\r\n<data>\r\n`
pub fn value(out: &mut BytesMut, key: &[u8], flags: u32, data: &[u8], cas: Option<u64>) {
    out.reserve(key.len() + data.len() + 48);
    out.extend_from_slice(b"VALUE ");
    out.extend_from_slice(key);
    out.extend_from_slice(b" ");
    u64(out, flags as u64);
    out.extend_from_slice(b" ");
    u64(out, data.len() as u64);
    if let Some(c) = cas {
        out.extend_from_slice(b" ");
        u64(out, c);
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

pub fn end(out: &mut BytesMut) {
    out.extend_from_slice(b"END\r\n");
}

pub fn number_line(out: &mut BytesMut, v: u64) {
    u64(out, v);
    out.extend_from_slice(b"\r\n");
}

pub fn stat(out: &mut BytesMut, name: &str, value: impl std::fmt::Display) {
    use std::fmt::Write;
    let mut s = String::with_capacity(32);
    let _ = write!(s, "STAT {name} {value}\r\n");
    out.extend_from_slice(s.as_bytes());
}

/// What a meta reply can report about an item.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetaView<'a> {
    pub value: &'a [u8],
    /// Value length reported by `s`.
    pub size: u64,
    pub flags: u32,
    pub cas: u64,
    /// Remaining TTL in seconds, -1 for "never expires".
    pub ttl: i64,
    pub fetched: bool,
    pub last_access: u64,
    pub won: bool,
    pub stale: bool,
    pub already_won: bool,
}

/// Writes one return flag (preceded by a space), if it applies.
fn meta_flag(out: &mut BytesMut, f: u8, opts: &MetaOpts, key: &[u8], item: Option<&MetaView<'_>>) {
    match (f, item) {
        (b'O', _) => {
            if let Some(o) = &opts.opaque {
                out.extend_from_slice(b" O");
                out.extend_from_slice(o);
            }
        }
        (b'k', _) => {
            out.extend_from_slice(b" k");
            if opts.base64 {
                out.extend_from_slice(base64::engine::general_purpose::STANDARD.encode(key).as_bytes());
                out.extend_from_slice(b" b");
            } else {
                out.extend_from_slice(key);
            }
        }
        (b'c', Some(it)) => {
            out.extend_from_slice(b" c");
            u64(out, it.cas);
        }
        (b'f', Some(it)) => {
            out.extend_from_slice(b" f");
            u64(out, it.flags as u64);
        }
        (b'h', Some(it)) => out.extend_from_slice(if it.fetched { b" h1" } else { b" h0" }),
        (b'l', Some(it)) => {
            out.extend_from_slice(b" l");
            u64(out, it.last_access);
        }
        (b's', Some(it)) => {
            out.extend_from_slice(b" s");
            u64(out, it.size);
        }
        (b't', Some(it)) => {
            out.extend_from_slice(b" t");
            i64(out, it.ttl);
        }
        _ => {}
    }
}

/// Writes the return flags requested in `opts` (each preceded by a space), then `Z X W`.
pub fn meta_flags(out: &mut BytesMut, opts: &MetaOpts, key: &[u8], item: Option<&MetaView<'_>>) {
    for &f in &opts.ret {
        meta_flag(out, f, opts, key, item);
    }
    // memcached appends the lease flags in this order.
    if let Some(it) = item {
        if it.already_won {
            out.extend_from_slice(b" Z");
        }
        if it.stale {
            out.extend_from_slice(b" X");
        }
        if it.won {
            out.extend_from_slice(b" W");
        }
    }
}

/// `<code><flags>\r\n`, e.g. `HD c5 Oabc`.
pub fn meta_status(out: &mut BytesMut, code: &[u8], opts: &MetaOpts, key: &[u8], item: Option<&MetaView<'_>>) {
    out.extend_from_slice(code);
    meta_flags(out, opts, key, item);
    out.extend_from_slice(b"\r\n");
}

/// The status line of `ms`: `HD`, `NS`, `EX` or `NF`.
///
/// Like memcached, `ms` reports only `O`, `k`, `c` and `s`, in request order; the other return
/// flags (`f`, `t`, `h`, `l`) are accepted and ignored. `size` is the length of the stored value
/// (the combined one after append/prepend), or of the value sent when nothing was stored.
pub fn meta_set_status(out: &mut BytesMut, code: &[u8], opts: &MetaOpts, key: &[u8], cas: u64, size: u64) {
    let view = MetaView { cas, size, ..MetaView::default() };
    out.extend_from_slice(code);
    for &f in &opts.ret {
        if matches!(f, b'O' | b'k' | b'c' | b's') {
            meta_flag(out, f, opts, key, Some(&view));
        }
    }
    out.extend_from_slice(b"\r\n");
}

/// `VA <len><flags>\r\n<data>\r\n`
pub fn meta_value(out: &mut BytesMut, opts: &MetaOpts, key: &[u8], item: &MetaView<'_>) {
    out.reserve(item.value.len() + 64);
    out.extend_from_slice(b"VA ");
    u64(out, item.value.len() as u64);
    meta_flags(out, opts, key, Some(item));
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(item.value);
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers() {
        let mut b = BytesMut::new();
        u64(&mut b, 0);
        b.extend_from_slice(b",");
        u64(&mut b, u64::MAX);
        b.extend_from_slice(b",");
        i64(&mut b, -1);
        assert_eq!(&b[..], b"0,18446744073709551615,-1");
    }

    #[test]
    fn value_line() {
        let mut b = BytesMut::new();
        value(&mut b, b"k", 3, b"abc", Some(9));
        end(&mut b);
        assert_eq!(&b[..], b"VALUE k 3 3 9\r\nabc\r\nEND\r\n");
    }

    fn ms_opts(flags: &str) -> MetaOpts {
        let mut buf = BytesMut::from(format!("ms k 1 {flags}\r\nx\r\n").as_bytes());
        match crate::Parser::new(crate::Limits::default()).parse(&mut buf) {
            Some(crate::Frame::Request(crate::Request::MetaSet { opts, .. })) => opts,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn meta_set_reports_only_o_k_c_s() {
        let mut b = BytesMut::new();
        meta_set_status(&mut b, b"HD", &ms_opts("s t f h l c k Oab F9 T100"), b"k", 42, 5);
        assert_eq!(&b[..], b"HD s5 c42 kk Oab\r\n");

        let mut b = BytesMut::new();
        meta_set_status(&mut b, b"NS", &ms_opts("c s"), b"k", 0, 3);
        assert_eq!(&b[..], b"NS c0 s3\r\n");

        let mut b = BytesMut::new();
        meta_set_status(&mut b, b"HD", &ms_opts("t f"), b"k", 7, 1);
        assert_eq!(&b[..], b"HD\r\n");
    }
}
