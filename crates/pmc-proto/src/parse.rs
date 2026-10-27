//! Request parsing. Behaviour follows memcached master (1.6.40+ parser) and also accepts the
//! harmless 1.6.38 forms (leading spaces, `delete <key> 0`).

use bytes::{Buf, Bytes, BytesMut};
use memchr::memchr;
use pmc_store::numeric::{parse_i64_token, parse_u64_token};
use pmc_store::store::ITEM_OVERHEAD;

use crate::meta::{BAD_FORMAT, MAX_META_TOKENS, MetaCmd, meta_key, parse_flags};
use crate::{Frame, MAX_KEY_LEN, Request, StoreMode};

const INVALID_EXPTIME: &str = "CLIENT_ERROR invalid exptime argument";

#[derive(Clone, Debug)]
pub struct Limits {
    /// Maximum `key + value + overhead`, like memcached `-I` (default 1 MiB).
    pub max_item_size: usize,
    /// More than this many buffered bytes without a newline closes the connection.
    pub max_line: usize,
    /// Same for `get`/`gets`, which may carry many keys.
    pub max_get_line: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { max_item_size: 1 << 20, max_line: 2048, max_get_line: 8 << 20 }
    }
}

impl Limits {
    pub fn fits(&self, key_len: usize, value_len: usize) -> bool {
        key_len + value_len + ITEM_OVERHEAD <= self.max_item_size
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Storage(StoreMode, bool),
    MetaSet,
    Other,
}

fn classify(cmd: &[u8]) -> Kind {
    match cmd {
        b"set" => Kind::Storage(StoreMode::Set, false),
        b"add" => Kind::Storage(StoreMode::Add, false),
        b"replace" => Kind::Storage(StoreMode::Replace, false),
        b"append" => Kind::Storage(StoreMode::Append, false),
        b"prepend" => Kind::Storage(StoreMode::Prepend, false),
        b"cas" => Kind::Storage(StoreMode::Set, true),
        b"ms" => Kind::MetaSet,
        _ => Kind::Other,
    }
}

fn tokenize(line: &[u8], out: &mut Vec<(usize, usize)>) {
    out.clear();
    let mut i = 0;
    while i < line.len() {
        while i < line.len() && line[i] == b' ' {
            i += 1;
        }
        if i == line.len() {
            break;
        }
        let s = i;
        while i < line.len() && line[i] != b' ' {
            i += 1;
        }
        out.push((s, i));
    }
}

/// Strict int32 (digits with an optional leading `-`): touch/gat/flush exptime.
fn strict_i32(t: &[u8]) -> Option<i64> {
    let v = parse_i64_token(t)?;
    (i32::MIN as i64..=i32::MAX as i64).contains(&v).then_some(v)
}

/// `strtol`-style int32 used for storage exptime: also accepts a leading `+`.
/// Out-of-range values are rejected instead of being truncated like memcached does.
fn lenient_i32(t: &[u8]) -> Option<i64> {
    strict_i32(t.strip_prefix(b"+").unwrap_or(t))
}

/// `strtoul`-style u32 used for storage flags.
fn lenient_u32(t: &[u8]) -> Option<u32> {
    let t = t.strip_prefix(b"+").unwrap_or(t);
    u32::try_from(parse_u64_token(t)?).ok()
}

/// Strict data length: `0 <= bytes <= INT_MAX - 2`.
fn data_len(t: &[u8]) -> Option<usize> {
    let v = parse_u64_token(t)?;
    (v <= i32::MAX as u64 - 2).then_some(v as usize)
}

/// An error that `noreply` silences (memcached suppresses even errors with it).
fn err(noreply: bool, e: &'static str) -> Frame {
    if noreply { Frame::Skip } else { Frame::Error(e) }
}

/// Incremental parser; keep one per connection.
pub struct Parser {
    limits: Limits,
    swallow: usize,
    need: usize,
    toks: Vec<(usize, usize)>,
}

impl Parser {
    pub fn new(limits: Limits) -> Self {
        Self { limits, swallow: 0, need: 0, toks: Vec::with_capacity(16) }
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// How many bytes the buffer must hold for the pending request; lets the reader reserve
    /// capacity for a large value in one go.
    pub fn need(&self) -> usize {
        self.need
    }

    /// Parses one request from the front of `buf`, consuming its bytes.
    /// Returns `None` when more data is needed.
    pub fn parse(&mut self, buf: &mut BytesMut) -> Option<Frame> {
        if self.swallow > 0 {
            let n = self.swallow.min(buf.len());
            buf.advance(n);
            self.swallow -= n;
            if self.swallow > 0 {
                return None;
            }
        }
        let Some(nl) = memchr(b'\n', buf) else {
            self.need = 0;
            if buf.len() > self.limits.max_line {
                let start = buf.iter().position(|&b| b != b' ').unwrap_or(buf.len());
                let rest = &buf[start..];
                let get_like = rest.starts_with(b"get ") || rest.starts_with(b"gets ");
                if !get_like || buf.len() > self.limits.max_get_line {
                    return Some(Frame::Close);
                }
            }
            return None;
        };
        let line_len = nl + 1;
        if line_len < 4 {
            buf.advance(line_len);
            return Some(Frame::Error(BAD_FORMAT));
        }
        let line_end = if buf[nl - 1] == b'\r' { nl - 1 } else { nl };
        let mut toks = std::mem::take(&mut self.toks);
        tokenize(&buf[..line_end], &mut toks);
        let frame = self.dispatch(buf, &toks, line_len);
        self.toks = toks;
        if frame.is_some() {
            self.need = 0;
        }
        frame
    }

    fn dispatch(&mut self, buf: &mut BytesMut, toks: &[(usize, usize)], line_len: usize) -> Option<Frame> {
        if toks.is_empty() {
            buf.advance(line_len);
            return Some(Frame::Error("ERROR"));
        }
        match classify(&buf[toks[0].0..toks[0].1]) {
            Kind::Storage(mode, is_cas) => self.storage(buf, toks, line_len, mode, is_cas),
            Kind::MetaSet => self.meta_set(buf, toks, line_len),
            Kind::Other => {
                let line = buf.split_to(line_len).freeze();
                let t: Vec<&[u8]> = toks.iter().map(|&(s, e)| &line[s..e]).collect();
                Some(simple(&line, &t))
            }
        }
    }

    /// Starts swallowing `n` bytes of a rejected value, eating what is already buffered.
    fn start_swallow(&mut self, buf: &mut BytesMut, n: usize) {
        let now = n.min(buf.len());
        buf.advance(now);
        self.swallow = n - now;
    }

    fn storage(
        &mut self,
        buf: &mut BytesMut,
        toks: &[(usize, usize)],
        line_len: usize,
        mode: StoreMode,
        is_cas: bool,
    ) -> Option<Frame> {
        let n = toks.len();
        let tk = |i: usize| &buf[toks[i].0..toks[i].1];
        let noreply = tk(n - 1) == b"noreply";
        let Some(vlen) = (n >= 5).then(|| data_len(tk(4))).flatten() else {
            // Without a length the data block cannot be skipped; it is parsed as commands.
            buf.advance(line_len);
            return Some(err(noreply, BAD_FORMAT));
        };
        if is_cas && n < 6 {
            buf.advance(line_len);
            self.start_swallow(buf, vlen + 2);
            return Some(err(noreply, BAD_FORMAT));
        }
        let key = toks[1];
        let key_len = key.1 - key.0;
        let flags = lenient_u32(tk(2));
        let exptime = lenient_i32(tk(3));
        let cas = if is_cas { parse_u64_token(tk(5)).map(Some) } else { Some(None) };
        let (Some(flags), Some(exptime), Some(cas)) = (flags, exptime, cas) else {
            buf.advance(line_len);
            self.start_swallow(buf, vlen + 2);
            return Some(err(noreply, BAD_FORMAT));
        };
        if key_len > MAX_KEY_LEN {
            buf.advance(line_len);
            self.start_swallow(buf, vlen + 2);
            return Some(err(noreply, BAD_FORMAT));
        }
        if !self.limits.fits(key_len, vlen) {
            let line = buf.split_to(line_len).freeze();
            self.start_swallow(buf, vlen + 2);
            return Some(Frame::Request(Request::TooLarge { mode, key: line.slice(key.0..key.1), noreply }));
        }
        let total = line_len + vlen + 2;
        if buf.len() < total {
            self.need = total;
            return None;
        }
        let frame = buf.split_to(total).freeze();
        if &frame[line_len + vlen..total] != b"\r\n" {
            return Some(err(noreply, "CLIENT_ERROR bad data chunk"));
        }
        Some(Frame::Request(Request::Store {
            mode,
            key: frame.slice(key.0..key.1),
            flags,
            exptime,
            value: frame.slice(line_len..line_len + vlen),
            cas,
            noreply,
        }))
    }

    fn meta_set(&mut self, buf: &mut BytesMut, toks: &[(usize, usize)], line_len: usize) -> Option<Frame> {
        let Some(vlen) = (toks.len() >= 3).then(|| data_len(&buf[toks[2].0..toks[2].1])).flatten() else {
            buf.advance(line_len);
            return Some(Frame::Error(BAD_FORMAT));
        };
        if toks.len() > MAX_META_TOKENS {
            buf.advance(line_len);
            self.start_swallow(buf, vlen + 2);
            return Some(Frame::Error("CLIENT_ERROR options flags are too long"));
        }
        let raw_key_len = toks[1].1 - toks[1].0;
        if !self.limits.fits(raw_key_len.min(MAX_KEY_LEN), vlen) {
            let line = buf.split_to(line_len).freeze();
            self.start_swallow(buf, vlen + 2);
            let t: Vec<&[u8]> = toks.iter().map(|&(s, e)| &line[s..e]).collect();
            let opts = match parse_flags(MetaCmd::Set, &t[3..], &line) {
                Ok(o) => o,
                Err(e) => return Some(Frame::Error(e)),
            };
            return Some(match meta_key(t[1], &line, opts.base64) {
                Ok(key) => Frame::Request(Request::TooLarge { mode: StoreMode::Set, key, noreply: false }),
                Err(e) => Frame::Error(e),
            });
        }
        let total = line_len + vlen + 2;
        if buf.len() < total {
            self.need = total;
            return None;
        }
        let frame = buf.split_to(total).freeze();
        let t: Vec<&[u8]> = toks.iter().map(|&(s, e)| &frame[s..e]).collect();
        let opts = match parse_flags(MetaCmd::Set, &t[3..], &frame) {
            Ok(o) => o,
            Err(e) => return Some(Frame::Error(e)),
        };
        let key = match meta_key(t[1], &frame, opts.base64) {
            Ok(k) => k,
            Err(e) => return Some(Frame::Error(e)),
        };
        if &frame[line_len + vlen..total] != b"\r\n" {
            return Some(Frame::Error("CLIENT_ERROR bad data chunk"));
        }
        Some(Frame::Request(Request::MetaSet { key, value: frame.slice(line_len..line_len + vlen), opts }))
    }
}

fn slice_of(line: &Bytes, s: &[u8]) -> Bytes {
    let start = s.as_ptr() as usize - line.as_ptr() as usize;
    line.slice(start..start + s.len())
}

fn keys_of(line: &Bytes, t: &[&[u8]]) -> Result<Vec<Bytes>, &'static str> {
    let mut keys = Vec::with_capacity(t.len());
    for k in t {
        if k.len() > MAX_KEY_LEN {
            return Err(BAD_FORMAT);
        }
        keys.push(slice_of(line, k));
    }
    Ok(keys)
}

fn key_of(line: &Bytes, k: &[u8]) -> Result<Bytes, &'static str> {
    if k.len() > MAX_KEY_LEN { Err(BAD_FORMAT) } else { Ok(slice_of(line, k)) }
}

/// Commands that honour a trailing `noreply` (it silences their errors too).
fn takes_noreply(cmd: &[u8]) -> bool {
    matches!(cmd, b"delete" | b"incr" | b"decr" | b"touch" | b"flush_all" | b"verbosity" | b"cache_memlimit")
}

fn simple(line: &Bytes, t: &[&[u8]]) -> Frame {
    match simple_inner(line, t) {
        Ok(r) => Frame::Request(r),
        Err("") => Frame::Close,
        Err(e) => {
            let noreply = t.len() > 1 && t[t.len() - 1] == b"noreply" && takes_noreply(t[0]);
            err(noreply, e)
        }
    }
}

fn simple_inner(line: &Bytes, t: &[&[u8]]) -> Result<Request, &'static str> {
    let n = t.len();
    let noreply = n > 1 && t[n - 1] == b"noreply";
    Ok(match t[0] {
        b"get" | b"gets" => {
            if n < 2 {
                return Err(BAD_FORMAT);
            }
            Request::Get { keys: keys_of(line, &t[1..])?, with_cas: t[0] == b"gets" }
        }
        b"gat" | b"gats" => {
            if n < 3 {
                return Err(BAD_FORMAT);
            }
            let exptime = strict_i32(t[1]).ok_or(INVALID_EXPTIME)?;
            Request::Gat { exptime, keys: keys_of(line, &t[2..])?, with_cas: t[0] == b"gats" }
        }
        b"delete" => {
            if n < 2 {
                return Err(BAD_FORMAT);
            }
            // The legacy hold time (`delete <key> 0`) and any extra tokens are ignored.
            Request::Delete { key: key_of(line, t[1])?, noreply }
        }
        b"incr" | b"decr" => {
            if n < 3 {
                return Err(BAD_FORMAT);
            }
            let key = key_of(line, t[1])?;
            let delta = parse_u64_token(t[2]).ok_or("CLIENT_ERROR invalid numeric delta argument")?;
            Request::Arith { key, incr: t[0] == b"incr", delta, noreply }
        }
        b"touch" => {
            if n < 3 {
                return Err(BAD_FORMAT);
            }
            let key = key_of(line, t[1])?;
            let exptime = strict_i32(t[2]).ok_or(INVALID_EXPTIME)?;
            Request::Touch { key, exptime, noreply }
        }
        b"flush_all" => {
            let delay = match t.get(1) {
                Some(&d) if d != b"noreply" => strict_i32(d).ok_or(INVALID_EXPTIME)?,
                _ => 0,
            };
            Request::FlushAll { delay, noreply }
        }
        b"version" => Request::Version,
        b"verbosity" => {
            if n < 2 {
                return Err(BAD_FORMAT);
            }
            parse_u64_token(t[1]).filter(|v| *v <= u32::MAX as u64).ok_or(BAD_FORMAT)?;
            Request::Verbosity { noreply }
        }
        b"stats" => Request::Stats { args: t[1..].iter().map(|a| slice_of(line, a)).collect() },
        b"quit" => Request::Quit,
        b"shutdown" => {
            return Err(match t.get(1) {
                None => "SERVER_ERROR: shutdown not enabled",
                Some(&b"graceful") => "SERVER_ERROR: shutdown not enabled",
                Some(_) => "CLIENT_ERROR invalid shutdown mode",
            });
        }
        b"cache_memlimit" => {
            let Some(mb) = t.get(1).and_then(|v| parse_u64_token(v)) else { return Err("ERROR") };
            if mb < 8 {
                return Err("MEMLIMIT_TOO_SMALL cannot set maxbytes to less than 8m");
            }
            if mb > 1_000_000_000 {
                return Err("MEMLIMIT_ADJUST_FAILED input value is megabytes not bytes");
            }
            Request::CacheMemlimit { megabytes: mb, noreply }
        }
        b"mn" => Request::MetaNoop,
        b"mg" | b"md" | b"ma" | b"me" => {
            if n < 2 {
                return Err(BAD_FORMAT);
            }
            if n > MAX_META_TOKENS {
                return Err("CLIENT_ERROR options flags are too long");
            }
            let cmd = match t[0] {
                b"mg" => MetaCmd::Get,
                b"md" => MetaCmd::Delete,
                b"ma" => MetaCmd::Arith,
                _ => MetaCmd::Debug,
            };
            let opts = parse_flags(cmd, &t[2..], line)?;
            let key = meta_key(t[1], line, opts.base64)?;
            match cmd {
                MetaCmd::Get => Request::MetaGet { key, opts },
                MetaCmd::Delete => Request::MetaDelete { key, opts },
                MetaCmd::Arith => Request::MetaArith { key, opts },
                _ => Request::MetaDebug { key, opts },
            }
        }
        _ => {
            // An HTTP request line ("GET / HTTP/1.1") closes the connection silently.
            if t[n - 1].starts_with(b"HTTP/") {
                return Err("");
            }
            return Err("ERROR");
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> Vec<Frame> {
        let mut p = Parser::new(Limits::default());
        let mut buf = BytesMut::from(input);
        let mut out = Vec::new();
        while let Some(f) = p.parse(&mut buf) {
            out.push(f);
        }
        out
    }

    fn req(f: &Frame) -> &Request {
        match f {
            Frame::Request(r) => r,
            other => panic!("expected request, got {other:?}"),
        }
    }

    #[test]
    fn pipelined_classic() {
        let frames = parse_all(b"set a 5 0 3\r\nabc\r\nget a b\r\ngets c\nincr n 10 noreply\r\ndelete a 0\r\n");
        assert_eq!(frames.len(), 5);
        match req(&frames[0]) {
            Request::Store { mode, key, flags, value, noreply, .. } => {
                assert_eq!(*mode, StoreMode::Set);
                assert_eq!(&key[..], b"a");
                assert_eq!(*flags, 5);
                assert_eq!(&value[..], b"abc");
                assert!(!noreply);
            }
            r => panic!("{r:?}"),
        }
        assert!(matches!(req(&frames[1]), Request::Get { keys, with_cas: false } if keys.len() == 2));
        assert!(matches!(req(&frames[2]), Request::Get { with_cas: true, .. }));
        assert!(matches!(req(&frames[3]), Request::Arith { incr: true, delta: 10, noreply: true, .. }));
        assert!(matches!(req(&frames[4]), Request::Delete { noreply: false, .. }));
    }

    #[test]
    fn leading_spaces_are_accepted() {
        let frames = parse_all(b"  get k\r\n");
        assert!(matches!(req(&frames[0]), Request::Get { .. }));
    }

    #[test]
    fn incremental_value() {
        let mut p = Parser::new(Limits::default());
        let mut buf = BytesMut::from(&b"set k 0 0 10\r\n01234"[..]);
        assert!(p.parse(&mut buf).is_none());
        assert_eq!(p.need(), 14 + 12);
        buf.extend_from_slice(b"56789\r\n");
        assert!(matches!(p.parse(&mut buf), Some(Frame::Request(Request::Store { .. }))));
        assert!(buf.is_empty());
    }

    #[test]
    fn errors_follow_memcached_master() {
        let frames = parse_all(
            b"bogus\r\nset k 0 0\r\nset k 0 0 3\r\nabcd\r\n\r\nincr k x\r\nget\r\nmn\nflush_all x\r\nshutdown\r\n",
        );
        assert_eq!(frames[0], Frame::Error("ERROR"));
        assert_eq!(frames[1], Frame::Error(BAD_FORMAT));
        assert_eq!(frames[2], Frame::Error("CLIENT_ERROR bad data chunk"));
        // exactly <bytes>+2 ("abcd\r") was consumed; "\n" and "\r\n" are too-short lines
        assert_eq!(frames[3], Frame::Error(BAD_FORMAT));
        assert_eq!(frames[4], Frame::Error(BAD_FORMAT));
        assert_eq!(frames[5], Frame::Error("CLIENT_ERROR invalid numeric delta argument"));
        assert_eq!(frames[6], Frame::Error(BAD_FORMAT));
        assert_eq!(frames[7], Frame::Error(BAD_FORMAT));
        assert_eq!(frames[8], Frame::Error(INVALID_EXPTIME));
        assert_eq!(frames[9], Frame::Error("SERVER_ERROR: shutdown not enabled"));
    }

    #[test]
    fn bad_header_swallows_data() {
        let frames = parse_all(b"set k x 0 3\r\nabc\r\nversion\r\nset k 0 0 -1\r\nversion\r\n");
        assert_eq!(frames[0], Frame::Error(BAD_FORMAT));
        assert_eq!(req(&frames[1]), &Request::Version);
        // a bad length cannot be skipped
        assert_eq!(frames[2], Frame::Error(BAD_FORMAT));
        assert_eq!(req(&frames[3]), &Request::Version);
    }

    #[test]
    fn noreply_silences_errors() {
        let frames = parse_all(b"incr k x noreply\r\nset k 0 0 1 noreply\r\nab\r\n");
        assert_eq!(frames[0], Frame::Skip);
        assert_eq!(frames[1], Frame::Skip);
    }

    #[test]
    fn too_large_is_swallowed() {
        let mut p = Parser::new(Limits { max_item_size: 100, ..Limits::default() });
        let mut buf = BytesMut::from(&b"set k 0 0 200\r\n"[..]);
        buf.extend_from_slice(&[b'x'; 50]);
        assert!(matches!(p.parse(&mut buf), Some(Frame::Request(Request::TooLarge { .. }))));
        assert!(p.parse(&mut buf).is_none());
        buf.extend_from_slice(&[b'x'; 150]);
        buf.extend_from_slice(b"\r\nversion\r\n");
        assert_eq!(p.parse(&mut buf), Some(Frame::Request(Request::Version)));
    }

    #[test]
    fn long_lines_and_http_close() {
        let mut p = Parser::new(Limits::default());
        let mut buf = BytesMut::from(&[b'a'; 3000][..]);
        assert_eq!(p.parse(&mut buf), Some(Frame::Close));
        let mut buf = BytesMut::from(&b"get "[..]);
        buf.extend_from_slice(&[b'k'; 3000]);
        assert_eq!(p.parse(&mut buf), None);
        assert_eq!(parse_all(b"GET / HTTP/1.1\r\n"), vec![Frame::Close]);
    }

    #[test]
    fn meta_commands() {
        let frames = parse_all(
            b"mg foo v c t Oabc q\r\nms bar 2 T60 F5 MA\r\nhi\r\nmd baz q I T30\r\nma cnt N0 J10 D5 v\r\nmn\r\nmg Zm9v b k v\r\n",
        );
        match req(&frames[0]) {
            Request::MetaGet { key, opts } => {
                assert_eq!(&key[..], b"foo");
                assert!(opts.value && opts.quiet);
                assert_eq!(opts.ret, b"ctO");
                assert!(!opts.base64);
                assert_eq!(opts.opaque.as_deref(), Some(&b"abc"[..]));
            }
            r => panic!("{r:?}"),
        }
        match req(&frames[1]) {
            Request::MetaSet { key, value, opts } => {
                assert_eq!(&key[..], b"bar");
                assert_eq!(&value[..], b"hi");
                assert_eq!(opts.ttl, Some(60));
                assert_eq!(opts.client_flags, Some(5));
                assert_eq!(opts.mode, Some(b'A'));
            }
            r => panic!("{r:?}"),
        }
        assert!(matches!(req(&frames[2]), Request::MetaDelete { opts, .. } if opts.invalidate && opts.ttl == Some(30)));
        assert!(
            matches!(req(&frames[3]), Request::MetaArith { opts, .. } if opts.delta == Some(5) && opts.initial == Some(10))
        );
        assert_eq!(req(&frames[4]), &Request::MetaNoop);
        assert!(matches!(req(&frames[5]), Request::MetaGet { key, .. } if &key[..] == b"foo"));
    }

    #[test]
    fn meta_errors() {
        let frames = parse_all(b"mg\r\nmg k zz\r\nmg k v v\r\nms k abc\r\nma k Q\r\nms k 1 Mx\r\nz\r\nmg k O12345678901234567890123456789012\r\n");
        assert_eq!(frames[0], Frame::Error(BAD_FORMAT));
        assert_eq!(frames[1], Frame::Error("CLIENT_ERROR invalid flag"));
        assert_eq!(frames[2], Frame::Error("CLIENT_ERROR duplicate flag"));
        assert_eq!(frames[3], Frame::Error(BAD_FORMAT));
        assert_eq!(frames[4], Frame::Error("CLIENT_ERROR invalid or duplicate flag"));
        assert_eq!(frames[5], Frame::Error("CLIENT_ERROR invalid mode for ms M token"));
        assert_eq!(frames[6], Frame::Error("CLIENT_ERROR opaque token too long"));
    }
}
