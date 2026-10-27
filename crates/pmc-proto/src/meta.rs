use base64::Engine;
use bytes::Bytes;
use pmc_store::numeric::{parse_i64_token, parse_u64_token};

use crate::MAX_KEY_LEN;

/// Parsed flags of a meta command. Return flags keep the order the client sent them in,
/// because replies list them in that order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaOpts {
    /// `b`: the key is base64-encoded.
    pub base64: bool,
    /// `q`: suppress the nominal reply (EN for mg, HD for ms/md/ma).
    pub quiet: bool,
    /// `O`: opaque token echoed back.
    pub opaque: Option<Bytes>,
    /// Return flags in request order: `c f h k l O s t`.
    pub ret: Vec<u8>,
    /// `v`: return the value.
    pub value: bool,
    /// `u`: do not bump the item (no access tracking).
    pub no_bump: bool,
    /// `C`: compare CAS.
    pub cas: Option<u64>,
    /// `T`: TTL to set.
    pub ttl: Option<i64>,
    /// `N`: autovivify on miss with this TTL.
    pub vivify: Option<i64>,
    /// `R`: win the recache token if the remaining TTL is below this.
    pub recache: Option<u64>,
    /// `F`: client flags (ms).
    pub client_flags: Option<u32>,
    /// `I`: invalidate (md: mark stale; ms: store as stale if CAS is older).
    pub invalidate: bool,
    /// `x` (md): replace the value with an empty one but keep the item.
    pub remove_value: bool,
    /// `M`: mode (ms: E A P R S; ma: I + D -).
    pub mode: Option<u8>,
    /// `J` (ma): initial value on autovivify.
    pub initial: Option<u64>,
    /// `D` (ma): delta.
    pub delta: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetaCmd {
    Get,
    Set,
    Delete,
    Arith,
    Debug,
}

pub(crate) type MetaResult<T> = Result<T, &'static str>;

pub(crate) const BAD_FORMAT: &str = "CLIENT_ERROR bad command line format";
const BAD_TOKEN: &str = "CLIENT_ERROR bad token in command line format";
/// memcached allows at most this many tokens in a meta command line.
pub(crate) const MAX_META_TOKENS: usize = 20;

/// Parses meta flags. memcached shares one flag table between all meta commands: a known flag
/// that means nothing for this command is ignored, an unknown one is an error.
pub(crate) fn parse_flags(cmd: MetaCmd, tokens: &[&[u8]], line: &Bytes) -> MetaResult<MetaOpts> {
    let r = parse_flags_inner(cmd, tokens, line);
    if cmd == MetaCmd::Arith {
        // `ma` reports every flag problem with one string.
        return r.map_err(|e| if e.contains("invalid mode") { e } else { "CLIENT_ERROR invalid or duplicate flag" });
    }
    r
}

fn parse_flags_inner(cmd: MetaCmd, tokens: &[&[u8]], line: &Bytes) -> MetaResult<MetaOpts> {
    let mut o = MetaOpts::default();
    let mut seen = [false; 128];
    for tok in tokens {
        let f = tok[0];
        if !f.is_ascii_alphanumeric() {
            return Err(BAD_FORMAT);
        }
        if seen[f as usize] {
            return Err("CLIENT_ERROR duplicate flag");
        }
        seen[f as usize] = true;
        let arg = &tok[1..];
        let num = || parse_u64_token(arg).ok_or(BAD_TOKEN);
        let signed = || parse_i64_token(arg).ok_or(BAD_TOKEN);
        match f {
            // `b` is not a positional return flag: it follows `k` in replies.
            b'b' => o.base64 = true,
            b'c' | b'f' | b'h' | b'k' | b'l' | b's' | b't' => o.ret.push(f),
            b'O' => {
                if arg.len() > 31 {
                    return Err("CLIENT_ERROR opaque token too long");
                }
                // Zero-copy: the opaque token lives inside the request line.
                let start = arg.as_ptr() as usize - line.as_ptr() as usize;
                o.opaque = Some(line.slice(start..start + arg.len()));
                o.ret.push(f);
            }
            b'q' => o.quiet = true,
            b'u' => o.no_bump = true,
            b'v' => o.value = true,
            b'C' => o.cas = Some(num()?),
            b'T' => o.ttl = Some(signed()?),
            b'N' => o.vivify = Some(signed()?),
            b'R' => o.recache = Some(num()?),
            b'F' => o.client_flags = Some(u32::try_from(num()?).map_err(|_| BAD_TOKEN)?),
            b'I' => o.invalidate = true,
            b'x' => o.remove_value = true,
            b'M' => {
                let Some(&m) = arg.first() else { return Err(mode_error(cmd)) };
                o.mode = Some(m);
            }
            b'J' => o.initial = Some(parse_u64_token(arg).ok_or("CLIENT_ERROR invalid numeric initial value")?),
            b'D' => o.delta = Some(parse_u64_token(arg).ok_or("CLIENT_ERROR invalid numeric delta value")?),
            // Proxy routing hints; always ignored.
            b'P' | b'L' => {}
            // Explicit CAS (`E`) is not supported: CAS values come from the replicated log.
            _ => return Err("CLIENT_ERROR invalid flag"),
        }
    }
    match (cmd, o.mode) {
        (MetaCmd::Set, Some(m)) if !matches!(m, b'E' | b'A' | b'P' | b'R' | b'S') => Err(mode_error(cmd)),
        (MetaCmd::Arith, Some(m)) if !matches!(m, b'I' | b'+' | b'D' | b'-') => Err(mode_error(cmd)),
        _ => Ok(o),
    }
}

fn mode_error(cmd: MetaCmd) -> &'static str {
    match cmd {
        MetaCmd::Arith => "CLIENT_ERROR invalid mode for ma M token",
        _ => "CLIENT_ERROR invalid mode for ms M token",
    }
}

/// Resolves the key token, decoding base64 when the `b` flag is present.
pub(crate) fn meta_key(raw: &[u8], line: &Bytes, base64: bool) -> MetaResult<Bytes> {
    if base64 {
        let decoded =
            base64::engine::general_purpose::STANDARD.decode(raw).map_err(|_| "CLIENT_ERROR error decoding key")?;
        if decoded.is_empty() || decoded.len() > MAX_KEY_LEN {
            return Err(BAD_FORMAT);
        }
        Ok(Bytes::from(decoded))
    } else {
        if raw.len() > MAX_KEY_LEN {
            return Err(BAD_FORMAT);
        }
        let start = raw.as_ptr() as usize - line.as_ptr() as usize;
        Ok(line.slice(start..start + raw.len()))
    }
}
