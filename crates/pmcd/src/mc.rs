//! memcached text protocol server.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use pmc_proto::encode::{self, MetaView};
use pmc_proto::{Frame, Limits, MetaOpts, Parser, Request, StoreMode};
use pmc_store::store::ITEM_OVERHEAD;
use pmc_store::{Item, Op, Reply};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};

use crate::router::Router;
use crate::shard::{ExecError, unix_now};
use crate::stats::{Stats, add, dec, inc};

pub const VERSION: &str = concat!("1.6.45-pmcd-", env!("CARGO_PKG_VERSION"));

pub struct Ctx {
    pub router: Arc<Router>,
    pub stats: Arc<Stats>,
    pub limits: Limits,
    pub memory_limit: u64,
}

pub async fn serve(listener: TcpListener, ctx: Arc<Ctx>) {
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(e) = conn(sock, &ctx).await {
                        tracing::debug!(error = %e, "connection closed with error");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

struct ConnGuard<'a>(&'a Stats);

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        dec(&self.0.counters.curr_connections);
    }
}

async fn flush(wr: &mut OwnedWriteHalf, out: &mut BytesMut, stats: &Stats) -> std::io::Result<()> {
    if !out.is_empty() {
        wr.write_all(out).await?;
        add(&stats.counters.bytes_written, out.len() as u64);
        out.clear();
    }
    Ok(())
}

async fn conn(sock: TcpStream, ctx: &Ctx) -> std::io::Result<()> {
    sock.set_nodelay(true)?;
    let stats = &*ctx.stats;
    inc(&stats.counters.curr_connections);
    inc(&stats.counters.total_connections);
    let _guard = ConnGuard(stats);
    let (mut rd, mut wr) = sock.into_split();
    let mut rbuf = BytesMut::with_capacity(16 * 1024);
    let mut out = BytesMut::with_capacity(16 * 1024);
    let mut parser = Parser::new(ctx.limits.clone());
    loop {
        // Requests are executed strictly in order; replies are batched until the input is drained.
        while let Some(frame) = parser.parse(&mut rbuf) {
            match frame {
                Frame::Request(Request::Quit) | Frame::Close => {
                    flush(&mut wr, &mut out, stats).await?;
                    return Ok(());
                }
                Frame::Request(req) => exec(ctx, req, &mut out).await,
                Frame::Error(e) => encode::line(&mut out, e.as_bytes()),
                Frame::Skip => {}
            }
            if out.len() >= 256 * 1024 {
                flush(&mut wr, &mut out, stats).await?;
            }
        }
        flush(&mut wr, &mut out, stats).await?;
        rbuf.reserve(parser.need().saturating_sub(rbuf.len()).max(16 * 1024));
        let n = rd.read_buf(&mut rbuf).await?;
        if n == 0 {
            return Ok(());
        }
        add(&stats.counters.bytes_read, n as u64);
    }
}

fn server_error(out: &mut BytesMut, e: &ExecError) {
    match e {
        ExecError::OutOfMemory => encode::line(out, b"SERVER_ERROR out of memory storing object"),
        other => {
            let msg = other.to_string().replace(['\r', '\n'], " ");
            encode::line(out, format!("SERVER_ERROR {msg}").as_bytes());
        }
    }
}

fn delete_op(key: Bytes) -> Op {
    Op::Delete { key, cas: None, invalidate: false, exptime: None, remove_value: false }
}

fn ttl_of(item: &Item, now: u64) -> i64 {
    if item.exptime == 0 { -1 } else { item.exptime.saturating_sub(now) as i64 }
}

async fn exec(ctx: &Ctx, req: Request, out: &mut BytesMut) {
    let c = &ctx.stats.counters;
    let r = &*ctx.router;
    match req {
        Request::Get { keys, with_cas } => {
            add(&c.cmd_get, keys.len() as u64);
            match r.get_many(&keys, true).await {
                Ok(hits) => {
                    for (k, h) in keys.iter().zip(hits) {
                        match h {
                            Some(h) => {
                                inc(&c.get_hits);
                                encode::value(out, k, h.item.flags, &h.item.value, with_cas.then_some(h.item.cas));
                            }
                            None => inc(&c.get_misses),
                        }
                    }
                    encode::end(out);
                }
                Err(e) => server_error(out, &e),
            }
        }
        Request::Gat { exptime, keys, with_cas } => {
            add(&c.cmd_touch, keys.len() as u64);
            let mut body = BytesMut::new();
            for k in &keys {
                match r.write(Op::Touch { key: k.clone(), exptime }).await {
                    Ok(Reply::Item { item, .. }) => {
                        inc(&c.touch_hits);
                        encode::value(&mut body, k, item.flags, &item.value, with_cas.then_some(item.cas));
                    }
                    Ok(_) => inc(&c.touch_misses),
                    Err(e) => return server_error(out, &e),
                }
            }
            out.extend_from_slice(&body);
            encode::end(out);
        }
        Request::Store { mode, key, flags, exptime, value, cas, noreply } => {
            inc(&c.cmd_set);
            let res = r.write(Op::Store { mode, key, value, flags, exptime, cas, invalidate: false }).await;
            if cas.is_some() {
                match &res {
                    Ok(Reply::Stored { .. }) => inc(&c.cas_hits),
                    Ok(Reply::Exists) => inc(&c.cas_badval),
                    Ok(Reply::NotFound) => inc(&c.cas_misses),
                    _ => {}
                }
            }
            if matches!(res, Ok(Reply::TooLarge)) {
                inc(&c.store_too_large);
            }
            if matches!(res, Err(ExecError::OutOfMemory)) {
                inc(&c.store_no_memory);
            }
            if noreply {
                return;
            }
            match res {
                Ok(Reply::Stored { .. }) => encode::line(out, b"STORED"),
                Ok(Reply::NotStored) => encode::line(out, b"NOT_STORED"),
                Ok(Reply::Exists) => encode::line(out, b"EXISTS"),
                Ok(Reply::NotFound) => encode::line(out, b"NOT_FOUND"),
                Ok(Reply::TooLarge) => encode::line(out, b"SERVER_ERROR object too large for cache"),
                Ok(other) => server_error(out, &crate::shard::unexpected(other)),
                Err(e) => server_error(out, &e),
            }
        }
        Request::TooLarge { mode, key, noreply } => {
            inc(&c.cmd_set);
            inc(&c.store_too_large);
            // Like memcached: a failed set drops the old value so stale data does not linger.
            if mode == StoreMode::Set {
                let _ = r.write(delete_op(key)).await;
            }
            if !noreply {
                encode::line(out, b"SERVER_ERROR object too large for cache");
            }
        }
        Request::Delete { key, noreply } => {
            let res = r.write(delete_op(key)).await;
            match &res {
                Ok(Reply::Deleted) => inc(&c.delete_hits),
                Ok(Reply::NotFound) => inc(&c.delete_misses),
                _ => {}
            }
            if noreply {
                return;
            }
            match res {
                Ok(Reply::Deleted) => encode::line(out, b"DELETED"),
                Ok(_) => encode::line(out, b"NOT_FOUND"),
                Err(e) => server_error(out, &e),
            }
        }
        Request::Arith { key, incr, delta, noreply } => {
            let res = r.write(Op::Arith { key, incr, delta, cas: None, vivify: None, exptime: None }).await;
            let (hits, misses) = if incr { (&c.incr_hits, &c.incr_misses) } else { (&c.decr_hits, &c.decr_misses) };
            match &res {
                Ok(Reply::Arith { .. }) => inc(hits),
                Ok(Reply::NotFound) => inc(misses),
                _ => {}
            }
            if noreply {
                return;
            }
            match res {
                Ok(Reply::Arith { value, .. }) => encode::number_line(out, value),
                Ok(Reply::NotFound) => encode::line(out, b"NOT_FOUND"),
                Ok(Reply::NonNumeric) => {
                    encode::line(out, b"CLIENT_ERROR cannot increment or decrement non-numeric value")
                }
                Ok(other) => server_error(out, &crate::shard::unexpected(other)),
                Err(e) => server_error(out, &e),
            }
        }
        Request::Touch { key, exptime, noreply } => {
            inc(&c.cmd_touch);
            let res = r.write(Op::Touch { key, exptime }).await;
            match &res {
                Ok(Reply::Item { .. }) => inc(&c.touch_hits),
                Ok(_) => inc(&c.touch_misses),
                _ => {}
            }
            if noreply {
                return;
            }
            match res {
                Ok(Reply::Item { .. }) => encode::line(out, b"TOUCHED"),
                Ok(_) => encode::line(out, b"NOT_FOUND"),
                Err(e) => server_error(out, &e),
            }
        }
        Request::FlushAll { delay, noreply } => {
            inc(&c.cmd_flush);
            let res = r.flush_all(delay.max(0) as u64).await;
            if noreply {
                return;
            }
            match res {
                Ok(()) => encode::line(out, b"OK"),
                Err(e) => server_error(out, &e),
            }
        }
        Request::Stats { args } => stats(ctx, &args, out).await,
        Request::Version => encode::line(out, format!("VERSION {VERSION}").as_bytes()),
        Request::Verbosity { noreply } | Request::CacheMemlimit { noreply, .. } => {
            if !noreply {
                encode::line(out, b"OK");
            }
        }
        Request::Quit => {}
        Request::MetaNoop => encode::line(out, b"MN"),
        Request::MetaGet { key, opts } => meta_get(ctx, key, opts, out).await,
        Request::MetaSet { key, value, opts } => meta_set(ctx, key, value, opts, out).await,
        Request::MetaDelete { key, opts } => meta_delete(ctx, key, opts, out).await,
        Request::MetaArith { key, opts } => meta_arith(ctx, key, opts, out).await,
        Request::MetaDebug { key, opts: _ } => match r.get_many(std::slice::from_ref(&key), false).await {
            Ok(mut hits) => match hits.pop().flatten() {
                Some(h) => {
                    let now = unix_now();
                    let line = format!(
                        "ME {} exp={} la={} cas={} fetch={} cls=1 size={}",
                        String::from_utf8_lossy(&key),
                        ttl_of(&h.item, now),
                        now.saturating_sub(h.last_access),
                        h.item.cas,
                        if h.fetched_before { "yes" } else { "no" },
                        key.len() + h.item.value.len() + ITEM_OVERHEAD,
                    );
                    encode::line(out, line.as_bytes());
                }
                None => encode::line(out, b"EN"),
            },
            Err(e) => server_error(out, &e),
        },
    }
}

/// A view that only carries a CAS, for ms/md/ma statuses.
fn cas_view(cas: u64) -> MetaView<'static> {
    MetaView { cas, ttl: -1, ..MetaView::default() }
}

async fn meta_get(ctx: &Ctx, key: Bytes, opts: MetaOpts, out: &mut BytesMut) {
    let c = &ctx.stats.counters;
    inc(&c.cmd_meta);
    inc(&c.cmd_get);
    if opts.ttl.is_some() {
        inc(&c.cmd_touch);
    }
    match ctx.router.meta_get(key.clone(), opts.ttl, opts.vivify, opts.recache, !opts.no_bump).await {
        Err(e) => server_error(out, &e),
        Ok(None) => {
            inc(&c.get_misses);
            if !opts.quiet {
                encode::meta_status(out, b"EN", &opts, &key, None);
            }
        }
        Ok(Some(h)) => {
            inc(&c.get_hits);
            let now = unix_now();
            let view = MetaView {
                value: &h.item.value,
                flags: h.item.flags,
                cas: h.item.cas,
                ttl: ttl_of(&h.item, now),
                fetched: h.fetched_before,
                last_access: now.saturating_sub(h.last_access),
                won: h.won,
                stale: h.item.is_stale(),
                already_won: h.already_won,
            };
            // `C`: "fetch only if changed" — drop the value when the client's CAS is current.
            let with_value = opts.value && opts.cas != Some(h.item.cas);
            if with_value {
                encode::meta_value(out, &opts, &key, &view);
            } else {
                encode::meta_status(out, b"HD", &opts, &key, Some(&view));
            }
        }
    }
}

async fn meta_set(ctx: &Ctx, key: Bytes, value: Bytes, opts: MetaOpts, out: &mut BytesMut) {
    let c = &ctx.stats.counters;
    inc(&c.cmd_meta);
    inc(&c.cmd_set);
    let mode = match opts.mode.unwrap_or(b'S') {
        b'E' => StoreMode::Add,
        b'A' => StoreMode::Append,
        b'P' => StoreMode::Prepend,
        b'R' => StoreMode::Replace,
        _ => StoreMode::Set,
    };
    let flags = opts.client_flags.unwrap_or(0);
    // `C` means nothing for add: the item must be missing anyway.
    let cas = if mode == StoreMode::Add { None } else { opts.cas };
    let op = Op::Store {
        mode,
        key: key.clone(),
        value: value.clone(),
        flags,
        exptime: opts.ttl.unwrap_or(0),
        cas,
        invalidate: opts.invalidate,
    };
    let mut res = ctx.router.write(op).await;
    if let (Ok(Reply::NotStored), Some(ttl), StoreMode::Append | StoreMode::Prepend) = (&res, opts.vivify, mode) {
        // `N` with append/prepend: create the item on miss.
        let add = Op::Store {
            mode: StoreMode::Add,
            key: key.clone(),
            value,
            flags,
            exptime: ttl,
            cas: None,
            invalidate: false,
        };
        res = ctx.router.write(add).await;
    }
    match res {
        Ok(Reply::Stored { cas }) => {
            if !opts.quiet {
                encode::meta_status(out, b"HD", &opts, &key, Some(&cas_view(cas)));
            }
        }
        Ok(Reply::NotStored) => encode::meta_status(out, b"NS", &opts, &key, Some(&cas_view(0))),
        Ok(Reply::Exists) => encode::meta_status(out, b"EX", &opts, &key, Some(&cas_view(0))),
        Ok(Reply::NotFound) => encode::meta_status(out, b"NF", &opts, &key, Some(&cas_view(0))),
        Ok(Reply::TooLarge) => {
            inc(&c.store_too_large);
            encode::line(out, b"SERVER_ERROR object too large for cache");
        }
        Ok(other) => server_error(out, &crate::shard::unexpected(other)),
        Err(e) => server_error(out, &e),
    }
}

async fn meta_delete(ctx: &Ctx, key: Bytes, opts: MetaOpts, out: &mut BytesMut) {
    let c = &ctx.stats.counters;
    inc(&c.cmd_meta);
    let op = Op::Delete {
        key: key.clone(),
        cas: opts.cas,
        invalidate: opts.invalidate,
        exptime: opts.ttl,
        remove_value: opts.remove_value,
    };
    match ctx.router.write(op).await {
        Ok(Reply::Deleted) => {
            inc(&c.delete_hits);
            if !opts.quiet {
                encode::meta_status(out, b"HD", &opts, &key, None);
            }
        }
        Ok(Reply::NotFound) => {
            inc(&c.delete_misses);
            encode::meta_status(out, b"NF", &opts, &key, None);
        }
        Ok(Reply::Exists) => encode::meta_status(out, b"EX", &opts, &key, None),
        Ok(other) => server_error(out, &crate::shard::unexpected(other)),
        Err(e) => server_error(out, &e),
    }
}

async fn meta_arith(ctx: &Ctx, key: Bytes, opts: MetaOpts, out: &mut BytesMut) {
    let c = &ctx.stats.counters;
    inc(&c.cmd_meta);
    let incr = !matches!(opts.mode, Some(b'D') | Some(b'-'));
    let op = Op::Arith {
        key: key.clone(),
        incr,
        delta: opts.delta.unwrap_or(1),
        cas: opts.cas,
        vivify: opts.vivify.map(|ttl| (opts.initial.unwrap_or(0), ttl)),
        exptime: opts.ttl,
    };
    let (hits, misses) = if incr { (&c.incr_hits, &c.incr_misses) } else { (&c.decr_hits, &c.decr_misses) };
    match ctx.router.write(op).await {
        Ok(Reply::Arith { value, item }) => {
            hits.fetch_add(1, Relaxed);
            if opts.quiet {
                return;
            }
            let digits = value.to_string();
            let view = MetaView {
                value: digits.as_bytes(),
                flags: item.flags,
                cas: item.cas,
                ttl: ttl_of(&item, unix_now()),
                ..MetaView::default()
            };
            if opts.value {
                encode::meta_value(out, &opts, &key, &view);
            } else {
                encode::meta_status(out, b"HD", &opts, &key, Some(&view));
            }
        }
        Ok(Reply::NotFound) => {
            misses.fetch_add(1, Relaxed);
            encode::meta_status(out, b"NF", &opts, &key, None);
        }
        Ok(Reply::Exists) => encode::meta_status(out, b"EX", &opts, &key, None),
        Ok(Reply::NonNumeric) => encode::line(out, b"CLIENT_ERROR cannot increment or decrement non-numeric value"),
        Ok(other) => server_error(out, &crate::shard::unexpected(other)),
        Err(e) => server_error(out, &e),
    }
}

async fn stats(ctx: &Ctx, args: &[Bytes], out: &mut BytesMut) {
    let sub = args.first().map(|a| &a[..]);
    match sub {
        None => {
            let r = &ctx.router;
            let now = unix_now();
            encode::stat(out, "pid", std::process::id());
            encode::stat(out, "uptime", ctx.stats.started.elapsed().as_secs());
            encode::stat(out, "time", now);
            encode::stat(out, "version", VERSION);
            encode::stat(out, "libevent", "tokio");
            encode::stat(out, "pointer_size", usize::BITS);
            encode::stat(out, "rusage_user", "0.000000");
            encode::stat(out, "rusage_system", "0.000000");
            encode::stat(out, "max_connections", 0);
            for (name, v) in ctx.stats.counters.snapshot() {
                encode::stat(out, name, v);
            }
            encode::stat(out, "limit_maxbytes", ctx.memory_limit);
            encode::stat(out, "threads", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1));
            let items = r.store_stats().await;
            encode::stat(out, "bytes", items.bytes);
            encode::stat(out, "curr_items", items.curr_items);
            encode::stat(out, "total_items", items.total_items);
            encode::stat(out, "evictions", 0);
            encode::stat(out, "reclaimed", 0);
            let local = r.local_shards();
            encode::stat(out, "pmcd_node_id", r.node_id);
            encode::stat(out, "pmcd_shards", r.topo.shards.len());
            encode::stat(out, "pmcd_local_shards", local.len());
            encode::stat(out, "pmcd_leader_shards", local.iter().filter(|s| s.is_leader()).count());
            encode::end(out);
        }
        Some(b"settings") => {
            encode::stat(out, "maxbytes", ctx.memory_limit);
            encode::stat(out, "item_size_max", ctx.limits.max_item_size);
            encode::stat(out, "cas_enabled", "yes");
            encode::stat(out, "evictions", "off");
            encode::stat(out, "client_flags_size", 4);
            encode::end(out);
        }
        Some(b"items") | Some(b"slabs") | Some(b"sizes") | Some(b"conns") => encode::end(out),
        Some(b"reset") => encode::line(out, b"RESET"),
        Some(_) => encode::line(out, b"ERROR"),
    }
}
