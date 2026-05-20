use bytes::Bytes;
use config::Client;
use consensus::statistics_latency;
use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use net_common::{CertSource, TlsOptions};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tcp_sender::TcpSimpleSender;
use tls_receiver::TlsReceiver;
use tokio::sync::mpsc::channel;
use tokio_stream::StreamExt;
use types::apollo::{ClientMsg, Propose, Replica, Transaction};
use super::Context;

/// Run the apollo client.
///
/// - `rate == 0`: original closed-loop with `window`-based flow control
///   (producer task fills a bounded channel; main reactor sends one tx
///   per slot freed by a commit).  `window` is the credit count.
/// - `rate > 0`:  open-loop burst pacing.  Every `burst_interval_ms`
///   the reactor sends `rate * burst_interval_ms / 1000` transactions
///   regardless of commit speed.  `window` is ignored.  Matches the
///   pattern used by leto/zeus's stresser so Pareto sweeps see honest
///   offered-rate response curves.
pub async fn start(
    c: &Client,
    metric: u64,
    window: usize,
    rate: u64,
    burst_interval_ms: u64,
) {
    // Outgoing tx submission: plaintext TCP into each node's mempool
    // client listener.
    let mut peer_map: HashMap<Replica, SocketAddr> = HashMap::default();
    for (&id, addr) in &c.net_map {
        peer_map.insert(
            id,
            addr.parse()
                .unwrap_or_else(|_| panic!("invalid server addr for {}: {}", id, addr)),
        );
    }
    let all_servers: Vec<Replica> = peer_map.keys().copied().collect();
    let mut tx_net = TcpSimpleSender::<Replica, Transaction>::with_peers(peer_map);

    // Incoming `ClientMsg` pushes: TLS, using client's cert as the
    // server-side identity.
    let tls = || TlsOptions {
        cert_source: CertSource::PemFiles {
            cert_chain: PathBuf::from(&c.my_cert_path),
            private_key: PathBuf::from(&c.my_cert_key_path),
        },
        ..TlsOptions::high_throughput()
    };
    let listen: SocketAddr = c
        .my_listen_addr
        .parse()
        .expect("invalid client listen addr");
    let mut block_recv = TlsReceiver::<ClientMsg>::spawn_with_options(listen, tls());

    let payload = c.payload;

    let mut cx = Context::new();
    cx.pending = window;
    cx.num_cmds = 0;

    // Warmup (shared between both paths): burst-send `f * block_size`
    // transactions so the chain can warm up, then absorb the first `f`
    // blocks so `cx.round` is synced before latency tracking starts.
    let mut tx_counter: u64 = 0;
    let first_send = c.num_faults * c.block_size;
    log::debug!("Warmup: sending {} transactions", first_send);
    for _ in 0..first_send {
        let tx = Transaction::new_dummy_tx(tx_counter, payload);
        tx_counter += 1;
        let bytes = Bytes::from(bincode::serialize(&tx).expect("tx serialize"));
        let _ = tx_net.broadcast(&all_servers, bytes).await;
    }

    let first_recv = c.num_faults as u64;
    log::debug!("Warmup: receiving first {} blocks", first_recv);
    for _ in 0..first_recv {
        let msg = match block_recv.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => panic!("bad ClientMsg bytes: {}", e),
            None => panic!("server push listener closed"),
        };
        let (prop, block, _tx_hashes) = match msg {
            ClientMsg::NewBlock(p, b, tx_hashes, _pl) => (p, b, tx_hashes),
            _ => continue,
        };
        update_props(prop, block, &mut cx);
        while let Some(p) = cx.future_msgs.remove(&cx.round) {
            let b = cx
                .storage
                .delivered_block_from_hash(&p.block_hash)
                .expect("block must be in storage");
            if !cx.storage.is_delivered_by_hash(&b.header.prev.clone()) {
                panic!("Got an undelivered block");
            }
            cx.round += 1;
        }
    }
    log::info!("Warmup complete at round {}", cx.round);

    let m = metric;
    let start = SystemTime::now();

    if rate == 0 {
        // ── Closed-loop path (original behaviour) ────────────────────
        // Producer task: generates a continuous stream of dummy txs
        // into a bounded channel.  Drained by the main reactor only
        // when `cx.pending > 0` (commits free up credit).
        let (send, mut recv) = channel(util::CHANNEL_SIZE);
        let payload_for_producer = payload;
        let mut producer_counter = tx_counter;
        tokio::spawn(async move {
            loop {
                let tx = Transaction::new_dummy_tx(producer_counter, payload_for_producer);
                producer_counter += 1;
                if let Err(e) = send.send(Arc::new(tx)).await {
                    log::info!("Closing tx producer channel: {}", e);
                    std::process::exit(0);
                }
            }
        });

        loop {
            tokio::select! {
                tx_opt = recv.recv(), if cx.pending > 0 => {
                    if let Some(x) = tx_opt {
                        let hash = Hash::<Transaction>::ser_and_hash(x.as_ref());
                        let bytes = Bytes::from(bincode::serialize(x.as_ref()).expect("tx serialize"));
                        let _ = tx_net.broadcast(&all_servers, bytes).await;
                        cx.time_map.insert(hash, SystemTime::now());
                        cx.pending -= 1;
                    } else {
                        println!("Finished sending messages");
                        std::process::exit(0);
                    }
                },
                block_opt = block_recv.next() => {
                    let now = SystemTime::now();
                    let msg = match block_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            log::warn!("bad ClientMsg bytes: {}", e);
                            continue;
                        }
                        None => panic!("server push listener closed"),
                    };
                    let (prop, block, tx_hashes) = match msg {
                        ClientMsg::NewBlock(p, b, tx_hashes, _pl) => (p, b, tx_hashes),
                        _ => continue,
                    };
                    let prop_round = prop.round;
                    update_props(prop, block, &mut cx);
                    cx.tx_hash_map.insert(prop_round, tx_hashes);
                    // Drain any other ready blocks in one pass.
                    while let Some(Ok(ClientMsg::NewBlock(p, b, tx_hashes, _))) =
                        futures::FutureExt::now_or_never(block_recv.next()).flatten()
                    {
                        let r = p.round;
                        update_props(p, b, &mut cx);
                        cx.tx_hash_map.insert(r, tx_hashes);
                    }
                    handle_new_blocks(c, &mut cx, now);
                }
            }
            if cx.num_cmds > m as u128 {
                let now = SystemTime::now();
                statistics_latency(now, start, cx.latency_map);
                return;
            }
        }
    } else {
        // ── Open-loop burst-paced path ───────────────────────────────
        // Every `burst_interval_ms`, send `burst_size` transactions.
        // Latency is tracked the same way (commits look up send time
        // in `cx.time_map`).  No `cx.pending` gating — if the chain
        // can't keep up, send-side queues at the kernel will fill and
        // `tx_net.broadcast` will naturally back-pressure.
        let burst_size = ((rate * burst_interval_ms) / 1000).max(1) as usize;
        log::info!(
            "Open-loop pacing: rate={} tx/s, burst_interval={} ms, burst_size={}",
            rate, burst_interval_ms, burst_size
        );
        let mut burst_timer = tokio::time::interval(Duration::from_millis(burst_interval_ms));

        loop {
            tokio::select! {
                _ = burst_timer.tick() => {
                    for _ in 0..burst_size {
                        let tx = Transaction::new_dummy_tx(tx_counter, payload);
                        tx_counter += 1;
                        let hash = Hash::<Transaction>::ser_and_hash(&tx);
                        let bytes = Bytes::from(bincode::serialize(&tx).expect("tx serialize"));
                        let _ = tx_net.broadcast(&all_servers, bytes).await;
                        cx.time_map.insert(hash, SystemTime::now());
                    }
                },
                block_opt = block_recv.next() => {
                    let now = SystemTime::now();
                    let msg = match block_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            log::warn!("bad ClientMsg bytes: {}", e);
                            continue;
                        }
                        None => panic!("server push listener closed"),
                    };
                    let (prop, block, tx_hashes) = match msg {
                        ClientMsg::NewBlock(p, b, tx_hashes, _pl) => (p, b, tx_hashes),
                        _ => continue,
                    };
                    let prop_round = prop.round;
                    update_props(prop, block, &mut cx);
                    cx.tx_hash_map.insert(prop_round, tx_hashes);
                    while let Some(Ok(ClientMsg::NewBlock(p, b, tx_hashes, _))) =
                        futures::FutureExt::now_or_never(block_recv.next()).flatten()
                    {
                        let r = p.round;
                        update_props(p, b, &mut cx);
                        cx.tx_hash_map.insert(r, tx_hashes);
                    }
                    handle_new_blocks(c, &mut cx, now);
                }
            }
            if cx.num_cmds > m as u128 {
                let now = SystemTime::now();
                statistics_latency(now, start, cx.latency_map);
                return;
            }
        }
    }
}

fn update_props(p: Propose, b: types::apollo::Block, cx: &mut Context) {
    if p.round < cx.round {
        if cx.storage.is_delivered_by_hash(&p.block_hash.clone()) {
            log::warn!("Got a block {} from the past - {}", p.round, cx.round);
            return;
        } else {
            // Someone equivocated.
            panic!("equivocation detected");
        }
    }
    let round = p.round;
    cx.storage.add_delivered_block(Arc::new(b));
    cx.future_msgs.insert(round, p);
}

fn handle_new_blocks(c: &Client, cx: &mut Context, now: SystemTime) {
    while let Some(p) = cx.future_msgs.remove(&cx.round) {
        let b = cx
            .storage
            .delivered_block_from_hash(&p.block_hash)
            .expect("block must have been added in update_props");
        if !cx.storage.is_delivered_by_hash(&b.header.prev.clone()) {
            panic!("Do not have parent for this block {:?}, yet", b);
        }
        cx.pending += c.block_size;
        if cx.round <= c.num_faults as u64 {
            cx.round += 1;
            return;
        }
        log::debug!("Adding block ht:{} in round {}", b.header.height, cx.round);
        let commit_round = cx.round - c.num_faults as u64;
        // `delivered_block_from_ht` uses the block's internal height,
        // which on the apollo client grows 1:1 with the round.
        let _commit_block = cx
            .storage
            .delivered_block_from_ht(commit_round)
            .unwrap_or_else(|| {
                panic!(
                    "Must be in the height map:cxr: {}, cmr:{}",
                    cx.round, commit_round
                )
            });

        // f+1 rule to commit the block. Tx hashes come from the
        // server-hydrated `ClientMsg::NewBlock` for the commit round.
        cx.num_cmds += c.block_size as u128;
        if let Some(tx_hashes) = cx.tx_hash_map.remove(&commit_round) {
            for t in &tx_hashes {
                if let Some(old) = cx.time_map.get(t) {
                    cx.latency_map.insert(t.clone(), (*old, now));
                } else {
                    cx.num_cmds -= 1;
                }
            }
        } else {
            log::warn!(
                "No tx hashes stashed for commit round {}; skipping latency update",
                commit_round
            );
        }
        cx.round += 1;
    }
}
