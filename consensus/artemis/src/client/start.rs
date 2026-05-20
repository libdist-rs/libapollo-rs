//! Artemis client — keyed-mempool / per-tx-confirmation variant.
//!
//! Submits transactions as `ClientMsg::NewBatch { batch, reply_to }`
//! to every server's mempool TCP port. Each tx is stamped with
//! `(client_id, monotonically-increasing nonce)` so the server-side
//! `Txpool` can dedupe across replicas and enforce replay protection.
//! Latency is measured per-tx via the server's confirmation router.

use bytes::Bytes;
use config::Client;
use consensus::statistics;
use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use libmempool::ClientMsg;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tcp_receiver::TcpReceiver;
use tcp_sender::TcpSimpleSender;
use tokio_stream::StreamExt;
use types::artemis::{Replica, Transaction};

pub async fn start(
    c: Arc<Client>,
    metric: u64,
    txs_per_burst: usize,
    burst_interval_ms: u64,
) {
    let mut peer_map: HashMap<Replica, SocketAddr> = HashMap::default();
    for (&id, addr) in &c.net_map {
        peer_map.insert(
            id,
            addr.parse()
                .unwrap_or_else(|_| panic!("invalid server addr for {}: {}", id, addr)),
        );
    }
    let all_servers: Vec<Replica> = peer_map.keys().copied().collect();
    let mut tx_net = TcpSimpleSender::<Replica, ClientMsg<Transaction>>::with_peers(peer_map);

    let reply_to: SocketAddr = c
        .my_listen_addr
        .parse()
        .expect("invalid client listen addr");
    let mut confirm_recv = TcpReceiver::<ClientMsg<Transaction>>::spawn(reply_to);

    let payload = c.payload;
    let client_id = c.my_id;
    let mut nonce: u64 = 1;

    let mut time_map: HashMap<Hash<Transaction>, SystemTime> = HashMap::default();
    let mut latency_map: HashMap<Hash<Transaction>, (SystemTime, SystemTime)> = HashMap::default();
    let mut num_cmds: u128 = 0;

    let burst_size = txs_per_burst.max(1);
    log::info!(
        "Artemis client {} starting: burst_size={} burst_interval={}ms metric={}",
        client_id, burst_size, burst_interval_ms, metric
    );
    let start = SystemTime::now();
    let mut burst_timer = tokio::time::interval(Duration::from_millis(burst_interval_ms));

    loop {
        tokio::select! {
            _ = burst_timer.tick() => {
                let mut batch: Vec<Transaction> = Vec::with_capacity(burst_size);
                let now = SystemTime::now();
                for _ in 0..burst_size {
                    let tx = Transaction::new_dummy_tx_keyed(client_id, nonce, payload);
                    nonce += 1;
                    let h = Hash::<Transaction>::ser_and_hash(&tx);
                    time_map.insert(h, now);
                    batch.push(tx);
                }
                let msg = ClientMsg::<Transaction>::NewBatch { batch, reply_to };
                let bytes = Bytes::from(bincode::serialize(&msg).expect("ClientMsg serialize"));
                let _ = tx_net.broadcast(&all_servers, bytes).await;
            },
            confirm_opt = confirm_recv.next() => {
                let now = SystemTime::now();
                let msg = match confirm_opt {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => { log::warn!("bad Confirmation bytes: {}", e); continue; }
                    None => { log::warn!("confirmation stream closed"); continue; }
                };
                if let ClientMsg::Confirmation(h) = msg {
                    if let Some(t0) = time_map.remove(&h) {
                        latency_map.insert(h, (t0, now));
                        num_cmds += 1;
                    }
                    while let Some(Ok(ClientMsg::Confirmation(h))) =
                        futures::FutureExt::now_or_never(confirm_recv.next()).flatten()
                    {
                        if let Some(t0) = time_map.remove(&h) {
                            latency_map.insert(h, (t0, now));
                            num_cmds += 1;
                        }
                    }
                }
            }
        }
        if num_cmds > metric as u128 {
            let now = SystemTime::now();
            statistics(now, start, latency_map);
            return;
        }
    }
}
