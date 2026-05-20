//! Apollo client — keyed-mempool / per-tx-confirmation variant.
//!
//! Submits transactions to every server via plaintext TCP as
//! `ClientMsg::NewBatch { batch, reply_to }`. Each tx is stamped with
//! `(client_id, monotonically-increasing nonce)` so the server-side
//! `Txpool` can dedupe across replicas and enforce replay protection.
//!
//! Latency is measured per-tx: the server's `ConfirmationRouter`
//! sends back `ClientMsg::Confirmation(Hash<Tx>)` for every committed
//! tx; the client matches the hash against a `time_map` populated at
//! send time. Throughput aggregates committed-tx count.

use bytes::Bytes;
use config::Client;
use consensus::statistics;
use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use libmempool::ClientMsg;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use tcp_receiver::TcpReceiver;
use tcp_sender::TcpSimpleSender;
use tokio_stream::StreamExt;
use types::apollo::{Replica, Transaction};

/// Run the apollo client.
///
/// - `metric` — number of confirmations to wait for before printing
///   `DP[Throughput]` / `DP[Latency]` and exiting.
/// - `txs_per_burst` / `burst_interval_ms` — open-loop burst pacing.
///   Every `burst_interval_ms` the client mints `txs_per_burst` txs
///   and broadcasts them as one `NewBatch` to every server.
pub async fn start(
    c: &Client,
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

    // Plain TCP listener for the per-tx `Confirmation` stream. The
    // mempool's confirmation router opens a fresh outbound TCP
    // connection per client address it sees, so no TLS handshake.
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
        "Apollo client {} starting: burst_size={} burst_interval={}ms metric={}",
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
                let msg = ClientMsg::<Transaction>::NewBatch {
                    batch,
                    reply_to,
                };
                let bytes = Bytes::from(bincode::serialize(&msg).expect("ClientMsg serialize"));
                let _ = tx_net.broadcast(&all_servers, bytes).await;
            },
            confirm_opt = confirm_recv.next() => {
                let now = SystemTime::now();
                let msg = match confirm_opt {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        log::warn!("bad Confirmation bytes: {}", e);
                        continue;
                    }
                    None => {
                        log::warn!("confirmation stream closed");
                        continue;
                    }
                };
                if let ClientMsg::Confirmation(h) = msg {
                    if let Some(t0) = time_map.remove(&h) {
                        latency_map.insert(h, (t0, now));
                        num_cmds += 1;
                    }
                    // Drain any pending confirmations in one pass.
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
