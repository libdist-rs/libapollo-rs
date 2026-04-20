use bytes::Bytes;
use config::Client;
use consensus::statistics;
use fnv::FnvHashMap as HashMap;
use net_common::{CertSource, TlsOptions};
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_stream::StreamExt;
use types::artemis::{Block, ClientMsg, Payload, Replica, Transaction, UCRVote};
use types::BlockTrait;
use super::Context;

type TxFactory = Receiver<Arc<Transaction>>;

/// Setup a concurrent thread that produces a stream of dummy transactions
/// so that the main reactor has a buffer of transactions always ready to send to the nodes
async fn setup_tx_factory(payload: usize) -> TxFactory {
    let (send, recv) = channel(util::CHANNEL_SIZE);
    tokio::spawn(async move {
        let mut i = 0u64;
        loop {
            let tx = Transaction::new_dummy_tx(i, payload);
            i += 1;
            if let Err(e) = send.send(Arc::new(tx)).await {
                log::info!("Closing tx producer channel: {}", e);
                std::process::exit(0);
            }
        }
    });
    recv
}

pub async fn start(c: Arc<Client>, metric: u64, window: usize) {
    let tls = || TlsOptions {
        cert_source: CertSource::PemFiles {
            cert_chain: PathBuf::from(&c.my_cert_path),
            private_key: PathBuf::from(&c.my_cert_key_path),
        },
        ..TlsOptions::high_throughput()
    };

    let mut peer_map: HashMap<Replica, SocketAddr> = HashMap::default();
    for (&id, addr) in &c.net_map {
        peer_map.insert(
            id,
            addr.parse()
                .unwrap_or_else(|_| panic!("invalid server addr for {}: {}", id, addr)),
        );
    }
    let all_servers: Vec<Replica> = peer_map.keys().copied().collect();
    let mut tx_net = TlsReliableSender::<Replica, Transaction>::with_peers_and_options(
        peer_map,
        tls(),
    )
    .expect("tx sender setup");

    let listen: SocketAddr = c
        .my_listen_addr
        .parse()
        .expect("invalid client listen addr");
    let mut block_recv = TlsReceiver::<ClientMsg>::spawn_with_options(listen, tls());

    let mut cancel_handlers: Vec<tls_reliable_sender::CancelHandler> = Vec::new();
    let handler_budget = 4 * window;

    let payload = c.payload;
    let mut cx = Context::new(c.clone());
    let mut recv = setup_tx_factory(payload).await;
    let m = metric;
    cx.pending = window;
    cx.num_cmds = 0;

    let start = SystemTime::now();
    loop {
        tokio::select! {
            tx_opt = recv.recv(), if cx.pending > 0 => {
                if let Some(x) = tx_opt {
                    let hash = libcrypto::hash::Hash::<Transaction>::ser_and_hash(x.as_ref());
                    let bytes = Bytes::from(bincode::serialize(x.as_ref()).expect("tx serialize"));
                    let results = tx_net.broadcast(&all_servers, bytes).await;
                    for r in results {
                        if let Ok(h) = r {
                            cancel_handlers.push(h);
                        }
                    }
                    if cancel_handlers.len() > handler_budget {
                        cancel_handlers.retain_mut(|h| matches!(
                            h.try_recv(),
                            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                        ));
                    }
                    cx.time_map.insert(hash, SystemTime::now());
                    cx.pending -= 1;
                } else {
                    log::info!("TxFactory closed");
                    std::process::exit(0);
                }
            },
            block_opt = block_recv.next() => {
                let now = SystemTime::now();
                let msg = match block_opt {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => { log::warn!("bad ClientMsg bytes: {}", e); continue; }
                    None => panic!("server push listener closed"),
                };
                match msg {
                    ClientMsg::NewBlock(v, block_vec) => try_new_round(v, block_vec, &mut cx, now).await,
                    _ => continue,
                };
                while let Some(Ok(ClientMsg::NewBlock(v, block_vec))) =
                    futures::FutureExt::now_or_never(block_recv.next()).flatten()
                {
                    try_new_round(v, block_vec, &mut cx, now).await;
                }
            }
        }
        if cx.num_cmds > m as u128 {
            let now = SystemTime::now();
            statistics(now, start, cx.latency_map);
            return;
        }
    }
}

/// We got a new vote message. Check if we are in the correct round and then process it.
async fn try_new_round(
    v: UCRVote,
    block_vec: Vec<(Block, Payload)>,
    cx: &mut Context,
    ts: SystemTime,
) {
    // Wire-level validation (previously in ClientMsg::init).
    if block_vec.is_empty() {
        log::warn!("Got a vote with 0 blocks");
        return;
    }
    if block_vec.last().unwrap().0.get_hash() != v.hash {
        log::warn!("The hash of the last block does not match the vote's hash");
        return;
    }

    if cx.round() < v.round {
        log::debug!("We got a vote from the future");
        cx.future_msgs.insert(v.round, (v, block_vec));
        return;
    }
    if cx.round() > v.round {
        log::warn!("We got a vote from a round that we have already processed for");
        return;
    }
    new_round(v, block_vec, cx, ts).await;
    while let Some((v, block_vec)) = cx.future_msgs.remove(&cx.round()) {
        new_round(v, block_vec, cx, ts).await;
    }
}

/// Processing votes for the correct round
async fn new_round(
    v: UCRVote,
    block_vec: Vec<(Block, Payload)>,
    cx: &mut Context,
    ts: SystemTime,
) {
    for (b, _) in block_vec {
        cx.pending += b.blk.body.tx_hashes.len();
        cx.storage.add_delivered_block(Arc::new(b));
    }
    let v = Arc::new(v);
    cx.prop_chain.insert(v.round, v.clone());
    if v.round <= cx.config.num_faults {
        cx.update_round();
        return;
    }

    let com_round = v.round - cx.config.num_faults;
    let v = cx.prop_chain.get(&com_round).expect("Must have in prop map");

    let mut com_hash = v.hash.clone();
    while !cx.storage.is_committed_by_hash(&com_hash) {
        let b_rc = cx
            .storage
            .delivered_block_from_hash(&com_hash)
            .expect("Trying to commit an undelivered block");
        cx.storage.add_committed_block(b_rc.clone());
        com_hash = libcrypto::hash::Hash::<Block>::try_from(b_rc.blk.header.prev.as_ref())
            .expect("hash is exactly 32 bytes");
        for tx_hash in &b_rc.blk.body.tx_hashes {
            if let Some(start) = cx.time_map.remove(tx_hash) {
                cx.num_cmds += 1;
                cx.latency_map.insert(tx_hash.clone(), (start, ts));
            }
        }
    }

    cx.update_round();
}
