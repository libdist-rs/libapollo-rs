use bytes::Bytes;
use config::Client;
use consensus::statistics;
use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};
use libcrypto::hash::Hash;
use net_common::{CertSource, TlsOptions};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::channel;
use tokio_stream::StreamExt;
use types::optsync::{Block, ClientMsg, Replica, Transaction};

pub async fn start(c: &Client, metric: u64, window: usize) {
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

    let (send, mut recv) = channel(util::CHANNEL_SIZE);
    let m = metric;
    let payload = c.payload;
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
    let mut pending = window;
    let mut time_map: HashMap<Hash<Transaction>, SystemTime> = HashMap::default();
    let mut count_map: HashMap<Hash<Block>, usize> = HashMap::default();
    let mut finished_map: HashSet<Hash<Block>> = HashSet::default();
    let mut latency_map: HashMap<Hash<Transaction>, (SystemTime, SystemTime)> = HashMap::default();
    let mut num_cmds: u128 = 0;

    let start = SystemTime::now();
    loop {
        tokio::select! {
            tx_opt = recv.recv(), if pending > 0 => {
                if let Some(x) = tx_opt {
                    let hash = Hash::<Transaction>::ser_and_hash(x.as_ref());
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
                    time_map.insert(hash, SystemTime::now());
                    pending -= 1;
                } else {
                    log::info!("Finished sending messages");
                    std::process::exit(0);
                }
            },
            block_opt = block_recv.next() => {
                match block_opt {
                    Some(Ok(ClientMsg::NewBlock(b, _))) => {
                        let entry = count_map.entry(b.hash.clone()).or_insert(0);
                        *entry += 1;
                        if *entry < c.num_faults + 1 {
                            continue;
                        }
                        if finished_map.contains(&b.hash) {
                            continue;
                        }
                        let now = SystemTime::now();
                        pending += c.block_size;
                        num_cmds += c.block_size as u128;
                        for t in &b.body.tx_hashes {
                            if let Some(old) = time_map.get(t) {
                                latency_map.insert(t.clone(), (*old, now));
                            } else {
                                log::warn!("transaction not found in time map");
                                num_cmds -= 1;
                            }
                        }
                        finished_map.insert(b.hash.clone());
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => log::warn!("bad ClientMsg bytes: {}", e),
                    None => panic!("server push listener closed"),
                }
            }
        }
        if num_cmds > m as u128 {
            let now = SystemTime::now();
            statistics(now, start, latency_map);
            return;
        }
    }
}
