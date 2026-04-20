use bytes::Bytes;
use config::Client;
use consensus::statistics;
use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};
use libcrypto::hash::Hash;
use net_common::{CertSource, TlsOptions};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::channel;
use tokio_stream::StreamExt;
use types::apollo::{Block, ClientMsg, Replica, Transaction};

struct Context {
    pending: usize,
    time_map: HashMap<Hash<Transaction>, SystemTime>,
    count_map: HashMap<Hash<Block>, usize>,
    finished_map: HashSet<Hash<Block>>,
    latency_map: HashMap<Hash<Transaction>, (SystemTime, SystemTime)>,
    num_cmds: u128,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("Pending", &self.pending)
            .field("count_map", &self.count_map.len())
            .field("time_map", &self.time_map.len())
            .field("latency_map", &self.latency_map.len())
            .field("num_cmds", &self.num_cmds)
            .finish()
    }
}

impl Context {
    pub fn new() -> Self {
        Self {
            pending: 0,
            time_map: HashMap::default(),
            count_map: HashMap::default(),
            finished_map: HashSet::default(),
            latency_map: HashMap::default(),
            num_cmds: 0,
        }
    }
}

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

    let mut cx = Context::new();
    cx.pending = window;

    let start = SystemTime::now();
    let mut new_blocks: VecDeque<Arc<Block>> = VecDeque::new();
    loop {
        tokio::select! {
            tx_opt = recv.recv(), if cx.pending > 0 => {
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
                    cx.time_map.insert(hash, SystemTime::now());
                    cx.pending -= 1;
                    log::trace!("Sending transaction to every server");
                } else {
                    log::info!("Finished sending messages");
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
                let b = match msg {
                    ClientMsg::NewBlock(_p, b, _) => Arc::new(b),
                    _ => continue,
                };
                new_blocks.push_back(b);
                while let Some(Ok(ClientMsg::NewBlock(_p, b, _))) =
                    futures::FutureExt::now_or_never(block_recv.next()).flatten()
                {
                    new_blocks.push_back(Arc::new(b));
                }
                process_blocks(c, now, &mut new_blocks, &mut cx);
                log::debug!("Sending {} commands to the nodes", cx.pending);
            }
        }
        if cx.num_cmds > m as u128 {
            let now = SystemTime::now();
            statistics(now, start, cx.latency_map);
            return;
        }
    }
}

fn process_blocks(
    c: &Client,
    now: SystemTime,
    new_blocks: &mut VecDeque<Arc<Block>>,
    cx: &mut Context,
) {
    log::debug!("Processing new {:?}", new_blocks);
    log::debug!("Before processing: {:?}", cx);
    for b in new_blocks.drain(..) {
        // Check if the block is valid?
        if !cx.count_map.contains_key(&b.hash) {
            cx.count_map.insert(b.hash.clone(), 1);
            continue;
        }
        let ct = *cx.count_map.get(&b.hash).unwrap();
        if ct < c.num_faults {
            cx.count_map.insert(b.hash.clone(), ct + 1);
            continue;
        }
        if cx.finished_map.contains(&b.hash) {
            continue;
        }
        cx.pending += c.block_size;
        cx.num_cmds += c.block_size as u128;
        for t in &b.body.tx_hashes {
            if let Some(old) = cx.time_map.get(t) {
                cx.latency_map.insert(t.clone(), (*old, now));
            } else {
                log::warn!("transaction not found in time map");
                cx.num_cmds -= 1;
            }
        }
        cx.finished_map.insert(b.hash.clone());
    }
    log::debug!("After processing: {:?}", cx);
}
