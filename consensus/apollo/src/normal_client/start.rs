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
use tcp_sender::TcpSimpleSender;
use tls_receiver::TlsReceiver;
use tokio::sync::mpsc::channel;
use tokio_stream::StreamExt;
use types::apollo::{Block, ClientMsg, Replica, Transaction};

struct Context {
    pending: usize,
    time_map: HashMap<Hash<Transaction>, SystemTime>,
    count_map: HashMap<Hash<Block>, usize>,
    finished_map: HashSet<Hash<Block>>,
    tx_hash_map: HashMap<Hash<Block>, Vec<Hash<Transaction>>>,
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
            tx_hash_map: HashMap::default(),
            latency_map: HashMap::default(),
            num_cmds: 0,
        }
    }
}

pub async fn start(c: &Client, metric: u64, window: usize) {
    // Outgoing tx submission: plaintext TCP into each node's mempool.
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

    // Incoming `ClientMsg::NewBlock` pushes: still TLS.
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
    let mut new_blocks: VecDeque<(Arc<Block>, Vec<Hash<Transaction>>)> = VecDeque::new();
    loop {
        tokio::select! {
            tx_opt = recv.recv(), if cx.pending > 0 => {
                if let Some(x) = tx_opt {
                    let hash = Hash::<Transaction>::ser_and_hash(x.as_ref());
                    let bytes = Bytes::from(bincode::serialize(x.as_ref()).expect("tx serialize"));
                    let _ = tx_net.broadcast(&all_servers, bytes).await;
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
                let (b, tx_hashes) = match msg {
                    ClientMsg::NewBlock(_p, b, tx_hashes, _) => (Arc::new(b), tx_hashes),
                    _ => continue,
                };
                new_blocks.push_back((b, tx_hashes));
                while let Some(Ok(ClientMsg::NewBlock(_p, b, tx_hashes, _))) =
                    futures::FutureExt::now_or_never(block_recv.next()).flatten()
                {
                    new_blocks.push_back((Arc::new(b), tx_hashes));
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
    new_blocks: &mut VecDeque<(Arc<Block>, Vec<Hash<Transaction>>)>,
    cx: &mut Context,
) {
    log::debug!("Processing new {:?}", new_blocks.len());
    log::debug!("Before processing: {:?}", cx);
    for (b, tx_hashes) in new_blocks.drain(..) {
        // Remember the tx hashes from the first delivery -- other
        // replicas will resend the same block hash with the same
        // payload, and the f+1 commit rule matches on block hash.
        cx.tx_hash_map
            .entry(b.hash.clone())
            .or_insert_with(|| tx_hashes);

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
        if let Some(hashes) = cx.tx_hash_map.get(&b.hash) {
            for t in hashes {
                if let Some(old) = cx.time_map.get(t) {
                    cx.latency_map.insert(t.clone(), (*old, now));
                } else {
                    log::warn!("transaction not found in time map");
                    cx.num_cmds -= 1;
                }
            }
        }
        cx.finished_map.insert(b.hash.clone());
    }
    log::debug!("After processing: {:?}", cx);
}
