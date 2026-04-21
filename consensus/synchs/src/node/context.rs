use bytes::Bytes;
use config::{ClientId, Node};
use fnv::FnvHashMap as HashMap;
use libcrypto::{hash::Hash, ed25519, secp256k1, Keypair, PublicKey};
use libmempool::{BatchCache, BatchHash, CachedBatch, ConsensusMempoolMsg};
use libstorage::rocksdb::Storage as RocksStore;
use std::collections::VecDeque;
use std::sync::Arc;
use tls_reliable_sender::{CancelHandler, TlsReliableSender};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::error::TryRecvError;
use tokio_util::time::DelayQueue;
use types::synchs::{
    Block, Certificate, ClientMsg, Height, ProtocolMsg, Propose, Replica, Storage, Transaction,
    View, GENESIS_BLOCK,
};

pub struct Context {
    /// Consensus network: node-to-node `ProtocolMsg` delivery.
    pub consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    /// Client network: push `ClientMsg` to every registered client.
    pub client_net: TlsReliableSender<ClientId, ClientMsg>,
    /// All peer ids except my own -- the broadcast list reused for
    /// every multicast so we don't rebuild it per proposal.
    pub broadcast_peers: Vec<Replica>,
    /// All client ids -- also precomputed once.
    pub all_clients: Vec<ClientId>,
    /// Retained cancel handlers for peer sends, keyed by the `height`
    /// observed at send time. See `gc_handlers` for the retention rule.
    pub consensus_handlers: HashMap<Height, Vec<CancelHandler>>,
    /// Retained cancel handlers for client sends, same retention rule.
    pub client_handlers: HashMap<Height, Vec<CancelHandler>>,

    /// Handle to the shared batch store (`libstorage::Store` via
    /// rocksdb). Durable-write fallback; reads fall through here on
    /// a `batch_cache` miss.
    pub batch_store: RocksStore,

    /// In-memory, Arc-keyed batch cache shared with the libapollo-
    /// mempool pipeline. `read_batch` consults this first; `persist_batch`
    /// installs here before firing a background rocksdb write.
    pub batch_cache: Arc<BatchCache<Transaction>>,

    /// Batches the mempool has announced as ready, waiting for this
    /// node to become leader / unblock. Each entry is the `(hash,
    /// Arc<batch>)` pair the mempool's Processor forwarded -- the
    /// leader propses straight from the Arc, with no `read_batch`
    /// round-trip.
    pub pending_batches: VecDeque<(BatchHash<Transaction>, Arc<CachedBatch<Transaction>>)>,

    /// Control channel to the mempool. Currently only used to report
    /// round advancement so the synchronizer can GC old entries.
    pub tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Height, Transaction>>,

    /// Data context
    pub num_nodes: usize,
    pub myid: Replica,
    pub num_faults: usize,
    pub payload: usize,

    /// PKI
    pub my_secret_key: Keypair,
    pub pub_key_map: HashMap<Replica, PublicKey>,

    /// State context
    pub storage: Storage,
    pub cert_map: HashMap<Hash<Block>, Certificate>, // Contains all certified blocks
    pub height: Height,
    pub last_leader: Replica,
    pub last_seen_block: Arc<Block>,
    pub last_seen_cert: Certificate,
    pub last_committed_block_ht: Height,
    pub vote_map: HashMap<Hash<Block>, Certificate>,
    pub view: View,
    pub commit_queue: DelayQueue<Arc<Propose>>,
}

impl Context {
    pub fn new(
        config: &Node,
        consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
        client_net: TlsReliableSender<ClientId, ClientMsg>,
        batch_store: RocksStore,
        batch_cache: Arc<BatchCache<Transaction>>,
        tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Height, Transaction>>,
    ) -> Self {
        let genesis_arc = Arc::new(GENESIS_BLOCK);
        let broadcast_peers: Vec<Replica> = (0..config.num_nodes as Replica)
            .filter(|r| *r != config.id)
            .collect();
        let all_clients: Vec<ClientId> = config.client_net_map.keys().copied().collect();
        let mut c = Context {
            consensus_net,
            client_net,
            broadcast_peers,
            all_clients,
            consensus_handlers: HashMap::default(),
            client_handlers: HashMap::default(),
            batch_store,
            batch_cache,
            pending_batches: VecDeque::new(),
            tx_consensus_to_mem,
            num_nodes: config.num_nodes,
            my_secret_key: match config.crypto_alg {
                libcrypto::Algorithm::ED25519 => {
                    let kp: ed25519::Keypair = bincode::deserialize(&config.secret_key_bytes)
                        .expect("Failed to decode the secret key from the config");
                    Keypair::Ed25519(Box::new(kp))
                }
                libcrypto::Algorithm::SECP256K1 => {
                    let kp: secp256k1::Keypair = bincode::deserialize(&config.secret_key_bytes)
                        .expect("Failed to decode the secret key from the config");
                    Keypair::Secp256k1(kp)
                }
                _ => panic!("Unimplemented algorithm"),
            },
            pub_key_map: HashMap::default(),
            myid: config.id,
            num_faults: config.num_faults,
            storage: Storage::new(),
            height: 0,
            last_leader: 0,
            last_seen_block: genesis_arc.clone(),
            last_committed_block_ht: 0,
            cert_map: HashMap::default(),
            view: 0,
            last_seen_cert: Certificate::empty_cert(),
            vote_map: HashMap::default(),
            payload: config.payload * config.block_size,
            commit_queue: DelayQueue::new(),
        };
        for (id, pk_data) in config.pk_map.clone() {
            let pk = match config.crypto_alg {
                libcrypto::Algorithm::ED25519 => {
                    let kp: ed25519::PublicKey = bincode::deserialize(&pk_data)
                        .expect("Failed to decode the secret key from the config");
                    PublicKey::Ed25519(kp)
                }
                libcrypto::Algorithm::SECP256K1 => {
                    let sk: secp256k1::PublicKey = bincode::deserialize(&pk_data)
                        .expect("Failed to decode the secret key from the config");
                    PublicKey::Secp256k1(sk)
                }
                _ => panic!("Unimplemented algorithm"),
            };
            c.pub_key_map.insert(id, pk);
        }

        // Initialize storage
        c.storage.add_delivered_block(genesis_arc.clone());
        c.storage.add_committed_block(genesis_arc);
        c.cert_map
            .insert(GENESIS_BLOCK.hash.clone(), Certificate::empty_cert());
        c
    }

    /// For sync hotstuff, the next leader is the current leader
    pub fn next_leader(&self) -> Replica {
        self.last_leader
    }

    /// Leader of a view
    pub fn leader_of_view(&self) -> Replica {
        (self.view as usize % self.num_nodes) as Replica
    }

    /// Multicast a `ProtocolMsg` to every peer but myself.
    pub async fn multicast(&mut self, msg: &ProtocolMsg) {
        let bytes = Bytes::from(bincode::serialize(msg).expect("ProtocolMsg serialize"));
        let results = self
            .consensus_net
            .broadcast(&self.broadcast_peers, bytes)
            .await;
        for r in results {
            match r {
                Ok(h) => self.remember_consensus(h),
                Err(e) => log::warn!("consensus broadcast leg failed: {:?}", e),
            }
        }
    }

    /// Multicast a `ClientMsg` to every registered client.
    pub async fn multicast_client(&mut self, msg: &ClientMsg) {
        if self.all_clients.is_empty() {
            return;
        }
        let bytes = Bytes::from(bincode::serialize(msg).expect("ClientMsg serialize"));
        let results = self.client_net.broadcast(&self.all_clients, bytes).await;
        for r in results {
            match r {
                Ok(h) => self.remember_client(h),
                Err(e) => log::warn!("client broadcast leg failed: {:?}", e),
            }
        }
    }

    /// Install an incoming `Batch` in the in-memory cache and enqueue
    /// a rocksdb write. Cache insertion is sync-ordered before the
    /// store.write so a read between the two still hits the cache.
    /// `libstorage::Store::write` is already a fire-and-forget mpsc
    /// send into the rocksdb writer task, so `.await`-ing it here is
    /// effectively free -- no detached `tokio::spawn` needed.
    pub async fn persist_batch(
        &mut self,
        batch_hash: BatchHash<Transaction>,
        batch: Arc<CachedBatch<Transaction>>,
    ) {
        self.batch_cache.insert(batch_hash.clone(), Arc::clone(&batch));
        let key = batch_hash.as_ref().to_vec();
        let bytes = bincode::serialize(batch.as_ref()).expect("Batch serialize");
        self.batch_store.write(key, bytes).await;
    }

    /// Read a batch by hash: in-memory cache first (99%+ of hits on
    /// the hot path), rocksdb fallback for crash recovery / evicted
    /// batches. A rocksdb hit is re-installed in the cache so
    /// subsequent reads are free.
    pub async fn read_batch(
        &mut self,
        batch_hash: &BatchHash<Transaction>,
    ) -> Option<Arc<CachedBatch<Transaction>>> {
        if let Some(cached) = self.batch_cache.get(batch_hash) {
            return Some(cached);
        }
        let key = batch_hash.as_ref().to_vec();
        match self.batch_store.read(key).await {
            Ok(Some(bytes)) => {
                match bincode::deserialize::<CachedBatch<Transaction>>(&bytes) {
                    Ok(b) => {
                        let arc = Arc::new(b);
                        self.batch_cache.insert(batch_hash.clone(), Arc::clone(&arc));
                        Some(arc)
                    }
                    Err(_) => None,
                }
            }
            _ => None,
        }
    }

    #[inline]
    fn remember_consensus(&mut self, h: CancelHandler) {
        self.consensus_handlers
            .entry(self.height)
            .or_default()
            .push(h);
    }

    #[inline]
    fn remember_client(&mut self, h: CancelHandler) {
        self.client_handlers
            .entry(self.height)
            .or_default()
            .push(h);
    }

    /// Garbage-collect resolved cancel handlers. See the matching
    /// comment in the pre-mempool version for the retention rule.
    pub fn gc_handlers(&mut self) {
        let gc = |map: &mut HashMap<Height, Vec<CancelHandler>>| {
            map.retain(|_, handlers| {
                handlers.retain_mut(|h| matches!(h.try_recv(), Err(TryRecvError::Empty)));
                !handlers.is_empty()
            });
        };
        gc(&mut self.consensus_handlers);
        gc(&mut self.client_handlers);
    }
}
