use bytes::Bytes;
use config::{ClientId, Node};
use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use libcrypto::{ed25519, secp256k1, Keypair, PublicKey};
use libmempool::{BatchCache, BatchHash, CachedBatch, ConsensusMempoolMsg};
use libstorage::rocksdb::Storage as RocksStore;
use std::collections::VecDeque;
use std::sync::Arc;
use tls_reliable_sender::{CancelHandler, TlsReliableSender};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::error::TryRecvError;
use types::apollo::{
    Block, ClientMsg, GENESIS_BLOCK, Propose, ProtocolMsg, Replica, Round, Storage, Transaction,
};

pub struct Context {
    /// Config context
    /// The number of nodes in the system
    num_nodes: usize,
    /// The number of faults in the system
    num_faults: usize,
    /// My ID
    myid: Replica,
    /// Everyone's public keys
    pub pub_key_map: HashMap<Replica, PublicKey>,
    /// My key
    pub my_secret_key: Arc<Keypair>,
    /// Whether the client supports Apollo or not
    is_client_apollo_enabled: bool,

    /// Consensus network: node-to-node `ProtocolMsg` delivery.
    pub consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    /// Client network: push `ClientMsg` to every registered client.
    pub client_net: TlsReliableSender<ClientId, ClientMsg>,
    /// All peer ids except my own (cached broadcast list).
    pub broadcast_peers: Vec<Replica>,
    /// All client ids (cached broadcast list).
    pub all_clients: Vec<ClientId>,
    /// Retained cancel handlers, keyed by the round observed at send
    /// time. See `gc_handlers` for the retention rule.
    pub consensus_handlers: HashMap<Round, Vec<CancelHandler>>,
    pub client_handlers: HashMap<Round, Vec<CancelHandler>>,

    /// Rocksdb fallback for batch reads; writes are fire-and-forget
    /// background tasks spawned from `persist_batch`.
    pub batch_store: RocksStore,

    /// In-memory batch cache shared with the libapollo-mempool
    /// pipeline. Read consulted on cache-first, write installed here
    /// before the rocksdb spawn.
    pub batch_cache: Arc<BatchCache<Transaction>>,

    /// Batches the mempool has announced as ready, paired with the
    /// `Arc<CachedBatch>` the Processor hands us. The leader pops
    /// from the front when `round_leader() == myid`; no rocksdb
    /// round-trip to read its own batch back out.
    pub pending_batches: VecDeque<(BatchHash<Transaction>, Arc<CachedBatch<Transaction>>)>,

    /// Control channel to the mempool -- currently only used for
    /// mempool-synchronizer round-advance notifications.
    pub tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Round, Transaction>>,

    // Reordering context: proposals that arrived with their block ride in
    // `prop_buf`; relays arrive block-less and fetch from storage (or
    // request) in `relay_buf`. `future_msgs` parks out-of-order proposals
    // after their block has already landed in storage. The `Replica`
    // tagged in each entry is the node to ask if a block (or parent) is
    // missing: for NewProposal that's `p.sig.origin` (the leader); for
    // Response/Relay that's the embedded `from` (who just forwarded).
    pub prop_buf: VecDeque<(Replica, Propose, Block, Arc<CachedBatch<Transaction>>)>,
    pub relay_buf: VecDeque<(Replica, Propose)>,
    pub other_buf: VecDeque<ProtocolMsg>,
    pub future_msgs: HashMap<Round, (Replica, Propose)>,

    /// Storage context -- block index only now; transaction pool lives
    /// in `libmempool-rs`'s `Mempool` / `Batcher` pipeline.
    pub storage: Storage,
    /// The chain of proposals: Map of block hash to its proposal
    pub prop_chain_by_round: HashMap<Round, Arc<Propose>>,
    pub prop_chain_by_hash: HashMap<Hash<Block>, Arc<Propose>>,

    /// Round state
    round: Round,
    round_leader: Replica,

    /// Per-block payload size used when pushing committed blocks out
    /// to clients (carried through from config).
    pub payload: usize,

    /// Block size, carried through from config; used by the throughput
    /// sampler to convert "committed blocks" into "committed txs".
    pub block_size: usize,

    /// Bench-only throughput sampler state. The reactor reads/writes
    /// these directly under a `tokio::time::interval` arm:
    /// - `bench_committed_tx_count` is incremented at every commit;
    /// - `bench_emit_window_secs` controls the tick rate;
    /// - `bench_metrics_node` is the single replica id that prints.
    pub bench_committed_tx_count: u64,
    pub bench_emit_window_secs: u64,
    pub bench_metrics_node: Replica,

    // Protocol state
    pub last_seen_block: Arc<Block>,
    /// The blocks we are waiting for, to handle propose messages
    pub prop_waiting: HashMap<Hash<Block>, Propose>,
    /// The blocks we are waiting for to handle the propose message
    pub prop_waiting_parent: HashMap<Hash<Block>, Propose>,
    pub req_ctr: u64,
}

impl Context {
    pub fn new(
        config: &Node,
        consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
        client_net: TlsReliableSender<ClientId, ClientMsg>,
        batch_store: RocksStore,
        batch_cache: Arc<BatchCache<Transaction>>,
        tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Round, Transaction>>,
        is_apollo_enabled: bool,
    ) -> Self {
        let broadcast_peers: Vec<Replica> = (0..config.num_nodes as Replica)
            .filter(|r| *r != config.id)
            .collect();
        let all_clients: Vec<ClientId> = config.client_net_map.keys().copied().collect();
        let mut c = Context {
            num_nodes: config.num_nodes,
            relay_buf: VecDeque::new(),
            num_faults: config.num_faults,
            myid: config.id,
            my_secret_key: match config.crypto_alg {
                libcrypto::Algorithm::ED25519 => {
                    let kp: ed25519::Keypair = bincode::deserialize(&config.secret_key_bytes)
                        .expect("Failed to decode the secret key from the config");
                    Arc::new(Keypair::Ed25519(Box::new(kp)))
                }
                libcrypto::Algorithm::SECP256K1 => {
                    let kp: secp256k1::Keypair = bincode::deserialize(&config.secret_key_bytes)
                        .expect("Failed to decode the secret key from the config");
                    Arc::new(Keypair::Secp256k1(kp))
                }
                _ => panic!("Unimplemented algorithm"),
            },
            pub_key_map: HashMap::default(),
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
            storage: Storage::new(),
            round_leader: 0,
            round: 1,
            future_msgs: HashMap::default(),
            last_seen_block: Arc::new(GENESIS_BLOCK),
            is_client_apollo_enabled: is_apollo_enabled,
            req_ctr: 0,
            prop_waiting: HashMap::default(),
            prop_waiting_parent: HashMap::default(),
            prop_chain_by_hash: HashMap::default(),
            prop_chain_by_round: HashMap::default(),
            prop_buf: VecDeque::new(),
            other_buf: VecDeque::new(),
            payload: config.payload,
            block_size: config.block_size,
            bench_committed_tx_count: 0,
            bench_emit_window_secs: config.bench_emit_window_secs.max(1),
            bench_metrics_node: config.bench_metrics_node,
        };
        for (id, pk_data) in &config.pk_map {
            if *id == c.myid {
                continue;
            }
            let pk = match config.crypto_alg {
                libcrypto::Algorithm::ED25519 => {
                    let kp: ed25519::PublicKey = bincode::deserialize(pk_data)
                        .expect("Failed to decode the secret key from the config");
                    PublicKey::Ed25519(kp)
                }
                libcrypto::Algorithm::SECP256K1 => {
                    let sk: secp256k1::PublicKey = bincode::deserialize(pk_data)
                        .expect("Failed to decode the secret key from the config");
                    PublicKey::Secp256k1(sk)
                }
                _ => panic!("Unimplemented algorithm"),
            };
            c.pub_key_map.insert(*id, pk);
        }
        // Initialize storage
        c.storage.add_delivered_block(c.last_seen_block.clone());
        c
    }

    #[inline]
    pub(crate) fn num_faults(&self) -> usize {
        self.num_faults
    }

    #[inline]
    pub(crate) fn myid(&self) -> Replica {
        self.myid
    }

    #[inline]
    pub(crate) fn round(&self) -> Round {
        self.round
    }

    #[inline]
    pub(crate) fn is_client_apollo_enabled(&self) -> bool {
        self.is_client_apollo_enabled
    }

    #[inline]
    pub(crate) fn round_leader(&self) -> Replica {
        self.round_leader
    }

    pub(crate) fn update_round(&mut self) {
        self.round_leader = self.next_leader();
        self.round += 1;
        // Every round advance is a natural checkpoint to reclaim
        // cancel-handler slots for messages the network has already
        // acked. See `gc_handlers` for the retention rule.
        self.gc_handlers();
    }

    pub(crate) fn next_leader(&self) -> Replica {
        self.next_of(self.round_leader)
    }

    pub(crate) fn next_of(&self, prev: Replica) -> Replica {
        if (prev as usize) + 1 == self.num_nodes {
            0
        } else {
            prev + 1
        }
    }

    /// Install an incoming batch in the cache; enqueue rocksdb write.
    /// Cache insertion is sync-ordered before the store.write so a
    /// read in-between still hits the cache. `libstorage::Store::write`
    /// is a fire-and-forget mpsc send, so `.await` is cheap.
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

    /// Read a batch: cache first, rocksdb fallback.
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

    /// Compute tx hashes from a batch payload. `CachedBatch::tx_hashes`
    /// is `OnceLock`-cached: free on the leader (pre-filled by the
    /// mempool intake path), one-time SHA256 pass on followers.
    pub fn hydrate_tx_hashes(batch: &CachedBatch<Transaction>) -> Vec<Hash<Transaction>> {
        batch.tx_hashes().to_vec()
    }

    /// Serialize `msg` once and stash the returned handlers under the
    /// current round so the network can resolve them at its own pace.
    #[inline]
    pub(crate) fn remember_consensus(&mut self, h: CancelHandler) {
        self.consensus_handlers
            .entry(self.round)
            .or_default()
            .push(h);
    }

    #[inline]
    pub(crate) fn remember_client(&mut self, h: CancelHandler) {
        self.client_handlers
            .entry(self.round)
            .or_default()
            .push(h);
    }

    /// Drop handlers whose messages have been acked (or whose sender
    /// gave up). We must NOT drop unresolved handlers -- libnet-rs
    /// treats a closed `CancelHandler` as "caller cancelled" and
    /// silently discards the payload. See `connection.rs:216-218`.
    pub(crate) fn gc_handlers(&mut self) {
        let gc = |map: &mut HashMap<Round, Vec<CancelHandler>>| {
            map.retain(|_, handlers| {
                handlers.retain_mut(|h| matches!(h.try_recv(), Err(TryRecvError::Empty)));
                !handlers.is_empty()
            });
        };
        gc(&mut self.consensus_handlers);
        gc(&mut self.client_handlers);
    }

    #[inline]
    pub(crate) fn serialize_proto(msg: &ProtocolMsg) -> Bytes {
        Bytes::from(bincode::serialize(msg).expect("ProtocolMsg serialize"))
    }
}
