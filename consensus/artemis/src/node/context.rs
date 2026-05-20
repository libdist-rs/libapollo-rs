use bytes::Bytes;
use config::Node;
use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use libcrypto::{ed25519, secp256k1, Keypair, PublicKey};
use libmempool::{BatchCache, BatchHash, BatcherConsensusMsg, CachedBatch};
use libstorage::rocksdb::Storage as RocksStore;
use linked_hash_map::LinkedHashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::Arc;
use tls_reliable_sender::{CancelHandler, TlsReliableSender};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::error::TryRecvError;
use types::artemis::{
    Block, GENESIS_BLOCK, ProtocolMsg, Replica, Round, Storage, Transaction, UCRVote, View,
};

/// Config context
pub struct Context {
    /// The number of nodes in the system
    num_nodes: usize,
    /// The number of faults
    num_faults: usize,
    /// myid in the protocol
    myid: Replica,
    /// Map of node IDs and public keys
    pub pub_key_map: HashMap<Replica, PublicKey>,
    /// My Secret Key
    pub my_secret_key: Arc<Keypair>,

    /// Consensus network (node-to-node).
    pub consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    /// All peer ids except my own.
    pub broadcast_peers: Vec<Replica>,
    /// Cancel handlers, keyed by the round observed at send time.
    pub consensus_handlers: HashMap<Round, Vec<CancelHandler>>,

    /// Rocksdb durable fallback; writes fire on a detached task.
    pub batch_store: RocksStore,
    /// In-memory batch cache shared with the libapollo-mempool pipeline.
    pub batch_cache: Arc<BatchCache<Transaction>>,
    /// Batches the keyed mempool has announced as ready, waiting for
    /// the current view leader to propose.
    pub pending_batches: VecDeque<(BatchHash<Transaction>, Arc<CachedBatch<Transaction>>)>,

    /// Outgoing BCM channel (NewRound / Proposed / Committed / Rollback)
    /// to the keyed batcher.
    pub tx_consensus_to_batcher: UnboundedSender<BatcherConsensusMsg<Transaction>>,
    /// Committed-batch sink for the confirmation router.
    pub tx_committed_to_router: UnboundedSender<Arc<CachedBatch<Transaction>>>,

    /// Storage context. Post-libmempool this is a block index only.
    pub storage: Storage,
    /// The vote chain: Map of block hash to its proposal
    pub vote_chain: HashMap<Round, Arc<UCRVote>>,

    /// The current round leader
    pub round_leader: Replica,
    /// The last f leaders
    last_f_leaders: LinkedHashMap<Replica, ()>,
    /// Eligible leaders
    eligible_leaders: Vec<Replica>,
    /// The current view leader
    pub view_leader: Replica,
    /// The current view
    pub view: View,
    /// The current round
    round: Round,
    /// The last observed block
    pub last_seen_block: Arc<Block>,
    /// The last block for which we have seen vote messages for
    pub last_voted_block: Arc<Block>,
    /// A counter to keep track of all the requests
    pub req_ctr: u64,

    /// Per-block payload size used when assembling client-side
    /// notifications (legacy; the new client only consumes tx-hash
    /// confirmations).
    pub payload: usize,

    /// Block size, carried through from config.
    pub block_size: usize,

    /// Bench-only throughput sampler state.
    pub bench_committed_tx_count: u64,
    pub bench_emit_window_secs: u64,
    pub bench_metrics_node: Replica,

    // Stuff related to message reordering
    pub vote_waiting: HashMap<Hash<Block>, UCRVote>,
    pub vote_ready: HashMap<Round, UCRVote>,
    pub block_processing_waiting: VecDeque<(Block, Arc<CachedBatch<Transaction>>)>,
    pub response_waiting: VecDeque<(Replica, Block, Arc<CachedBatch<Transaction>>)>,

    /// Low-overhead reactor metrics; dumped on SIGINT.
    pub metrics: Arc<super::metrics::Metrics>,
    pub other_buf: VecDeque<ProtocolMsg>,

    /// Block waiting (hash1, hash2)
    pub block_parent_waiting: HashMap<Hash<Block>, Hash<Block>>,
    /// Undelivered blocks (h, b)
    pub undelivered_blocks: HashMap<Hash<Block>, Block>,
}

impl Context {
    pub fn new(
        config: &Node,
        consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
        batch_store: RocksStore,
        batch_cache: Arc<BatchCache<Transaction>>,
        tx_consensus_to_batcher: UnboundedSender<BatcherConsensusMsg<Transaction>>,
        tx_committed_to_router: UnboundedSender<Arc<CachedBatch<Transaction>>>,
    ) -> Self {
        let genesis_arc = Arc::new(GENESIS_BLOCK);
        let broadcast_peers: Vec<Replica> = (0..config.num_nodes as Replica)
            .filter(|r| *r != config.id)
            .collect();
        let mut c = Context {
            num_nodes: config.num_nodes,
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
            broadcast_peers,
            consensus_handlers: HashMap::default(),
            batch_store,
            batch_cache,
            pending_batches: VecDeque::new(),
            tx_consensus_to_batcher,
            tx_committed_to_router,
            storage: Storage::new(),
            view_leader: 0,
            round_leader: (config.num_faults - 1) as Replica,
            last_f_leaders: LinkedHashMap::with_capacity(config.num_nodes),
            eligible_leaders: Vec::with_capacity(config.num_nodes),
            view: 0,
            round: 1,
            last_seen_block: genesis_arc.clone(),
            last_voted_block: genesis_arc,
            req_ctr: 0,
            payload: config.payload * config.block_size,
            block_size: config.block_size,
            bench_committed_tx_count: 0,
            bench_emit_window_secs: config.bench_emit_window_secs.max(1),
            bench_metrics_node: config.bench_metrics_node,
            vote_waiting: HashMap::default(),
            vote_ready: HashMap::default(),
            vote_chain: HashMap::default(),
            block_parent_waiting: HashMap::default(),
            undelivered_blocks: HashMap::default(),
            block_processing_waiting: VecDeque::new(),
            response_waiting: VecDeque::new(),
            other_buf: VecDeque::new(),
            metrics: super::metrics::Metrics::new(),
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
        c.storage.add_delivered_block(c.last_seen_block.clone());
        for i in 0..config.num_faults {
            c.last_f_leaders.insert(i as Replica, ());
        }
        for i in config.num_faults..config.num_nodes {
            c.eligible_leaders.push(i as Replica);
        }
        log::info!("Using last f leaders: {:?}", c.last_f_leaders);
        log::info!("Using eligible leaders: {:?}", c.eligible_leaders);
        c
    }

    /// Goes to the next round
    pub(crate) fn update_round(&mut self) {
        self.metrics.record_round_advance();
        let (new_leader, idx) = self.compute_next_round_leader();
        self.round_leader = new_leader;
        self.round += 1;
        let (eligible_again, _) = self.last_f_leaders.pop_front().unwrap();
        self.last_f_leaders.insert(self.round_leader, ());
        self.eligible_leaders[idx] = eligible_again;
        self.gc_handlers();
    }

    pub(crate) fn next_round_leader(&self) -> Replica {
        let (leader, _) = self.compute_next_round_leader();
        leader
    }

    fn compute_next_round_leader(&self) -> (Replica, usize) {
        let data = (self.round + 1).to_be_bytes();
        let h = libcrypto::hash::Hash::<Block>::do_hash(&data);
        let idx = usize::from_be_bytes(h.as_ref()[24..].try_into().unwrap())
            % self.eligible_leaders.len();
        (self.eligible_leaders[idx], idx)
    }

    /// Notify the keyed batcher that this node has advanced past
    /// `last_seen_block` so its `current_round` is up to date. The
    /// view leader is constant in the current artemis design; if a
    /// view change is added later, this is the place to surface a new
    /// `leader` to the batcher.
    pub(crate) fn announce_height_to_batcher(&self) {
        let next_height = self.last_seen_block.blk.header.height + 1;
        let _ = self
            .tx_consensus_to_batcher
            .send(BatcherConsensusMsg::NewRound {
                leader: self.view_leader,
                round: next_height,
            });
    }

    #[inline]
    pub const fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    #[inline]
    pub const fn num_faults(&self) -> usize {
        self.num_faults
    }

    #[inline]
    pub const fn myid(&self) -> Replica {
        self.myid
    }

    #[inline]
    pub const fn round(&self) -> Round {
        self.round
    }

    #[inline]
    pub(crate) fn remember_consensus(&mut self, h: CancelHandler) {
        self.consensus_handlers
            .entry(self.round)
            .or_default()
            .push(h);
    }

    pub(crate) fn gc_handlers(&mut self) {
        let gc = |map: &mut HashMap<Round, Vec<CancelHandler>>| {
            map.retain(|_, handlers| {
                handlers.retain_mut(|h| matches!(h.try_recv(), Err(TryRecvError::Empty)));
                !handlers.is_empty()
            });
        };
        gc(&mut self.consensus_handlers);
    }

    #[inline]
    pub(crate) fn serialize_proto(msg: &ProtocolMsg) -> Bytes {
        Bytes::from(bincode::serialize(msg).expect("ProtocolMsg serialize"))
    }

    /// Install an incoming batch in the cache; enqueue rocksdb write.
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
            Ok(Some(bytes)) => match bincode::deserialize::<CachedBatch<Transaction>>(&bytes) {
                Ok(b) => {
                    let arc = Arc::new(b);
                    self.batch_cache.insert(batch_hash.clone(), Arc::clone(&arc));
                    Some(arc)
                }
                Err(_) => None,
            },
            _ => None,
        }
    }
}
