use bytes::Bytes;
use config::{ClientId, Node};
use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use libcrypto::{ed25519, secp256k1, Keypair, PublicKey};
use libmempool::{Batch, BatchHash, ConsensusMempoolMsg};
use libstorage::rocksdb::Storage as RocksStore;
use linked_hash_map::LinkedHashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::Arc;
use tls_reliable_sender::{CancelHandler, TlsReliableSender};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::error::TryRecvError;
use types::artemis::{
    Block, ClientMsg, GENESIS_BLOCK, ProtocolMsg, Replica, Round, Storage, Transaction, UCRVote,
    View,
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
    /// Whether or not our client supports UCR or not.
    /// If yes, UCR is enabled, and we send the block on proposing.
    /// If no, UCR is disabled, and we notify the client on committing.
    is_client_apollo_enabled: bool,

    /// Consensus network (node-to-node).
    pub consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    /// Client network (node-to-every-client).
    pub client_net: TlsReliableSender<ClientId, ClientMsg>,
    /// All peer ids except my own.
    pub broadcast_peers: Vec<Replica>,
    /// All registered clients.
    pub all_clients: Vec<ClientId>,
    /// Cancel handlers, keyed by the round observed at send time.
    pub consensus_handlers: HashMap<Round, Vec<CancelHandler>>,
    pub client_handlers: HashMap<Round, Vec<CancelHandler>>,

    /// Batch store shared with the per-node Mempool.
    pub batch_store: RocksStore,
    /// Batches the mempool has announced as ready, waiting for the
    /// current view leader to propose.
    pub pending_batches: VecDeque<BatchHash<Transaction>>,
    /// Control channel to the mempool.
    pub tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Round, Transaction>>,

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

    /// Per-block payload size used when pushing committed blocks out
    /// to clients.
    pub payload: usize,

    // Stuff related to message reordering
    pub vote_waiting: HashMap<Hash<Block>, UCRVote>,
    pub vote_ready: HashMap<Round, UCRVote>,
    pub block_processing_waiting: VecDeque<(Block, Batch<Transaction>)>,
    pub response_waiting: VecDeque<(Replica, Block, Batch<Transaction>)>,
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
        client_net: TlsReliableSender<ClientId, ClientMsg>,
        batch_store: RocksStore,
        tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Round, Transaction>>,
        apollo_enabled: bool,
    ) -> Self {
        let genesis_arc = Arc::new(GENESIS_BLOCK);
        let broadcast_peers: Vec<Replica> = (0..config.num_nodes as Replica)
            .filter(|r| *r != config.id)
            .collect();
        let all_clients: Vec<ClientId> = config.client_net_map.keys().copied().collect();
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
            client_net,
            broadcast_peers,
            all_clients,
            consensus_handlers: HashMap::default(),
            client_handlers: HashMap::default(),
            batch_store,
            pending_batches: VecDeque::new(),
            tx_consensus_to_mem,
            storage: Storage::new(),
            view_leader: 0,
            round_leader: (config.num_faults - 1) as Replica,
            last_f_leaders: LinkedHashMap::with_capacity(config.num_nodes),
            eligible_leaders: Vec::with_capacity(config.num_nodes),
            view: 0,
            round: 1,
            last_seen_block: genesis_arc.clone(),
            last_voted_block: genesis_arc,
            is_client_apollo_enabled: apollo_enabled,
            req_ctr: 0,
            payload: config.payload * config.block_size,
            vote_waiting: HashMap::default(),
            vote_ready: HashMap::default(),
            vote_chain: HashMap::default(),
            block_parent_waiting: HashMap::default(),
            undelivered_blocks: HashMap::default(),
            block_processing_waiting: VecDeque::new(),
            response_waiting: VecDeque::new(),
            other_buf: VecDeque::new(),
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
        // Initialize storage with the genesis block
        c.storage.add_delivered_block(c.last_seen_block.clone());
        // Initialize the leaders
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
    pub const fn is_client_apollo_enabled(&self) -> bool {
        self.is_client_apollo_enabled
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

    #[inline]
    pub(crate) fn remember_client(&mut self, h: CancelHandler) {
        self.client_handlers.entry(self.round).or_default().push(h);
    }

    /// See the matching comment on `synchs` / `apollo` `gc_handlers`
    /// -- retain handlers that are still empty (message in flight),
    /// drop those that have resolved. Called on `update_round`.
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

    /// Persist an incoming batch into the local batch store.
    pub async fn persist_batch(
        &mut self,
        batch_hash: BatchHash<Transaction>,
        batch: &Batch<Transaction>,
    ) {
        let key = batch_hash.as_ref().to_vec();
        let value = bincode::serialize(batch).expect("Batch serialize");
        self.batch_store.write(key, value).await;
    }

    /// Read a batch from the store by hash.
    pub async fn read_batch(
        &mut self,
        batch_hash: &BatchHash<Transaction>,
    ) -> Option<Batch<Transaction>> {
        let key = batch_hash.as_ref().to_vec();
        match self.batch_store.read(key).await {
            Ok(Some(bytes)) => bincode::deserialize(&bytes).ok(),
            _ => None,
        }
    }

    /// Hydrate per-tx hashes from a batch. Used to fill in the
    /// hashes field of `ClientMsg::NewBlock` so the client's latency
    /// tracker can match commits to its outstanding submissions.
    pub fn hydrate_tx_hashes(batch: &Batch<Transaction>) -> Vec<Hash<Transaction>> {
        batch
            .payload
            .iter()
            .map(|tx| Hash::<Transaction>::ser_and_hash(tx))
            .collect()
    }
}
