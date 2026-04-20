use bytes::Bytes;
use config::{ClientId, Node};
use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use libcrypto::{ed25519, secp256k1, Keypair, PublicKey};
use libmempool::{Batch, BatchHash, ConsensusMempoolMsg};
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

    /// Batch store shared with the per-node Mempool. Leaders read
    /// batches out of it when proposing; followers write incoming
    /// batches in when handling proposals.
    pub batch_store: RocksStore,

    /// Batches the mempool has announced as ready; the leader pops
    /// from the front of this on the tick after `round_leader() == myid`.
    pub pending_batches: VecDeque<BatchHash<Transaction>>,

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
    pub prop_buf: VecDeque<(Replica, Propose, Block, Batch<Transaction>)>,
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

    /// Persist an incoming batch into the local batch store. Called
    /// from the proposal / response paths before voting / delivering.
    pub async fn persist_batch(
        &mut self,
        batch_hash: BatchHash<Transaction>,
        batch: &Batch<Transaction>,
    ) {
        let key = batch_hash.as_ref().to_vec();
        let value = bincode::serialize(batch).expect("Batch serialize");
        self.batch_store.write(key, value).await;
    }

    /// Read a batch from storage by hash.
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

    /// Compute tx hashes from a batch payload. Leaders feed this into
    /// `ClientMsg::NewBlock` at propose time (special-client mode);
    /// followers feed it at commit time.
    pub fn hydrate_tx_hashes(batch: &Batch<Transaction>) -> Vec<Hash<Transaction>> {
        batch
            .payload
            .iter()
            .map(|tx| Hash::<Transaction>::ser_and_hash(tx))
            .collect()
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
