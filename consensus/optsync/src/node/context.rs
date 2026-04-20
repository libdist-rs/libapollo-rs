use bytes::Bytes;
use config::{ClientId, Node};
use fnv::FnvHashMap as HashMap;
use libcrypto::{ed25519, hash::Hash, secp256k1, Keypair, PublicKey};
use std::{sync::Arc, time::Duration};
use tls_reliable_sender::{CancelHandler, TlsReliableSender};
use tokio::sync::oneshot::error::TryRecvError;
use tokio_util::time::DelayQueue;
use types::optsync::{
    Block, Certificate, ClientMsg, GENESIS_BLOCK, Height, ProtocolMsg, Propose, Replica, Storage, View,
};

pub struct Context {
    /// Consensus network (node-to-node).
    pub consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    /// Client network (node-to-every-client).
    pub client_net: TlsReliableSender<ClientId, ClientMsg>,
    /// All peer ids except my own (cached broadcast list).
    pub broadcast_peers: Vec<Replica>,
    /// All client ids (cached broadcast list).
    pub all_clients: Vec<ClientId>,
    /// Retained cancel handlers, keyed by the `height` observed at
    /// send time. See `gc_handlers`.
    pub consensus_handlers: HashMap<Height, Vec<CancelHandler>>,
    pub client_handlers: HashMap<Height, Vec<CancelHandler>>,

    /// Data context
    pub num_nodes: usize,
    pub myid: Replica,
    pub num_faults: usize,
    pub payload: usize,
    pub d2: Duration,

    /// PKI
    pub my_secret_key: Keypair,
    pub pub_key_map: HashMap<Replica, PublicKey>,

    /// State context
    pub storage: Storage,
    pub resp_cert: HashMap<Hash<Block>, Arc<Certificate>>, // Responsive certificates
    pub cert_map: HashMap<Hash<Block>, Certificate>,       // All certified blocks
    pub height: Height,
    pub last_leader: Replica,
    pub last_seen_block: Arc<Block>,
    pub last_seen_cert: Certificate,
    pub last_committed_block_ht: Height,
    pub vote_map: HashMap<Hash<Block>, Certificate>,
    pub view: View,
    pub commit_queue: DelayQueue<Arc<Propose>>,
}

const EXTRA_SPACE: usize = 10;

impl Context {
    pub fn new(
        config: &Node,
        consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
        client_net: TlsReliableSender<ClientId, ClientMsg>,
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
            d2: std::time::Duration::from_millis(2 * config.delta),
            num_faults: config.num_faults,
            storage: Storage::new(EXTRA_SPACE * config.block_size),
            height: 0,
            last_leader: 0,
            last_seen_block: genesis_arc.clone(),
            last_committed_block_ht: 0,
            resp_cert: HashMap::default(),
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
        (self.view % self.num_nodes) as Replica
    }

    /// Serialize once and broadcast `msg` to every peer but myself.
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

    /// Retain only handlers whose messages are still in flight.
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
