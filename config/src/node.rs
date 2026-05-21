use serde::{
    Serialize, 
    Deserialize
};
use types::Replica;
use libcrypto::Algorithm;
use fnv::FnvHashMap as HashMap;
use super::{
    ParseError,
    is_valid_replica
};
use std::fs::File;
use std::io::prelude::*;
use serde_json::from_reader;
use toml::from_str;

/// A short type alias used only in this crate -- clients are a
/// different replica family from consensus replicas, so we distinguish
/// them by using a dedicated id type. Re-exported by libapollo-mempool
/// as `libmempool::ClientId` for the keyed Txpool.
pub type ClientId = u16;

/// Default emit window (seconds) for the server-side throughput sampler.
/// Matches the leto-rs convention so a multi-protocol orchestrator can
/// reuse the same knob across libs.
fn default_bench_emit_window_secs() -> u64 {
    1
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Node {
    /// Peer addresses for node-to-node (consensus) TLS.
    pub net_map: HashMap<Replica, String>,

    /// Peer addresses for node-to-node mempool TCP. Mempool messages
    /// (`MempoolMsg::RequestBatch` / `Batch`) go over plain TCP, which
    /// matches libmempool-rs's `TcpSimpleSender` / `TcpReceiver`. Nodes
    /// look entries up by peer `Replica` id to forward/request batches.
    pub mempool_net_map: HashMap<Replica, String>,

    /// Addresses where each client listens for `ClientMsg` pushes. Nodes
    /// build a `TlsReliableSender<ClientId, ClientMsg>` over this map so
    /// commits can be streamed back to clients.
    pub client_net_map: HashMap<ClientId, String>,

    /// Protocol details
    pub delta: u64,
    pub id: Replica,
    pub num_nodes: usize,
    pub num_faults: usize,
    pub block_size:usize,
    /// Address this node listens on for incoming client transactions
    /// (plain TCP -- libmempool-rs's `Mempool::spawn` binds a
    /// `TcpReceiver<Transaction>` here).
    pub client_port: u16,
    /// Address this node listens on for peer-to-peer mempool sync
    /// traffic (`MempoolMsg::RequestBatch` / `MempoolMsg::Batch`). A
    /// second plain-TCP port; separate from `client_port` so the
    /// client endpoint is never hit by peer batch requests.
    pub mempool_port: u16,
    pub payload: usize,

    /// Bench-only: window (in seconds) over which the server-side
    /// throughput sampler aggregates committed transactions before
    /// emitting a `DP[Throughput]` line. Only the node whose id equals
    /// `bench_metrics_node` actually emits, so the orchestrator sees a
    /// single stream per run. Both default to a sensible value when the
    /// field is missing from older config files (5s, node 0).
    #[serde(default = "default_bench_emit_window_secs")]
    pub bench_emit_window_secs: u64,
    #[serde(default)]
    pub bench_metrics_node: Replica,

    /// Crypto primitives
    pub crypto_alg: Algorithm,
    pub pk_map: HashMap<Replica, Vec<u8>>,
    pub secret_key_bytes: Vec<u8>,

    /// TLS certificate paths (PEM). Absolute paths written by
    /// `genconfig`. The chain holds `[leaf cert, root CA]` so the same
    /// file works for both the server identity (leaf) and the trust
    /// store (root CA).
    pub my_cert_path: String,
    pub my_cert_key_path: String,
    pub root_cert_path: String,
}

impl Node {
    pub fn validate(&self) -> Result<(), ParseError> {
        if self.net_map.len() != self.num_nodes {
            return Err(ParseError::InvalidMapLen(self.num_nodes, self.net_map.len()));
        }
        if 2*self.num_faults >= self.num_nodes {
            return Err(ParseError::IncorrectFaults(self.num_faults, self.num_nodes));
        }
        for repl in &self.net_map {
            if !is_valid_replica(*repl.0, self.num_nodes) {
                return Err(ParseError::InvalidMapEntry(*repl.0));
            }
        }
        match self.crypto_alg {
            Algorithm::ED25519 | Algorithm::SECP256K1 => {
                // Keys are bincode-serialized libcrypto::<alg>::Keypair / PublicKey
                // blobs -- their exact byte length depends on serde, so we only
                // sanity-check presence here; decoding errors surface at node
                // startup when the key is actually parsed.
                for (id, pk_bytes) in &self.pk_map {
                    if !is_valid_replica(*id, self.num_nodes) {
                        return Err(ParseError::InvalidMapEntry(*id));
                    }
                    if pk_bytes.is_empty() {
                        return Err(ParseError::InvalidPkSize(pk_bytes.len()));
                    }
                }
                if self.secret_key_bytes.is_empty() {
                    return Err(ParseError::InvalidSkSize(self.secret_key_bytes.len()));
                }
            }
            Algorithm::RSA => {
                return Err(ParseError::Unimplemented("RSA"));
            }
        }
        Ok(())
    }

    pub fn new() -> Node {
        Node{
            block_size: 0,
            client_port: 0,
            mempool_port: 0,
            mempool_net_map: HashMap::default(),
            client_net_map: HashMap::default(),
            crypto_alg: Algorithm::ED25519,
            delta: 50,
            id: 0,
            net_map: HashMap::default(),
            num_faults: 0,
            num_nodes: 0,
            pk_map: HashMap::default(),
            secret_key_bytes: Vec::new(),
            payload: 0,
            my_cert_path: String::new(),
            root_cert_path: String::new(),
            my_cert_key_path: String::new(),
            bench_emit_window_secs: default_bench_emit_window_secs(),
            bench_metrics_node: 0,
        }
    }

    pub fn from_json(filename:String) -> Node {
        let f = File::open(filename)
            .unwrap();
        let c: Node = from_reader(f)
            .unwrap();
        return c;
    }

    pub fn from_toml(filename:String) -> Node {
        let mut buf = String::new();
        let mut f = File::open(filename)
            .unwrap();
        f.read_to_string(&mut buf)
            .unwrap();
        let c:Node = from_str(&buf)
            .unwrap();
        return c;
    }

    pub fn from_yaml(filename:String) -> Node {
        let f = File::open(filename)
            .unwrap();
        let c:Node = serde_yaml::from_reader(f)
            .unwrap();
        return c;
    }

    pub fn from_bin(filename:String) -> Node {
        let mut buf = Vec::new();
        let mut f = File::open(filename)
            .unwrap();
        f.read_to_end(&mut buf)
            .unwrap();
        let bytes:&[u8] = &buf;
        let c:Node = bincode::deserialize(bytes)
            .unwrap();
        return c;
    }

    pub fn update_config(&mut self, ips: Vec<String>) {
        let mut idx = 0;
        for ip in ips {
            // For self ip, put 0.0.0.0 with the same port
            if idx == self.id {
                let port:u16 = ip.split(":")
                    .last()
                    .expect("invalid ip found; unable to split at :")
                    .parse()
                    .expect("failed to parse the port after :");
                self.net_map.insert(idx, format!("0.0.0.0:{}", port));
                idx += 1;
                continue;
            }
            // Put others ips in the config
            self.net_map.insert(idx, ip);
            idx += 1;
        }
        log::info!("Talking to servers: {:?}", self.net_map);
    }

    pub fn my_ip(&self) -> String {
        // Small string, so it is okay to clone
        self.net_map.get(&self.id)
            .expect("Failed to obtain IP for self. Incorrect config file.")
            .clone()
    }

    /// Returns the address at which a server should listen to incoming client
    /// connections
    pub fn client_ip(&self) -> String {
        format!("0.0.0.0:{}", self.client_port)
    }

    /// Returns the bind address for this node's peer-to-peer mempool
    /// socket (`MempoolMsg::RequestBatch` / `Batch` traffic).
    pub fn mempool_ip(&self) -> String {
        format!("0.0.0.0:{}", self.mempool_port)
    }
}