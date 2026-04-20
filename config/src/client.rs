use serde::{
    Serialize,
    Deserialize
};
use types::Replica;
use libcrypto::Algorithm;
use fnv::FnvHashMap as HashMap;
use super::{
    ClientId,
    ParseError,
    is_valid_replica
};
use std::fs::File;
use std::io::prelude::*;
use serde_json::from_reader;
use toml::from_str;

#[derive(Debug, Serialize, Deserialize, Clone,PartialEq)]
pub struct Client {
    pub net_map:HashMap<Replica, String>,
    pub crypto_alg:Algorithm,
    pub server_pk:HashMap<Replica, Vec<u8>>,

    pub num_nodes: usize,
    pub num_faults: usize,
    pub block_size:usize,
    pub payload:usize,

    /// This client's identity and listen address. Nodes look up the
    /// address in their `client_net_map[my_id]` when pushing `ClientMsg`,
    /// so it must match on both sides of the config.
    pub my_id: ClientId,
    pub my_listen_addr: String,

    /// TLS cert paths. Chain holds `[leaf, root CA]`, same convention as
    /// `Node::my_cert_path`.
    pub my_cert_path: String,
    pub my_cert_key_path: String,
    pub root_cert_path: String,
}

impl Client {
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
                for (id, pk_bytes) in &self.server_pk {
                    if !is_valid_replica(*id, self.num_nodes) {
                        return Err(ParseError::InvalidMapEntry(*id));
                    }
                    if pk_bytes.is_empty() {
                        return Err(ParseError::InvalidPkSize(pk_bytes.len()));
                    }
                }
            }
            Algorithm::RSA => {
                return Err(ParseError::Unimplemented("RSA"));
            }
        }
        Ok(())
    }

    pub fn new() -> Client {
        Client {
            net_map: HashMap::default(),
            block_size: 0,
            crypto_alg: Algorithm::ED25519,
            num_faults: 0,
            num_nodes:0,
            server_pk: HashMap::default(),
            payload:0,
            my_id: 0,
            my_listen_addr: String::new(),
            my_cert_path: String::new(),
            my_cert_key_path: String::new(),
            root_cert_path: String::new(),
        }
    }

    pub fn from_json(filename:String) -> Client {
        let f = File::open(filename)
            .unwrap();
        let c: Client = from_reader(f)
            .unwrap();
        return c;
    }

    pub fn from_toml(filename:String) -> Client {
        let mut buf = String::new();
        let mut f = File::open(filename)
            .unwrap();
        f.read_to_string(&mut buf)
            .unwrap();
        let c:Client = from_str(&buf)
            .unwrap();
        return c;
    }

    pub fn from_yaml(filename:String) -> Client {
        let f = File::open(filename)
            .unwrap();
        let c:Client = serde_yaml::from_reader(f)
            .unwrap();
        return c;
    }

    pub fn from_bin(filename:String) -> Client {
        let mut buf = Vec::new();
        let mut f = File::open(filename)
            .unwrap();
        f.read_to_end(&mut buf)
            .unwrap();
        let bytes:&[u8] = &buf;
        let c:Client = bincode::deserialize(bytes)
            .unwrap();
        return c;
    }

    pub fn update_config(&mut self, ips: Vec<String>) {
        let mut idx = 0;
        for ip in ips {
            // Put others ips in the config
            self.net_map.insert(idx, ip);
            idx += 1;
        }
        log::info!("Talking to servers: {:?}", self.net_map);
    }
}