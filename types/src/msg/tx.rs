use libcrypto::hash::Hash;
use net_common::Message;
use serde::{Deserialize, Serialize};

use crate::TxTrait;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    pub data: Vec<u8>,
    pub request: Vec<u8>,
}

impl Transaction {
    pub fn compute_hash(&self) -> Hash<Self> {
        Hash::<Self>::ser_and_hash(self)
    }

    pub fn new_dummy_tx(i: u64, payload: usize) -> Self {
        log::trace!("Creating a dummy transaction with payload {}", payload);
        let t = Transaction {
            data: i.to_be_bytes().to_vec(),
            request: vec![1; payload],
        };
        log::trace!("Created dummy transaction {:?}", t);
        t
    }
}

impl Message for Transaction {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}

impl TxTrait for Transaction {
    fn get_hash(&self) -> Hash<Self> {
        self.compute_hash()
    }
}
