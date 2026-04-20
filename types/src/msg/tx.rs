use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use crate::{TxTrait, WireReady};

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

impl WireReady for Transaction {
    fn from_bytes(data: &[u8]) -> Self {
        let c: Self =
            bincode::deserialize(data).expect("failed to decode the transaction");
        c.init()
    }

    fn init(self) -> Self {
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize transaction")
    }
}

impl TxTrait for Transaction {
    fn get_hash(&self) -> Hash<Self> {
        self.compute_hash()
    }
}
