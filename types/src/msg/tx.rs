use libcrypto::hash::Hash;
use libmempool::{ClientId, MempoolTx};
use net_common::Message;
use serde::{Deserialize, Serialize};

use crate::TxTrait;

/// A client-submitted transaction.
///
/// `client_id` + `nonce` form the dedup key used by the server-side
/// `Txpool` to drive its Mineable/InFlight/Committed state machine
/// (libapollo-mempool's nonce-keyed pool, mirroring leto-rs).
/// Pre-existing single-client benchmarks set `client_id = 0` and
/// `nonce = i` via `new_dummy_tx`, preserving the old monotonic-counter
/// semantics for synchs/optsync.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    pub client_id: ClientId,
    pub nonce: u64,
    pub data: Vec<u8>,
    pub request: Vec<u8>,
}

impl Transaction {
    pub fn compute_hash(&self) -> Hash<Self> {
        Hash::<Self>::ser_and_hash(self)
    }

    /// Single-client benchmark constructor (legacy synchs/optsync /
    /// apollo-single-client). Sets `client_id = 0`, `nonce = i`.
    pub fn new_dummy_tx(i: u64, payload: usize) -> Self {
        Self::new_dummy_tx_keyed(0, i, payload)
    }

    /// Multi-client constructor. Each client should pick a distinct
    /// `client_id` and feed a per-client monotonically-increasing
    /// `nonce`. The server's `Txpool::add_tx` rejects nonces ≤ that
    /// client's `high_committed_nonce`, providing replay protection.
    pub fn new_dummy_tx_keyed(client_id: ClientId, nonce: u64, payload: usize) -> Self {
        Transaction {
            client_id,
            nonce,
            data: nonce.to_be_bytes().to_vec(),
            request: vec![1; payload],
        }
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

impl MempoolTx for Transaction {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn nonce(&self) -> u64 {
        self.nonce
    }
}
