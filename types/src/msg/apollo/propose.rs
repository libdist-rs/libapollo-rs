use libcrypto::{hash::Hash, Keypair, PublicKey};
use serde::{Deserialize, Serialize};

use super::*;
use crate::KeypairSign;

/// Apollo proposal: `(sig, round, block_hash)`. The signature is over
/// `block_hash`; the block itself travels alongside the `Propose` as a
/// separate field in `ProtocolMsg::NewProposal` / `Response` and
/// `ClientMsg::NewBlock` / `Response`, or is fetched from storage on
/// `Relay`. Keeping the block out of this struct avoids an allocation
/// per received proposal and lets `check_sig` use the already-cached
/// block hash instead of rehashing the block body.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Propose {
    pub sig: Vote,
    pub round: Round,
    pub block_hash: Hash<Block>,
}

impl Propose {
    pub fn new(block_hash: Hash<Block>) -> Self {
        Propose {
            round: 0,
            sig: Vote {
                auth: Vec::new(),
                origin: 0,
            },
            block_hash,
        }
    }

    /// Signs the committed block hash with the leader's secret key.
    /// Callers must have populated `block_hash` from the actual block.
    pub fn sign(&mut self, sk: &Keypair) {
        self.sig.auth = sk
            .sign(self.block_hash.as_ref())
            .expect("Failed to sign a block");
    }

    /// Verifies the proposal's signature over its `block_hash`. The caller
    /// is separately responsible for checking `block.hash == block_hash`.
    pub fn check_sig(&self, pk: &PublicKey) -> bool {
        pk.verify(self.block_hash.as_ref(), &self.sig.auth)
    }
}
