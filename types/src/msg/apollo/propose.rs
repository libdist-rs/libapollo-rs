use libcrypto::{hash::Hash, Keypair, PublicKey};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::*;
use crate::KeypairSign;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Propose {
    pub sig: Vote,
    pub round: Round,
    pub block_hash: Hash<Block>,

    #[serde(skip)]
    pub block: Option<Arc<Block>>,
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
            block: None,
        }
    }

    /// Signs the block's hash with the leader's secret key.
    pub fn sign_block(&mut self, b: &Block, sk: &Keypair) {
        let hash = Hash::<Block>::ser_and_hash(b);
        self.sig.auth = sk
            .sign(hash.as_ref())
            .expect("Failed to sign a block");
    }

    /// Verifies the proposal's signature over the block hash.
    pub fn check_sig(&self, b: &Block, pk: &PublicKey) -> bool {
        let hash = Hash::<Block>::ser_and_hash(b);
        pk.verify(hash.as_ref(), &self.sig.auth)
    }
}
