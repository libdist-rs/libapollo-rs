use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{Block, Certificate, View};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Propose {
    /// Signature by the author.
    pub proof: Vec<u8>,
    /// Certificate for the parent of this block.
    pub cert: Certificate,
    /// View number for this certificate.
    pub view: View,
    /// Hash of the block being proposed.
    pub block_hash: Hash<Block>,

    #[serde(skip_serializing, skip_deserializing, default = "empty_block_hash_option")]
    pub block: Option<Arc<Block>>,
}

fn empty_block_hash_option() -> Option<Arc<Block>> {
    None
}

impl Propose {
    pub fn new() -> Self {
        Self {
            proof: Vec::new(),
            cert: Certificate::empty_cert(),
            view: 0,
            block: None,
            block_hash: Hash::<Block>::EMPTY_HASH,
        }
    }
}
