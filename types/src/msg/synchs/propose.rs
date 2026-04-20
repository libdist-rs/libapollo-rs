use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, Certificate, View};

/// Sync HotStuff proposal metadata. The block itself is carried alongside
/// `Propose` in the enum variant (`ProtocolMsg::NewProposal`,
/// `ClientMsg::NewBlock`) or fetched from storage when reconstructing
/// history; it is not embedded here.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Propose {
    /// Signature by the author over `block_hash`.
    pub proof: Vec<u8>,
    /// Certificate for the parent of this block.
    pub cert: Certificate,
    /// View number for this certificate.
    pub view: View,
    /// Hash of the block being proposed.
    pub block_hash: Hash<Block>,
}

impl Propose {
    pub fn new() -> Self {
        Self {
            proof: Vec::new(),
            cert: Certificate::empty_cert(),
            view: 0,
            block_hash: Hash::<Block>::EMPTY_HASH,
        }
    }
}
