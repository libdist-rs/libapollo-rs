use libcrypto::{hash::Hash, Keypair, PublicKey};
use net_common::Message;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::sync::Arc;

use super::super::Block as OldBlock;
use super::{Replica, Height, Transaction, Vote};
use crate::GENESIS_BLOCK as OldGenesis;
use crate::{BlockTrait, KeypairSign};

/// Artemis wraps the shared block with a leader signature. The block's
/// *identity* hash is still the hash of the underlying content (`self.blk`);
/// the signature authenticates the content but isn't part of the identity.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Block {
    pub blk: OldBlock,
    pub sig: Vote,
}

pub const GENESIS_BLOCK: Block = Block {
    blk: OldGenesis,
    sig: Vote {
        auth: vec![],
        origin: 0,
    },
};

impl Block {
    pub fn with_tx(txs: Vec<Arc<Transaction>>) -> Self {
        Block {
            blk: OldBlock::with_tx(txs),
            sig: Vote {
                auth: vec![],
                origin: 0,
            },
        }
    }

    /// Check the leader's signature over the block's content hash.
    pub fn check_sig(&self, pk: &PublicKey) -> bool {
        pk.verify(self.blk.hash.as_ref(), &self.sig.auth)
    }

    /// Sign the block. Caller must have already called `Block::init()` so
    /// that `self.blk.hash` is populated.
    pub fn sign(&mut self, sk: &Keypair) {
        let auth = sk
            .sign(self.blk.hash.as_ref())
            .expect("Failed to sign the block");
        self.sig.auth = auth;
    }

    /// Builder finalizer: cascades to `OldBlock::init` to recompute the
    /// cached content hash after the caller mutated header/body fields.
    pub fn init(self) -> Self {
        Block {
            blk: self.blk.init(),
            sig: self.sig,
        }
    }
}

impl BlockTrait for Block {
    fn get_hash(&self) -> Hash<Self> {
        // The content hash is Hash<OldBlock>; re-tag its bytes as Hash<Self>
        // so Storage / HashMaps keyed by `Hash<artemis::Block>` remain
        // type-consistent without a new digest.
        Hash::<Self>::try_from(self.blk.hash.as_ref())
            .expect("hash is exactly 32 bytes")
    }

    fn get_height(&self) -> Height {
        self.blk.get_height()
    }

    fn get_author(&self) -> Replica {
        self.blk.get_author()
    }
}

impl Message for Block {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        // Inner `OldBlock`'s Deserialize fills its hash; no post-process.
        bincode::deserialize(bytes)
    }
}
