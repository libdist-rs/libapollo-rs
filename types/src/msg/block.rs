use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{Certificate, Transaction};
use crate::{protocol::{Height, Replica}, BlockTrait, WireReady};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    pub header: Header,
    pub body: Body,

    /// Cache -- populated by `init()` after deserialization, not wire-transmitted.
    /// Explicit `default` because the auto-derived `Default` on libcrypto's
    /// `Hash<T>` bounds on `T: Default`, which Block cannot satisfy (cyclic).
    #[serde(skip, default = "empty_block_hash")]
    pub hash: Hash<Block>,
}

fn empty_block_hash() -> Hash<Block> {
    Hash::<Block>::EMPTY_HASH
}

impl Block {
    pub fn with_tx(txs: Vec<Arc<Transaction>>) -> Self {
        Block {
            header: Header::new(),
            body: Body::new(txs),
            hash: Hash::<Block>::EMPTY_HASH,
        }
    }

    pub fn compute_hash(&self) -> Hash<Self> {
        Hash::<Self>::ser_and_hash(self)
    }
}

pub const GENESIS_BLOCK: Block = Block {
    header: Header {
        prev: Hash::<Block>::EMPTY_HASH,
        extra: Vec::new(),
        author: 0,
        height: 0,
        blame_certificates: Vec::new(),
    },
    body: Body {
        tx_hashes: Vec::new(),
    },
    hash: Hash::<Block>::EMPTY_HASH,
};

impl WireReady for Block {
    fn from_bytes(data: &[u8]) -> Self {
        let c: Self = bincode::deserialize(data).expect("failed to decode the block");
        c.init()
    }

    fn init(mut self) -> Self {
        self.hash = self.compute_hash();
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize Block")
    }
}

impl BlockTrait for Block {
    fn get_hash(&self) -> Hash<Self> {
        self.hash.clone()
    }

    fn get_height(&self) -> Height {
        self.header.height
    }

    fn get_author(&self) -> Replica {
        self.header.author
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Body {
    pub tx_hashes: Vec<Hash<Transaction>>,
}

impl Body {
    pub fn new(txs: Vec<Arc<Transaction>>) -> Self {
        let hashes = txs
            .iter()
            .map(|tx| Hash::<Transaction>::ser_and_hash(tx.as_ref()))
            .collect();
        Self { tx_hashes: hashes }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Header {
    pub prev: Hash<Block>,
    pub extra: Vec<u8>,
    pub author: Replica,
    pub height: Height,
    pub blame_certificates: Vec<Certificate>,
}

impl std::fmt::Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block Header")
            .field("author", &self.author)
            .field("height", &self.height)
            .field("prev", &self.prev)
            .finish()
    }
}

impl std::fmt::Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.tx_hashes.is_empty() {
            f.debug_struct("Block Body")
                .field("Length", &self.tx_hashes.len())
                .field("First", &self.tx_hashes[0])
                .field("Last", &self.tx_hashes[self.tx_hashes.len() - 1])
                .finish()
        } else {
            f.debug_struct("Block Body")
                .field("Length", &self.tx_hashes.len())
                .finish()
        }
    }
}

impl Header {
    pub fn new() -> Self {
        Header {
            prev: Hash::<Block>::EMPTY_HASH,
            extra: Vec::new(),
            author: 0,
            height: 0,
            blame_certificates: Vec::new(),
        }
    }
}
