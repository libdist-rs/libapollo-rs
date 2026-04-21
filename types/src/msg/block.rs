use libcrypto::hash::Hash;
use libmempool::{BatchHash, CachedBatch};
use net_common::Message;
use serde::{Deserialize, Deserializer, Serialize};

use super::{Certificate, Transaction};
use crate::{protocol::{Height, Replica}, BlockTrait};

/// Wire format is `(header, body)`; the cached `hash` is never transmitted.
/// A custom `Deserialize` recomputes it on the way in, so any `Block` that
/// came off the network carries a valid `hash`. Locally-built blocks
/// (`with_batch`, `GENESIS_BLOCK`) start with `EMPTY_HASH` and finalize via
/// `Block::init()`.
///
/// Post-libmempool-rs the block payload is a single `BatchHash<Transaction>`
/// (Narwhal-style) rather than an inline `Vec<Hash<Transaction>>`: the
/// canonical tx ordering lives in the referenced `Batch` stored in
/// `libstorage::Store`, not the block itself.
#[derive(Serialize, Debug, Clone)]
pub struct Block {
    pub header: Header,
    pub body: Body,
    #[serde(skip)]
    pub hash: Hash<Block>,
}

impl<'de> Deserialize<'de> for Block {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `Wire` mirrors `Block` minus the skipped `hash`. Bincode encodes
        // struct fields positionally without names or length markers, so the
        // bytes for `Block` (with `hash` skipped) and `Wire` are identical.
        // Hashing the reconstructed `Block` therefore matches what the
        // sender hashed. A self-describing format (JSON, CBOR, MessagePack)
        // would break that identity.
        #[derive(Deserialize)]
        struct Wire {
            header: Header,
            body: Body,
        }
        let w = Wire::deserialize(deserializer)?;
        let mut block = Block {
            header: w.header,
            body: w.body,
            hash: Hash::<Block>::EMPTY_HASH,
        };
        block.hash = Hash::<Block>::ser_and_hash(&block);
        Ok(block)
    }
}

impl Block {
    /// Build a block that references the given batch. Caller is
    /// expected to populate header fields (author, height, prev) and
    /// finalize with `init()`.
    pub fn with_batch(batch_hash: BatchHash<Transaction>) -> Self {
        Block {
            header: Header::new(),
            body: Body { batch_hash },
            hash: Hash::<Block>::EMPTY_HASH,
        }
    }

    pub fn compute_hash(&self) -> Hash<Self> {
        Hash::<Self>::ser_and_hash(self)
    }

    /// Builder finalizer: recompute the cached hash after mutating
    /// header/body fields.
    pub fn init(mut self) -> Self {
        self.hash = self.compute_hash();
        self
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
        batch_hash: Hash::<CachedBatch<Transaction>>::EMPTY_HASH,
    },
    hash: Hash::<Block>::EMPTY_HASH,
};

impl Message for Block {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        // Custom Deserialize populates `hash` as part of decoding.
        bincode::deserialize(bytes)
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

/// The payload reference. A single `BatchHash` rather than inlined
/// transactions: the batch itself lives in `libstorage::Store`, keyed
/// by hash. Receivers that don't have the batch already can request
/// it from the leader or a peer via `MempoolMsg::RequestBatch`.
#[derive(Serialize, Deserialize, Clone)]
pub struct Body {
    pub batch_hash: BatchHash<Transaction>,
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
        f.debug_struct("Block Body")
            .field("batch", &self.batch_hash)
            .finish()
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
