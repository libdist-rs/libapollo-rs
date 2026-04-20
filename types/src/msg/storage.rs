use crate::{BlockTrait, Height, TxTrait};
use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};
use libcrypto::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

/// Per-node block index. Now purely a *block* store -- transaction
/// mempool state lives in `libmempool-rs`'s `Mempool` / `Processor` /
/// `libstorage::Store` pipeline, not in this struct.
///
/// The `T: TxTrait` parameter is retained so callers can keep the
/// existing per-protocol `Storage<Block, Transaction>` aliases
/// without a per-crate rename.
pub struct Storage<B, T>
where
    B: BlockTrait,
    T: TxTrait,
{
    all_delivered_blocks_by_hash: HashMap<Hash<B>, Arc<B>>,
    all_delivered_blocks_by_ht: HashMap<Height, Arc<B>>,
    committed_blocks_by_hash: HashSet<Hash<B>>,
    committed_blocks_by_ht: HashSet<Height>,
    _tx: PhantomData<T>,
}

impl<B, T> Storage<B, T>
where
    B: BlockTrait,
    T: TxTrait,
{
    pub fn new() -> Self {
        Storage {
            all_delivered_blocks_by_hash: HashMap::default(),
            all_delivered_blocks_by_ht: HashMap::default(),
            committed_blocks_by_hash: HashSet::default(),
            committed_blocks_by_ht: HashSet::default(),
            _tx: PhantomData,
        }
    }

    pub fn delivered_block_from_ht(&self, height: Height) -> Option<Arc<B>> {
        self.all_delivered_blocks_by_ht.get(&height).cloned()
    }

    pub fn delivered_block_from_hash(&self, hash: &Hash<B>) -> Option<Arc<B>> {
        self.all_delivered_blocks_by_hash.get(hash).cloned()
    }

    pub fn committed_block_from_ht(&self, height: Height) -> Option<Arc<B>> {
        if self.committed_blocks_by_ht.contains(&height) {
            self.delivered_block_from_ht(height)
        } else {
            None
        }
    }

    pub fn committed_block_by_hash(&self, hash: &Hash<B>) -> Option<Arc<B>> {
        if self.committed_blocks_by_hash.contains(hash) {
            self.delivered_block_from_hash(hash)
        } else {
            None
        }
    }

    pub fn add_delivered_block(&mut self, b_rc: Arc<B>) {
        let ht = b_rc.get_height();
        self.all_delivered_blocks_by_hash
            .insert(b_rc.get_hash(), b_rc.clone());
        self.all_delivered_blocks_by_ht.insert(ht, b_rc);
    }

    pub fn add_committed_block(&mut self, b_rc: Arc<B>) {
        self.committed_blocks_by_hash.insert(b_rc.get_hash());
        self.committed_blocks_by_ht.insert(b_rc.get_height());
    }

    pub fn is_committed_by_ht(&self, height: Height) -> bool {
        self.committed_blocks_by_ht.contains(&height)
    }

    pub fn is_delivered_by_ht(&self, height: Height) -> bool {
        self.all_delivered_blocks_by_ht.contains_key(&height)
    }

    pub fn is_delivered_by_hash(&self, hash: &Hash<B>) -> bool {
        self.all_delivered_blocks_by_hash.contains_key(hash)
    }

    pub fn is_committed_by_hash(&self, hash: &Hash<B>) -> bool {
        self.committed_blocks_by_hash.contains(hash)
    }
}
