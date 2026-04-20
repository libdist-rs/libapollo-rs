use crate::{BlockTrait, Height, TxTrait};
use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};
use libcrypto::hash::Hash;
use linked_hash_map::LinkedHashMap;
use std::sync::Arc;

/// Storage holds on to all the blocks and transactions.
/// Disable feature `mempool` if the end program does not need any client.
pub struct Storage<B, T>
where
    B: BlockTrait,
    T: TxTrait,
{
    all_delivered_blocks_by_hash: HashMap<Hash<B>, Arc<B>>,
    all_delivered_blocks_by_ht: HashMap<Height, Arc<B>>,
    committed_blocks_by_hash: HashSet<Hash<B>>,
    committed_blocks_by_ht: HashSet<Height>,
    #[cfg(feature = "mempool")]
    pending_tx: LinkedHashMap<Hash<T>, Arc<T>>,
}

impl<B, T> Storage<B, T>
where
    B: BlockTrait,
    T: TxTrait,
{
    pub fn new(space: usize) -> Self {
        Storage {
            all_delivered_blocks_by_hash: HashMap::default(),
            all_delivered_blocks_by_ht: HashMap::default(),
            committed_blocks_by_hash: HashSet::default(),
            committed_blocks_by_ht: HashSet::default(),
            #[cfg(feature = "mempool")]
            pending_tx: LinkedHashMap::with_capacity(space),
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

    /// Removes `block_size` transactions from the tx pool (for block creation).
    #[cfg(feature = "mempool")]
    pub fn cleave(&mut self, block_size: usize) -> Vec<Arc<T>> {
        let mut txs = Vec::with_capacity(block_size);
        for _ in 0..block_size {
            let (_hash, tx) = self
                .pending_tx
                .pop_front()
                .expect("Dequeued when tx pool was not block size");
            txs.push(tx);
        }
        txs
    }

    /// Removes the transaction hashes from the pool (called after commit).
    #[cfg(feature = "mempool")]
    pub fn clear(&mut self, tx_hashes: &Vec<Hash<T>>) {
        for h in tx_hashes {
            self.pending_tx.remove(h);
        }
    }

    /// Adds a transaction to the pool.
    #[cfg(feature = "mempool")]
    pub fn add_transaction(&mut self, t: T) {
        let tx_hash = t.get_hash();
        self.pending_tx.insert(tx_hash, Arc::new(t));
    }

    /// Returns the number of transactions currently in the tx pool.
    #[cfg(feature = "mempool")]
    pub fn get_tx_pool_size(&self) -> usize {
        self.pending_tx.len()
    }
}
