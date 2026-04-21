//! The Processor task: receives sealed `Arc<CachedBatch>`es from the
//! Batcher, installs them in the in-memory cache, notifies consensus
//! via `tx_to_consensus`, and forwards to the durable-write task.
//!
//! Consensus becomes aware of the batch the moment it's sealed. The
//! rocksdb persist runs in parallel -- not on the critical path.
//! `libstorage::Store::write` is already a fire-and-forget channel
//! send into the rocksdb-writer task, so `await`-ing it here does not
//! gate consensus progress.

use crate::batch::{BatchHash, CachedBatch};
use crate::cache::BatchCache;
use libstorage::Store;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct Processor;

impl Processor {
    pub fn spawn<Tx, S>(
        mut rx: UnboundedReceiver<Arc<CachedBatch<Tx>>>,
        tx_consensus: UnboundedSender<(BatchHash<Tx>, Arc<CachedBatch<Tx>>)>,
        cache: Arc<BatchCache<Tx>>,
        mut store: S,
    ) where
        Tx: Serialize + Send + Sync + 'static,
        S: Store + Send + 'static,
    {
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                // Hash is cached on the `CachedBatch`; first call here
                // computes it, subsequent consumers get the cached value
                // for free.
                let hash = batch.hash();

                // 1. Install in the cache BEFORE notifying consensus, so
                //    a follower that asks us for this batch between the
                //    signal and the rocksdb persist still gets a hit.
                cache.insert(hash.clone(), Arc::clone(&batch));

                // 2. Notify consensus. It receives an `Arc<CachedBatch>`
                //    directly -- no rocksdb round-trip needed on the
                //    leader's propose path.
                if tx_consensus.send((hash.clone(), Arc::clone(&batch))).is_err() {
                    log::info!("Processor: consensus channel closed, exiting");
                    return;
                }

                // 3. Durable persist. `store.write` is already a channel
                //    send into the rocksdb-writer task, so this does not
                //    block consensus. A crash before the write lands
                //    makes this node unable to serve the batch on a
                //    request -- but any honest follower that saw the
                //    proposal can, and n > 2f guarantees at least one
                //    such follower exists.
                let bytes =
                    bincode::serialize(batch.as_ref()).expect("CachedBatch serialize");
                let key = hash.as_ref().to_vec();
                store.write(key, bytes).await;
            }
            log::info!("Processor: batcher channel closed, exiting");
        });
    }
}
