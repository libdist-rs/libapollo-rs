//! The Batcher task: accumulates `Tx` values arriving from the intake
//! receiver, and when the `Sealer`'s `should_seal` predicate fires,
//! emits an `Arc<CachedBatch<Tx>>` downstream to the Processor.
//!
//! The Batcher is the point at which we commit to a specific ordering
//! of transactions inside a batch. No further reordering happens.
//!
//! Tx hashes are NOT precomputed here. `CachedBatch::tx_hashes()` is
//! `OnceLock`-lazy and fills itself on first call -- typically on
//! the leader when it builds a `ClientMsg::NewBlock`. Doing it
//! eagerly at intake would cost `N × batch_size × SHA256` per block
//! across the cluster (every follower hashes every tx it receives on
//! the client-broadcast pattern), which is much worse than the
//! `1 × batch_size × SHA256` leader-side cost the lazy path pays.

use crate::batch::CachedBatch;
use crate::sealer::Sealer;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct Batcher;

impl Batcher {
    pub fn spawn<Tx, S>(
        mut rx: UnboundedReceiver<Tx>,
        tx: UnboundedSender<Arc<CachedBatch<Tx>>>,
        sealer: S,
    ) where
        Tx: Send + Sync + 'static,
        S: Sealer<Tx>,
    {
        tokio::spawn(async move {
            let mut queue_txs: Vec<Tx> = Vec::new();

            while let Some(t) = rx.recv().await {
                queue_txs.push(t);

                if sealer.should_seal(queue_txs.len()) {
                    let payload = std::mem::take(&mut queue_txs);
                    let batch = Arc::new(CachedBatch::new(payload));
                    if tx.send(batch).is_err() {
                        log::info!("Batcher: processor channel closed, exiting");
                        return;
                    }
                }
            }
            log::info!("Batcher: intake channel closed, exiting");
        });
    }
}
