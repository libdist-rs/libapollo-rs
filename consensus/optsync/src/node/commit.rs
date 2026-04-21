use libcrypto::hash::Hash;
use types::optsync::{ClientMsg, Payload, Propose, Transaction};

use crate::node::context::Context;
use std::sync::Arc;

/// Commit this block and all its ancestors. Hydrates the batch
/// contents into per-tx hashes so the client can match commits back
/// to outstanding submissions.
pub async fn on_commit(p: Arc<Propose>, cx: &mut Context) {
    let b = match cx.storage.delivered_block_from_hash(&p.block_hash) {
        Some(b) => b,
        None => {
            log::warn!("Commit fired for an undelivered block");
            return;
        }
    };
    if cx.storage.is_committed_by_hash(&b.hash) {
        return;
    }
    cx.last_committed_block_ht = b.header.height;
    cx.storage.add_committed_block(b.clone());

    // `tx_hashes()` is OnceLock-cached: free on the leader (intake
    // pipeline pre-filled), one-time SHA256 pass on followers the
    // first time a given batch commits.
    let tx_hashes: Vec<Hash<Transaction>> = match cx.read_batch(&b.body.batch_hash).await {
        Some(batch) => batch.tx_hashes().to_vec(),
        None => {
            log::warn!(
                "Batch {:?} missing from cache+store at commit time; pushing empty client notification",
                b.body.batch_hash
            );
            Vec::new()
        }
    };

    let payload = Payload::with_payload(cx.payload);
    let msg = ClientMsg::NewBlock(b.as_ref().clone(), tx_hashes, payload);
    cx.multicast_client(&msg).await;
    cx.gc_handlers();
}
