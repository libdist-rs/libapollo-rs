use libcrypto::hash::Hash;
use types::synchs::{ClientMsg, Payload, Propose, Transaction};

use super::context::Context;
use std::sync::Arc;

/// Commit this block and all its ancestors. Hydrates the batch
/// contents into per-tx hashes so the client can match commits back
/// to its outstanding submissions.
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

    // Hydrate tx hashes from the referenced batch. `persist_batch` ran
    // when the proposal was delivered, so absence here would mean a
    // rocksdb fault -- log + skip rather than panic.
    let tx_hashes: Vec<Hash<Transaction>> = match cx.read_batch(&b.body.batch_hash).await {
        Some(batch) => batch
            .payload
            .iter()
            .map(|tx| Hash::<Transaction>::ser_and_hash(tx))
            .collect(),
        None => {
            log::warn!(
                "Batch {:?} missing from store at commit time; pushing an empty client notification",
                b.body.batch_hash
            );
            Vec::new()
        }
    };

    let payload = Payload::with_payload(cx.payload);
    let msg = ClientMsg::NewBlock(b.as_ref().clone(), tx_hashes, payload);
    cx.multicast_client(&msg).await;

    // GC any cancel handlers that have resolved since we last advanced
    // the height. See `Context::gc_handlers` for the retention rule.
    cx.gc_handlers();
}
