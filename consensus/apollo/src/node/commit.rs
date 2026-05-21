use libmempool::BatcherConsensusMsg;
use std::sync::Arc;

use super::context::Context;

pub async fn do_commit(cx: &mut Context) {
    log::debug!("Trying to commit blocks");

    let commit_round = cx.round() - cx.num_faults() as u64;
    let p = cx.prop_chain_by_round.get(&commit_round).unwrap().clone();
    let mut hash = p.block_hash.clone();
    let mut newly_committed: Vec<Arc<types::apollo::Block>> = Vec::new();
    while !cx.storage.is_committed_by_hash(&hash) {
        let b_rc = cx.storage.delivered_block_from_hash(&hash).unwrap();
        cx.storage.add_committed_block(b_rc.clone());
        hash = b_rc.header.prev.clone();
        newly_committed.push(b_rc);
    }
    // Notify the keyed batcher and the confirmation router about each
    // newly-committed block. The batcher advances per-client
    // `high_committed_nonce` and GCs stale Mineable/InFlight; the
    // router looks up each tx's `reply_to` and sends a per-tx
    // `Confirmation(Hash<Tx>)` back to the originating client.
    //
    // We commit in chain order (oldest first) so the batcher sees
    // monotonically increasing rounds. `bench_committed_tx_count` is
    // accumulated here from the actual `batch.payload.len()` -- the
    // mempool's `block_size` is a byte budget, not a tx count, so the
    // old `newly_committed.len() * block_size` overstated throughput
    // (typically by ~26x at the default config).
    for block in newly_committed.into_iter().rev() {
        let batch = match cx.read_batch(&block.body.batch_hash).await {
            Some(b) => b,
            None => {
                log::warn!(
                    "Batch {:?} missing from cache+store at commit time",
                    block.body.batch_hash,
                );
                continue;
            }
        };
        cx.bench_committed_tx_count = cx
            .bench_committed_tx_count
            .saturating_add(batch.payload.len() as u64);
        let _ = cx.tx_consensus_to_batcher.send(BatcherConsensusMsg::Committed {
            batch: Arc::clone(&batch),
            round: block.header.height,
        });
        let _ = cx.tx_committed_to_router.send(batch);
    }
}
