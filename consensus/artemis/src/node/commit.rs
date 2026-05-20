use libcrypto::hash::Hash;
use libmempool::BatcherConsensusMsg;
use std::convert::TryFrom;
use std::sync::Arc;
use types::artemis::Block;
use types::BlockTrait;

use super::*;

/// Commit blocks up to `cx.round() - num_faults`. Caller must ensure
/// `cx.round() > cx.num_faults()`. Drains the keyed batcher's
/// inflight bookkeeping via `BCM::Committed` and the confirmation
/// router via `tx_committed_to_router`.
pub async fn do_commit(cx: &mut Context) {
    log::debug!("Trying to commit");
    debug_assert!(cx.round() > cx.num_faults() as u64);

    let commit_round = cx.round() - cx.num_faults() as u64;
    let v = cx.vote_chain.get(&commit_round).unwrap();

    let mut com_hash = v.hash.clone();
    let mut newly_committed: Vec<Arc<Block>> = Vec::new();
    while !cx.storage.is_committed_by_hash(&com_hash) {
        let b = cx.storage.delivered_block_from_hash(&com_hash).unwrap();
        log::debug!("Committing block - {} in round {}", b.get_height(), v.round);
        cx.storage.add_committed_block(b.clone());
        com_hash =
            Hash::<Block>::try_from(b.blk.header.prev.as_ref()).expect("hash is exactly 32 bytes");
        newly_committed.push(b);
    }
    cx.bench_committed_tx_count = cx
        .bench_committed_tx_count
        .saturating_add((newly_committed.len() as u64) * cx.block_size as u64);

    // Oldest-first so the batcher sees monotonically increasing rounds.
    for block in newly_committed.into_iter().rev() {
        let batch = match cx.read_batch(&block.blk.body.batch_hash).await {
            Some(b) => b,
            None => {
                log::warn!(
                    "Batch {:?} missing from cache+store at commit time",
                    block.blk.body.batch_hash,
                );
                continue;
            }
        };
        let _ = cx
            .tx_consensus_to_batcher
            .send(BatcherConsensusMsg::Committed {
                batch: Arc::clone(&batch),
                round: block.blk.header.height,
            });
        let _ = cx.tx_committed_to_router.send(batch);
    }
}
