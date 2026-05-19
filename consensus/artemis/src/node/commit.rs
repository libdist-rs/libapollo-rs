use std::convert::TryFrom;
use libcrypto::hash::Hash;
use types::BlockTrait;
use types::artemis::Block;
use super::*;

/// Do commit is called to trigger committing of blocks
/// Caller needs to ensure that `cx.round > cx.num_faults()`
pub fn do_commit(cx: &mut Context) {
    log::debug!("Trying to commit");
    debug_assert!(cx.round() > cx.num_faults() as u64);

    // Get the r-f^th vote
    let commit_round = cx.round() - cx.num_faults() as u64;
    let v = cx.vote_chain.get(&commit_round).unwrap();    

    let mut com_hash = v.hash.clone();
    // Commit com_hash and its parents
    let mut newly_committed_blocks: u64 = 0;
    while !cx.storage.is_committed_by_hash(&com_hash) {
        let b = cx.storage.delivered_block_from_hash(&com_hash).unwrap();
        log::debug!("Committing block - {} in round {}", b.get_height(), v.round);
        cx.storage.add_committed_block(b.clone());
        com_hash = Hash::<Block>::try_from(b.blk.header.prev.as_ref())
            .expect("hash is exactly 32 bytes");
        newly_committed_blocks += 1;
    }
    // Bench: one block_size's worth of txs per newly-committed block.
    // The reactor reads & resets this counter on a tokio interval to
    // emit `DP[Throughput]`.
    cx.bench_committed_tx_count = cx
        .bench_committed_tx_count
        .saturating_add(newly_committed_blocks * cx.block_size as u64);
}