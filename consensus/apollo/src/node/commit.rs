use super::context::Context;

pub async fn do_commit(cx: &mut Context) {
    log::debug!("Trying to commit blocks");

    // Add all parents if not committed already
    let commit_round = cx.round() - cx.num_faults() as u64;
    let p = cx.prop_chain_by_round.get(&commit_round).unwrap().clone();
    let mut hash = p.block_hash.clone();
    let mut newly_committed_blocks: u64 = 0;
    while !cx.storage.is_committed_by_hash(&hash) {
        let b_rc = cx.storage.delivered_block_from_hash(&hash).unwrap();
        cx.storage.add_committed_block(b_rc.clone());
        hash = b_rc.header.prev.clone();
        newly_committed_blocks += 1;
    }
    // Bench: count one block_size's worth of txs per newly-committed
    // block. The reactor reads & resets this counter on a tokio
    // interval to emit `DP[Throughput]`.
    cx.bench_committed_tx_count = cx
        .bench_committed_tx_count
        .saturating_add(newly_committed_blocks * cx.block_size as u64);

    // Non-special clients learn about commits here (special clients
    // were notified at propose time via `do_propose`). `read_batch`
    // hits the in-memory cache; hydrate_tx_hashes is `OnceLock`-cached.
    if !cx.is_client_apollo_enabled() {
        let commit_block = cx
            .storage
            .delivered_block_from_hash(&p.block_hash)
            .unwrap();
        let tx_hashes = match cx.read_batch(&commit_block.body.batch_hash).await {
            Some(batch) => Context::hydrate_tx_hashes(batch.as_ref()),
            None => {
                log::warn!(
                    "Batch {:?} missing from cache+store at commit time; pushing empty client notification",
                    commit_block.body.batch_hash
                );
                Vec::new()
            }
        };
        cx.multicast_client(p, commit_block, tx_hashes).await;
    }
}
