use super::context::Context;

pub async fn do_commit(cx: &mut Context) {
    log::debug!("Trying to commit blocks");

    // Add all parents if not committed already
    let commit_round = cx.round() - cx.num_faults() as u64;
    let p = cx.prop_chain_by_round.get(&commit_round).unwrap().clone();
    let mut hash = p.block_hash.clone();
    while !cx.storage.is_committed_by_hash(&hash) {
        let b_rc = cx.storage.delivered_block_from_hash(&hash).unwrap();
        cx.storage.add_committed_block(b_rc.clone());
        hash = b_rc.header.prev.clone();
    }

    // Non-special clients learn about commits here (special clients
    // were notified at propose time via `do_propose`). We hydrate tx
    // hashes out of the batch store so the client-side latency
    // tracker has something to match its submissions against.
    if !cx.is_client_apollo_enabled() {
        let commit_block = cx
            .storage
            .delivered_block_from_hash(&p.block_hash)
            .unwrap();
        let tx_hashes = match cx.read_batch(&commit_block.body.batch_hash).await {
            Some(batch) => Context::hydrate_tx_hashes(&batch),
            None => {
                log::warn!(
                    "Batch {:?} missing at commit time; pushing empty client notification",
                    commit_block.body.batch_hash
                );
                Vec::new()
            }
        };
        cx.multicast_client(p, commit_block, tx_hashes).await;
    }
}
