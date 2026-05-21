/// The core consensus module used for Apollo.
///
/// The reactor reacts to all the messages from the network, drains
/// freshly-sealed batches from the keyed mempool, and triggers
/// proposals when this node is the round leader.
use super::{context::Context, message::*, proposal::*};
use config::Node;
use futures::future::FutureExt;
use libmempool::{BatchCache, BatchHash, BatcherConsensusMsg, CachedBatch};
use libstorage::rocksdb::Storage as RocksStore;
use std::sync::Arc;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use types::apollo::{ProtocolMsg, Replica, Transaction};

pub async fn reactor(
    config: &Node,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    batch_store: RocksStore,
    batch_cache: Arc<BatchCache<Transaction>>,
    mut rx_mem_to_consensus: UnboundedReceiver<(
        BatchHash<Transaction>,
        Arc<CachedBatch<Transaction>>,
    )>,
    tx_consensus_to_batcher: UnboundedSender<BatcherConsensusMsg<Transaction>>,
    tx_committed_to_router: UnboundedSender<Arc<CachedBatch<Transaction>>>,
) {
    let mut cx = Context::new(
        config,
        consensus_net,
        batch_store,
        batch_cache,
        tx_consensus_to_batcher,
        tx_committed_to_router,
    );

    let myid = config.id;

    // Seed the batcher with the initial leader / round-0 state so it
    // knows it can start proposing immediately on the first batch.
    let _ = cx
        .tx_consensus_to_batcher
        .send(BatcherConsensusMsg::NewRound {
            leader: cx.round_leader(),
            round: cx.round(),
        });

    // Bench-only throughput sampler. `cx.bench_committed_tx_count`
    // counts actual `batch.payload.len()` per committed block (see
    // commit.rs), so the per-window tps reported here reflects the
    // true committed-tx rate from this node's view. The stress-test
    // orchestrator ignores this line (it scrapes the client process's
    // stdio); it is preserved for direct node-log inspection and for
    // `bench-artemis-metrics.sh`-style harnesses.
    let window_secs = cx.bench_emit_window_secs;
    let metrics_node = cx.bench_metrics_node;
    let mut throughput_tick =
        tokio::time::interval(std::time::Duration::from_secs(window_secs));
    throughput_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = throughput_tick.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            _ = throughput_tick.tick() => {
                if myid == metrics_node {
                    let tx = cx.bench_committed_tx_count;
                    cx.bench_committed_tx_count = 0;
                    eprintln!("DP[Throughput]: {}", (tx as f64) / window_secs as f64);
                }
            },
            pmsg_opt = consensus_recv.next() => {
                let pmsg = match pmsg_opt {
                    None => {
                        log::error!("Protocol message channel closed");
                        std::process::exit(0);
                    }
                    Some(Err(e)) => {
                        log::warn!("Dropping undecodable protocol message: {}", e);
                        continue;
                    }
                    Some(Ok(x)) => x,
                };
                handle_message(pmsg, &mut cx);
                while let Some(Ok(pmsg)) = consensus_recv.next().now_or_never().flatten() {
                    handle_message(pmsg, &mut cx);
                }
                process_message(&mut cx).await;
            },
            batch_opt = rx_mem_to_consensus.recv() => {
                match batch_opt {
                    None => {
                        log::error!("Mempool channel closed");
                        break;
                    }
                    Some((bh, batch)) => {
                        log::debug!("Got new batch from mempool: {:?}", bh);
                        cx.pending_batches.push_back((bh, batch));
                    }
                }
            }
        }
        // Leader drains its pending-batch queue.
        while cx.round_leader() == myid && !cx.pending_batches.is_empty() {
            let (bh, batch) = cx.pending_batches.pop_front().unwrap();
            do_propose(bh, batch, &mut cx).await;
        }
    }
}
