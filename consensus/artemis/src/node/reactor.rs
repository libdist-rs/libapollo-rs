/// The core consensus module used for Artemis
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use super::{buffer_message, context::Context, do_new_block, process_message};
use crate::node::round_vote::try_round_vote;
use config::{ClientId, Node};
use futures::future::FutureExt;
use libmempool::{BatchCache, BatchHash, CachedBatch, ConsensusMempoolMsg};
use libstorage::rocksdb::Storage as RocksStore;
use std::sync::Arc;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use types::artemis::{ClientMsg, ProtocolMsg, Replica, Round, Transaction};

pub async fn reactor(
    config: &Node,
    is_client_apollo_enabled: bool,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    client_net: TlsReliableSender<ClientId, ClientMsg>,
    batch_store: RocksStore,
    batch_cache: Arc<BatchCache<Transaction>>,
    mut rx_mem_to_consensus: UnboundedReceiver<(
        BatchHash<Transaction>,
        Arc<CachedBatch<Transaction>>,
    )>,
    tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Round, Transaction>>,
) {
    let mut cx = Context::new(
        config,
        consensus_net,
        client_net,
        batch_store,
        batch_cache,
        tx_consensus_to_mem,
        is_client_apollo_enabled,
    );
    let myid = config.id;
    let metrics = cx.metrics.clone();
    let sigint_myid = myid;
    // Install signal handlers once; on SIGINT or SIGTERM dump metrics
    // to stderr and bail. `process::exit` forces the flush even when
    // the reactor is blocked in `multicast`.
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut term = signal(SignalKind::terminate()).expect("SIGTERM stream");
            let mut int = signal(SignalKind::interrupt()).expect("SIGINT stream");
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        metrics.print_summary(sigint_myid as u32);
        std::process::exit(0);
    });

    // Bench-only: throughput sampler. We always tick the interval (the
    // overhead is negligible) but only the configured metrics node
    // actually emits, so a multi-node run produces a single
    // `DP[Throughput]` stream that the orchestrator can latch onto.
    let window_secs = cx.bench_emit_window_secs;
    let metrics_node = cx.bench_metrics_node;
    let mut throughput_tick = tokio::time::interval(
        std::time::Duration::from_secs(window_secs),
    );
    throughput_tick.set_missed_tick_behavior(
        tokio::time::MissedTickBehavior::Delay,
    );
    let _ = throughput_tick.tick().await; // consume the first (instant) tick

    loop {
        cx.metrics.record_reactor_iter();
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
                buffer_message(pmsg, &mut cx);
                while let Some(Ok(pmsg)) = consensus_recv.next().now_or_never().flatten() {
                    buffer_message(pmsg, &mut cx);
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
                        cx.metrics.record_batch_recv();
                        cx.pending_batches.push_back((bh, batch));
                    }
                }
            }
        }
        // View leader drains its pending-batch queue.
        while cx.view_leader == myid && !cx.pending_batches.is_empty() {
            let (bh, batch) = cx.pending_batches.pop_front().unwrap();
            log::debug!("I {} am the view leader and dispatching batch {:?}", cx.myid(), bh);
            do_new_block(bh, batch, &mut cx).await;
        }
        try_round_vote(&mut cx).await;
    }
}
