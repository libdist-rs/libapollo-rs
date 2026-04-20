/// The core consensus module used for Apollo
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use super::{context::Context, message::*, proposal::*};
use config::{ClientId, Node};
use futures::future::FutureExt;
use libmempool::{BatchHash, ConsensusMempoolMsg};
use libstorage::rocksdb::Storage as RocksStore;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use types::apollo::{ClientMsg, ProtocolMsg, Replica, Round, Transaction};

pub async fn reactor(
    config: &Node,
    is_client_apollo_enabled: bool,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    client_net: TlsReliableSender<ClientId, ClientMsg>,
    batch_store: RocksStore,
    mut rx_mem_to_consensus: UnboundedReceiver<BatchHash<Transaction>>,
    tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Round, Transaction>>,
) {
    let mut cx = Context::new(
        config,
        consensus_net,
        client_net,
        batch_store,
        tx_consensus_to_mem,
        is_client_apollo_enabled,
    );

    let myid = config.id;

    loop {
        tokio::select! {
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
                // Drain any other ready messages in one pass so the
                // delivery/relay buffers process together.
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
                    Some(bh) => {
                        log::debug!("Got new batch from mempool: {:?}", bh);
                        cx.pending_batches.push_back(bh);
                    }
                }
            }
        }
        // Leader drains its pending-batch queue.
        while cx.round_leader() == myid && !cx.pending_batches.is_empty() {
            let bh = cx.pending_batches.pop_front().unwrap();
            log::debug!("I {} am the leader and proposing batch {:?}", cx.myid(), bh);
            do_propose(bh, &mut cx).await;
        }
    }
}
