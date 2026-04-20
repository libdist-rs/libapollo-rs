/// The core consensus module used for Opt Sync
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use crate::node::{context::Context, process::process_msg, proposal::do_propose};
use config::{ClientId, Node};
use libmempool::{BatchHash, ConsensusMempoolMsg};
use libstorage::rocksdb::Storage as RocksStore;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use types::optsync::{ClientMsg, Height, ProtocolMsg, Replica, Transaction};

pub async fn reactor(
    config: &Node,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    client_net: TlsReliableSender<ClientId, ClientMsg>,
    batch_store: RocksStore,
    mut rx_mem_to_consensus: UnboundedReceiver<BatchHash<Transaction>>,
    tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Height, Transaction>>,
) {
    log::debug!("Started timers");
    let mut cx = Context::new(
        config,
        consensus_net,
        client_net,
        batch_store,
        tx_consensus_to_mem,
    );
    let myid = config.id;
    loop {
        tokio::select! {
            pmsg_opt = consensus_recv.next() => {
                let protmsg = match pmsg_opt {
                    None => break,
                    Some(Err(e)) => {
                        log::warn!("Dropping undecodable protocol message: {}", e);
                        continue;
                    }
                    Some(Ok(x)) => x,
                };
                process_msg(&mut cx, protmsg).await;
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
            },
            b_opt = cx.commit_queue.next(), if !cx.commit_queue.is_empty() => {
                match b_opt {
                    None => {
                        log::info!("Timer finished");
                    },
                    Some(Ok(b)) => {
                        log::debug!("2Delta timer finished");
                        crate::node::commit::on_commit(b.into_inner(), &mut cx).await;
                    },
                    Some(Err(e)) => {
                        log::warn!("Timer misfired: {}", e);
                        continue;
                    }
                }
            }
        }
        while cx.next_leader() == myid
            && cx.cert_map.contains_key(&cx.last_seen_block.hash.clone())
            && !cx.pending_batches.is_empty()
        {
            let bh = cx.pending_batches.pop_front().unwrap();
            log::debug!("I {} am the leader and proposing batch {:?}", cx.myid, bh);
            do_propose(bh, &mut cx).await;
        }
    }
}
