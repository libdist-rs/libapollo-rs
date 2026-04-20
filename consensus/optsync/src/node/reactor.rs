/// The core consensus module used for Opt Sync
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use crate::node::{
    context::Context, proposal::do_propose, process::process_msg,
};
use config::{ClientId, Node};
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio_stream::StreamExt;
use types::optsync::{ClientMsg, ProtocolMsg, Replica, Transaction};

pub async fn reactor(
    config: &Node,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    client_net: TlsReliableSender<ClientId, ClientMsg>,
    mut tx_recv: TlsReceiver<Transaction>,
) {
    log::debug!("Started timers");
    let mut cx = Context::new(config, consensus_net, client_net);
    let block_size = config.block_size;
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
            tx_opt = tx_recv.next() => {
                log::trace!("Got tx from the client: {:?}", tx_opt);
                let tx = match tx_opt {
                    None => break,
                    Some(Err(e)) => {
                        log::warn!("Dropping undecodable transaction: {}", e);
                        continue;
                    }
                    Some(Ok(x)) => x,
                };
                cx.storage.add_transaction(tx);
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
        if cx.storage.get_tx_pool_size() >= block_size
            && cx.next_leader() == myid
            && cx.cert_map.contains_key(&cx.last_seen_block.hash.clone())
        {
            log::debug!("I {} am the leader and, I am proposing", cx.myid);
            let txs = cx.storage.cleave(block_size);
            do_propose(txs, &mut cx).await;
        }
    }
}
