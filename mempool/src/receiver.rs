//! The intake task: binds a TCP listener on `client_addr`, accepts
//! incoming tx bytes, decodes them via `tcp-receiver`'s `Stream`, and
//! forwards them to the batcher.
//!
//! We deliberately do NOT hash at intake. In the stress-test topology
//! the client broadcasts every tx to every node, so intake-time
//! hashing would cost `N × batch_size × SHA256` per block instead of
//! the `1 × batch_size × SHA256` the old libmempool design paid at
//! propose time on the leader only. `CachedBatch::tx_hashes()` is
//! `OnceLock`-lazy, computed on first call -- so the leader pays the
//! hydrate cost once when it builds the client notification, just
//! like before.

use net_common::Message;
use std::net::SocketAddr;
use tcp_receiver::TcpReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::StreamExt;

pub struct Receiver;

impl Receiver {
    pub fn spawn<Tx>(client_addr: SocketAddr, tx_batcher: UnboundedSender<Tx>)
    where
        Tx: Message + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            let mut rx = TcpReceiver::<Tx>::spawn(client_addr);
            loop {
                match rx.next().await {
                    None => {
                        log::info!("Mempool receiver: tcp stream closed, exiting");
                        return;
                    }
                    Some(Err(_e)) => {
                        // `Tx::DeserializationError` isn't `Debug`-bounded;
                        // we know it's a decode failure, count it silently.
                        log::warn!("Mempool receiver: decode error on incoming tx");
                        continue;
                    }
                    Some(Ok(t)) => {
                        if tx_batcher.send(t).is_err() {
                            log::info!("Mempool receiver: batcher channel closed");
                            return;
                        }
                    }
                }
            }
        });
    }
}
