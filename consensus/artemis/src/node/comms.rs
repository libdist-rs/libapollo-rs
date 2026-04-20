use types::artemis::{ClientMsg, ProtocolMsg, Replica};

use super::context::Context;
use std::sync::Arc;

/// Communication logic: send/multicast/multicast_client helpers that
/// serialize and hand off to the underlying libnet-rs senders.
impl Context {
    /// Send a `ProtocolMsg` to a specific peer.
    pub(crate) async fn send(&mut self, to: Replica, msg: Arc<ProtocolMsg>) {
        if to == self.myid() {
            return;
        }
        let bytes = Self::serialize_proto(msg.as_ref());
        match self.consensus_net.send(to, bytes).await {
            Ok(h) => self.remember_consensus(h),
            Err(e) => log::warn!("consensus send to {} failed: {:?}", to, e),
        }
    }

    /// Multicast (Sendall) to every peer but myself.
    pub(crate) async fn multicast(&mut self, msg: Arc<ProtocolMsg>) {
        let bytes = Self::serialize_proto(msg.as_ref());
        let results = self
            .consensus_net
            .broadcast(&self.broadcast_peers, bytes)
            .await;
        for r in results {
            match r {
                Ok(h) => self.remember_consensus(h),
                Err(e) => log::warn!("consensus broadcast leg failed: {:?}", e),
            }
        }
    }

    /// Multicast a `ClientMsg` to every registered client.
    pub(crate) async fn multicast_client(&mut self, msg: Arc<ClientMsg>) {
        if self.all_clients.is_empty() {
            return;
        }
        let bytes = bytes::Bytes::from(bincode::serialize(msg.as_ref()).expect("ClientMsg serialize"));
        let results = self.client_net.broadcast(&self.all_clients, bytes).await;
        for r in results {
            match r {
                Ok(h) => self.remember_client(h),
                Err(e) => log::warn!("client broadcast leg failed: {:?}", e),
            }
        }
    }
}
