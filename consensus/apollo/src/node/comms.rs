use types::apollo::{ProtocolMsg, Replica};

use super::context::Context;
use std::sync::Arc;

/// Communication helpers: send / multicast on the consensus network.
/// Per-tx client confirmations are routed by the mempool's
/// `ConfirmationRouter`, so there is no `multicast_client` here any
/// more — the node-side code only ships `ProtocolMsg`s.
impl Context {
    /// Send a `ProtocolMsg` to a specific peer. Serializes once and
    /// stashes the returned handler under the current round.
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
}
