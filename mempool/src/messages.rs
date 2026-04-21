//! Consensus <-> mempool control messages.
//!
//! Matches libmempool-rs's `ConsensusMempoolMsg` shape for drop-in
//! source compatibility at the consensus side. We keep both variants
//! even though our mempool currently treats them as nop (we have no
//! separate Synchronizer -- consensus's own `ProtocolMsg::Request /
//! Response` path ferries missing batches inline with block recovery),
//! because the consensus reactors still call `tx_consensus_to_mem.send(...)`
//! on round advance and might evolve to use the signal for GC.

use crate::batch::BatchHash;

pub enum ConsensusMempoolMsg<Id, Round, Tx> {
    /// Round advanced; mempool may garbage-collect any state that was
    /// round-scoped (we currently have none).
    End(Round),
    /// Consensus observed a referenced batch hash it doesn't have.
    /// Kept for API compat; our mempool doesn't act on it today.
    UnknownBatch(Id, Vec<BatchHash<Tx>>),
}
