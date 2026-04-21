//! Batch-sealing policy.
//!
//! Unlike libmempool-rs's Future-based `Sealer`, ours is a plain
//! synchronous predicate polled by the batcher after every tx
//! insertion. The batcher owns the queue; the sealer is stateless
//! (or holds policy parameters only).
//!
//! The policy consumes only the current queue length. A size-based
//! sealer would need raw bytes alongside the decoded `Tx` on the
//! intake path, which we deliberately avoid so the receiver path
//! doesn't re-serialize each tx just to measure its size. Add a
//! `SizeSealer` on a separate intake path later if needed.

/// A sealing predicate. Called by `Batcher` after each tx lands; when
/// it returns `true`, the batcher ships the accumulated txs as a
/// `CachedBatch` and resets.
pub trait Sealer<Tx>: Send + 'static {
    fn should_seal(&self, queue_len: usize) -> bool;
}

/// Seal when the queue has accumulated `threshold` transactions.
/// Matches the historical `config.block_size` = "txs per block"
/// semantic used by libchatter-rs / libapollo-rs benchmarks.
pub struct CountSealer {
    threshold: usize,
}

impl CountSealer {
    pub fn new(threshold: usize) -> Self {
        assert!(threshold > 0, "CountSealer threshold must be > 0");
        Self { threshold }
    }
}

impl<Tx> Sealer<Tx> for CountSealer {
    fn should_seal(&self, queue_len: usize) -> bool {
        queue_len >= self.threshold
    }
}
