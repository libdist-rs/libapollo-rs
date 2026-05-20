//! Apollo "normal client" — distinguished from `apollo::client` only
//! historically (early-notification vs. commit-notification). With the
//! keyed mempool + per-tx confirmation router, both clients see commits
//! only, so `normal_client::start` is now a thin forwarder to
//! `apollo::client::start` with a default burst pacing.
//!
//! Kept as a separate binary so the existing orchestrator scripts that
//! reference `normal-client-apollo` still resolve.

use config::Client;

pub async fn start(c: &Client, metric: u64, window: usize) {
    // `window` historically gated a closed-loop sender; remapping to
    // burst-size keeps the CLI surface stable. Burst interval matches
    // the new client's default (100ms).
    let txs_per_burst = window.max(1);
    crate::client::start(c, metric, txs_per_burst, 100).await;
}
