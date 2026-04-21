//! Re-export of the count-based sealer from `libapollo-mempool`.
//!
//! The historical libchatter-rs / libapollo-rs benchmark parameters
//! are expressed in *transactions per block* (`config.block_size`),
//! not bytes. `CountSealer` preserves that semantic, keeping the
//! stress-test matrix directly comparable to pre-mempool baselines.

pub use libmempool::CountSealer;
