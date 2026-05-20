//! Apollo client state lives inline in `start.rs` post the keyed-
//! mempool / per-tx-confirmation rewrite. This file is kept for the
//! `mod context;` declaration in `mod.rs`; the legacy fields
//! (`pending` window, `time_map`, etc.) moved into `start::start`.
