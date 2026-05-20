//! Apollo no longer ships a protocol-specific `ClientMsg`. The new
//! mempool ingress / confirmation path uses `libmempool::ClientMsg<Tx>`
//! (NewTx / NewBatch / Confirmation), so this file is empty by design.
//! Kept as a placeholder so `mod client;` in `mod.rs` still resolves.
