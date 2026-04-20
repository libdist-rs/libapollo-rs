mod protocol;
pub use protocol::*;

mod msg;
pub use msg::*;

mod traits;
pub use traits::*;

pub mod hash;
pub use hash::{do_hash, ser_and_hash, Hash, EMPTY_HASH, HASH_SIZE};

pub type View = usize;

/// Extension trait preserving the libchatter-rs `Keypair::sign(msg)` ergonomic
/// on top of libcrypto-rs, which routes signing through `SecretKey` instead of
/// exposing it directly on `Keypair`.
pub trait KeypairSign {
    fn sign(&self, msg: &[u8]) -> anyhow::Result<Vec<u8>>;
}

impl KeypairSign for libcrypto::Keypair {
    #[inline]
    fn sign(&self, msg: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.private().sign(msg)
    }
}
