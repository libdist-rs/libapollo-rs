mod protocol;
pub use protocol::*;

mod msg;
pub use msg::*;

mod traits;
pub use traits::*;

mod sealer;
pub use sealer::*;

pub type View = u64;

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
