pub mod futures_manager;
pub mod tokio_manager;

/// Trait bound for types that travel through the vendored net codec.
/// Rely on raw serde + the standard thread-safety markers. Callers
/// implement `Message` (from `net_common`) for protocol-level decode
/// fallibility; inside the vendored net we just need a blanket bound.
pub trait NetMsg:
    serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static
{
}
impl<T> NetMsg for T where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static
{
}

use std::fs::File;
use std::io::BufReader;
use tokio_rustls::rustls::{self, internal::pemfile};

/// Load a chain of X.509 certs from a PEM file. Returns a single-cert
/// `Vec` for leaf certs, or multiple for full chains.
pub fn load_certs_pem(path: &str) -> Vec<rustls::Certificate> {
    let mut rdr = BufReader::new(
        File::open(path).unwrap_or_else(|e| panic!("open {}: {}", path, e)),
    );
    pemfile::certs(&mut rdr)
        .unwrap_or_else(|_| panic!("parse certs PEM: {}", path))
}

/// Load a single cert from a PEM file and return its DER bytes.
/// Convenience for callers that want the raw root cert.
pub fn load_root_cert_der(path: &str) -> Vec<u8> {
    let mut certs = load_certs_pem(path);
    certs.pop()
        .unwrap_or_else(|| panic!("no cert in {}", path))
        .0
}

/// Load a PKCS8-encoded private key from a PEM file.
pub fn load_private_key_pem(path: &str) -> rustls::PrivateKey {
    let mut rdr = BufReader::new(
        File::open(path).unwrap_or_else(|e| panic!("open {}: {}", path, e)),
    );
    let mut keys = pemfile::pkcs8_private_keys(&mut rdr)
        .unwrap_or_else(|_| panic!("parse pkcs8 key: {}", path));
    keys.pop()
        .unwrap_or_else(|| panic!("no pkcs8 key in {}", path))
}