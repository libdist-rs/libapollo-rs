use rustls::{NoClientAuth, ServerConfig};
use tokio::sync::mpsc::UnboundedSender;
use tokio_rustls::{TlsAcceptor, TlsConnector, rustls::{self, ClientConfig}};
use types::Replica;
use crate::{load_certs_pem, load_private_key_pem, NetMsg};
use std::{
    marker::PhantomData,
    sync::Arc
};
use fnv::FnvHashMap as HashMap;

pub struct TlsClient<I,O>
where I:NetMsg,
O:NetMsg,
{
    pub(crate) peers: HashMap<Replica, UnboundedSender<Arc<O>>>,
    pub(crate) connector: TlsConnector,
    phantom: PhantomData<(I,O)>,
}

impl<I,O> TlsClient<I,O>
where I:NetMsg,
O:NetMsg,
{
    /// Initialize a client manager. `root_cert_path` is an absolute path
    /// to a PEM file produced by `genconfig`.
    pub fn new(root_cert_path: &str) -> Self {
        let mut config = ClientConfig::new();
        for cert in load_certs_pem(root_cert_path) {
            config.root_store.add(&cert)
                .expect("Failed to add the root certificate");
        }

        Self{
            peers: HashMap::default(),
            phantom: PhantomData,
            connector: TlsConnector::from(Arc::new(config)),
        }
    }
}

pub struct Protocol<I,O>
where I:NetMsg,
O:NetMsg,
{
    pub(crate) my_id: Replica,
    pub(crate) num_nodes: Replica,
    pub(crate) cli_acceptor: TlsAcceptor,
    phantom: PhantomData<(I,O)>,
}

impl<I,O> Protocol<I,O>
where I:NetMsg,
O:NetMsg,
{
    /// All three `*_path` args are absolute paths to PEM files.
    pub fn new(
        my_id: Replica,
        num_nodes: Replica,
        _root_cert_path: &str,
        my_cert_path: &str,
        my_priv_key_path: &str,
    ) -> Self {
        let mut config = ServerConfig::new(NoClientAuth::new());
        let cert_chain = load_certs_pem(my_cert_path);
        let my_key = load_private_key_pem(my_priv_key_path);
        config.set_single_cert(cert_chain, my_key).unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        Self{
            phantom: PhantomData,
            my_id,
            num_nodes,
            cli_acceptor: acceptor,
        }
    }
}