use clap::{load_yaml, App};
use config::{ClientId, Node};
use fnv::FnvHashMap;
use libmempool::{BatchHash, CachedBatch, ConsensusMempoolMsg, Mempool};
use libstorage::rocksdb::Storage as RocksStore;
use net_common::{CertSource, TlsOptions};
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::unbounded_channel;
use types::optsync::{ClientMsg, Height, ProtocolMsg, Replica, Transaction};
use types::CountSealer;

fn main() -> Result<(), Box<dyn Error>> {
    let yaml = load_yaml!("cli.yml");
    let m = App::from_yaml(yaml).get_matches();

    let conf_str = m
        .value_of("config")
        .expect("unable to convert config file into a string");
    let conf_file = std::path::Path::new(conf_str);
    let str = String::from(conf_str);
    let mut config = match conf_file
        .extension()
        .expect("Unable to get file extension")
        .to_str()
        .expect("Failed to convert the extension into ascii string")
    {
        "json" => Node::from_json(str),
        "dat" => Node::from_bin(str),
        "toml" => Node::from_toml(str),
        "yaml" => Node::from_yaml(str),
        _ => panic!("Invalid config file extension"),
    };
    if let Some(v) = m.value_of("delta") {
        config.delta = v.parse().expect("unexpected delta value provided");
    }

    if let Some(v) = m.value_of("sleep") {
        unsafe {
            config::SLEEP_TIME = v.parse().expect("unexpected sleep time");
        }
    } else {
        unsafe {
            config::SLEEP_TIME = (5 + config.num_nodes) as u64;
        }
    }

    simple_logger::SimpleLogger::new().init().unwrap();
    match m.occurrences_of("debug") {
        0 => log::set_max_level(log::LevelFilter::Info),
        1 => log::set_max_level(log::LevelFilter::Debug),
        2 | _ => log::set_max_level(log::LevelFilter::Trace),
    }

    config
        .validate()
        .expect("The decoded config is not valid");
    if let Some(f) = m.value_of("ip") {
        config.update_config(util::io::file_to_ips(f.to_string()));
    }
    let config = config;

    let rocksdb_path = m
        .value_of("store")
        .map(String::from)
        .unwrap_or_else(|| {
            let parent = conf_file
                .parent()
                .expect("config file has no parent dir");
            parent
                .join(format!("node-{}.rocksdb", config.id))
                .to_string_lossy()
                .into_owned()
        });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let tls = || TlsOptions {
            cert_source: CertSource::PemFiles {
                cert_chain: PathBuf::from(&config.my_cert_path),
                private_key: PathBuf::from(&config.my_cert_key_path),
            },
            ..TlsOptions::high_throughput()
        };

        let batch_store = RocksStore::new(&rocksdb_path).expect("rocksdb store init");

        let consensus_listen: SocketAddr = config
            .my_ip()
            .parse()
            .expect("failed to parse consensus listen addr");
        let consensus_recv = TlsReceiver::<ProtocolMsg>::spawn_with_options(consensus_listen, tls());

        let mut peer_map: FnvHashMap<Replica, SocketAddr> = FnvHashMap::default();
        for (&id, addr) in &config.net_map {
            if id == config.id {
                continue;
            }
            peer_map.insert(
                id,
                addr.parse()
                    .unwrap_or_else(|_| panic!("invalid peer addr for {}: {}", id, addr)),
            );
        }
        let consensus_net = TlsReliableSender::<Replica, ProtocolMsg>::with_peers_and_options(
            peer_map,
            tls(),
        )
        .expect("consensus sender setup");

        let mut client_map: FnvHashMap<ClientId, SocketAddr> = FnvHashMap::default();
        for (&id, addr) in &config.client_net_map {
            client_map.insert(
                id,
                addr.parse()
                    .unwrap_or_else(|_| panic!("invalid client addr for {}: {}", id, addr)),
            );
        }
        let client_net = TlsReliableSender::<ClientId, ClientMsg>::with_peers_and_options(
            client_map,
            tls(),
        )
        .expect("client sender setup");

        // Mempool channels. The mempool->consensus channel carries
        // `(BatchHash, Arc<CachedBatch>)` -- the leader never reads
        // its own batch back out of rocksdb.
        let (tx_consensus_to_mem, rx_consensus_to_mem) =
            unbounded_channel::<ConsensusMempoolMsg<Replica, Height, Transaction>>();
        let (tx_mem_to_consensus, rx_mem_to_consensus) = unbounded_channel::<(
            BatchHash<Transaction>,
            Arc<CachedBatch<Transaction>>,
        )>();

        let client_intake: SocketAddr = config
            .client_ip()
            .parse()
            .expect("failed to parse mempool client-facing addr");

        let mempool = Mempool::<Transaction>::spawn::<
            CountSealer,
            RocksStore,
            Replica,
            Height,
        >(
            CountSealer::new(config.block_size),
            batch_store.clone(),
            client_intake,
            tx_mem_to_consensus,
            rx_consensus_to_mem,
            None,
        );
        let batch_cache = mempool.cache;

        let sleep_time = unsafe { config::SLEEP_TIME };
        tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;

        optsync::node::reactor(
            &config,
            consensus_net,
            consensus_recv,
            client_net,
            batch_store,
            batch_cache,
            rx_mem_to_consensus,
            tx_consensus_to_mem,
        )
        .await;
    });
    Ok(())
}
