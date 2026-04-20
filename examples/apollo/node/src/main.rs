use clap::{load_yaml, App};
use config::{ClientId, Node};
use fnv::FnvHashMap;
use libmempool::batcher::Batcher;
use libmempool::{Config as MempoolConfig, Mempool};
use libstorage::rocksdb::Storage as RocksStore;
use net_common::{CertSource, TlsOptions};
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tcp_sender::TcpSimpleSender;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::unbounded_channel;
use types::apollo::{ClientMsg, ProtocolMsg, Replica, Round, Transaction};
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

    config
        .validate()
        .expect("The decoded config is not valid");
    if let Some(f) = m.value_of("ip") {
        config.update_config(util::io::file_to_ips(f.to_string()));
    }
    let config = config;
    let is_client_apollo_enabled = m.is_present("special_client");

    simple_logger::SimpleLogger::new().init().unwrap();
    let x = m.occurrences_of("debug");
    match x {
        0 => log::set_max_level(log::LevelFilter::Info),
        1 => log::set_max_level(log::LevelFilter::Debug),
        2 | _ => log::set_max_level(log::LevelFilter::Trace),
    }

    log::info!("Successfully decoded the config file");
    log::info!("Using special apollo client: {}", is_client_apollo_enabled);

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

        let mut mempool_peer_map: FnvHashMap<Replica, SocketAddr> = FnvHashMap::default();
        for (&id, addr) in &config.mempool_net_map {
            if id == config.id {
                continue;
            }
            mempool_peer_map.insert(
                id,
                addr.parse()
                    .unwrap_or_else(|_| panic!("invalid mempool addr for {}: {}", id, addr)),
            );
        }
        let mempool_sender = TcpSimpleSender::with_peers(mempool_peer_map);

        let (tx_consensus_to_mem, rx_consensus_to_mem) =
            unbounded_channel::<libmempool::ConsensusMempoolMsg<Replica, Round, Transaction>>();
        let (tx_batcher, rx_batcher) = unbounded_channel();
        let (tx_processor, rx_processor) = unbounded_channel();
        let (tx_mem_to_consensus, rx_mem_to_consensus) = unbounded_channel();

        Batcher::spawn(
            rx_batcher,
            tx_processor.clone(),
            CountSealer::<Transaction>::new(config.block_size),
        );

        let all_ids: Vec<Replica> = (0..config.num_nodes as Replica).collect();
        let mempool_params = MempoolConfig::<Round> {
            gc_depth: 50,
            sync_retry_delay: Duration::from_millis(100),
            sync_retry_nodes: 3,
        };

        let mempool_addr: SocketAddr = config
            .mempool_ip()
            .parse()
            .expect("failed to parse mempool listen addr");
        let client_addr: SocketAddr = config
            .client_ip()
            .parse()
            .expect("failed to parse mempool client-facing addr");

        Mempool::<Replica, Round, RocksStore, Transaction>::spawn(
            config.id,
            all_ids,
            mempool_params,
            batch_store.clone(),
            mempool_sender,
            rx_consensus_to_mem,
            tx_batcher,
            tx_processor,
            rx_processor,
            tx_mem_to_consensus,
            mempool_addr,
            client_addr,
        );

        let sleep_time = unsafe { config::SLEEP_TIME };
        tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;

        apollo::node::reactor(
            &config,
            is_client_apollo_enabled,
            consensus_net,
            consensus_recv,
            client_net,
            batch_store,
            rx_mem_to_consensus,
            tx_consensus_to_mem,
        )
        .await;
    });
    Ok(())
}
