// A tool that builds config files for all the nodes and the clients for the
// protocol.

use libcrypto::{ed25519, secp256k1};
use config::{Node, Client};
use clap::{load_yaml, App};
use types::Replica;
use libcrypto::Algorithm;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use util::io::*;
use openssl::{asn1::Asn1Time, bn::{BigNum, MsbOption}, error::ErrorStack, hash::MessageDigest, pkey::{PKey, PKeyRef, Private}, rsa::Rsa, x509::{X509, X509NameBuilder, X509Ref, X509Req, X509ReqBuilder, extension::{AuthorityKeyIdentifier, BasicConstraints, KeyUsage, SubjectAlternativeName, SubjectKeyIdentifier}}};
use fnv::FnvHashMap as HashMap;

/// Write `bytes` to `{target}/{name}` and return the absolute path as a String.
fn write_cert_file(target_dir: &Path, name: &str, bytes: &[u8]) -> Result<String, Box<dyn Error>> {
    let path: PathBuf = target_dir.join(name);
    fs::write(&path, bytes)?;
    let abs = fs::canonicalize(&path)?;
    Ok(abs.to_string_lossy().into_owned())
}

fn new_root_cert() -> Result<(X509, PKey<Private>), ErrorStack> {
    let rsa = Rsa::generate(2048)?;
    let privkey = PKey::from_rsa(rsa)?;

    let mut x509_name = X509NameBuilder::new()?;
    x509_name.append_entry_by_text("C", "US")?;
    x509_name.append_entry_by_text("ST", "IN")?;
    x509_name.append_entry_by_text("O", "Libchatter Test")?;
    x509_name.append_entry_by_text("CN", "Root")?;
    let x509_name = x509_name.build();

    let mut cert_builder = X509::builder()?;
    cert_builder.set_version(2)?;
    let serial_number = {
        let mut serial = BigNum::new()?;
        serial.rand(159, MsbOption::MAYBE_ZERO, false)?;
        serial.to_asn1_integer()?
    };
    cert_builder.set_serial_number(&serial_number)?;
    cert_builder.set_subject_name(&x509_name)?;
    cert_builder.set_issuer_name(&x509_name)?;
    cert_builder.set_pubkey(&privkey)?;
    let not_before = Asn1Time::days_from_now(0)?;
    cert_builder.set_not_before(&not_before)?;
    let not_after = Asn1Time::days_from_now(365)?;
    cert_builder.set_not_after(&not_after)?;

    cert_builder.append_extension(BasicConstraints::new().critical().ca().build()?)?;
    cert_builder.append_extension(
        KeyUsage::new()
            .critical()
            .key_cert_sign()
            .crl_sign()
            .build()?,
    )?;

    let subject_key_identifier =
        SubjectKeyIdentifier::new().build(&cert_builder.x509v3_context(None, None))?;
    cert_builder.append_extension(subject_key_identifier)?;

    cert_builder.sign(&privkey, MessageDigest::sha256())?;
    let cert = cert_builder.build();

    Ok((cert, privkey))
}

/// Make a X509 request with the given private key
fn mk_request(privkey: &PKey<Private>) -> Result<X509Req, ErrorStack> {
    let mut req_builder = X509ReqBuilder::new()?;
    req_builder.set_pubkey(&privkey)?;

    let mut x509_name = X509NameBuilder::new()?;
    x509_name.append_entry_by_text("C", "US")?;
    x509_name.append_entry_by_text("ST", "IN")?;
    x509_name.append_entry_by_text("O", "Nodes")?;
    x509_name.append_entry_by_text("CN", "nodes.com")?;
    let x509_name = x509_name.build();
    req_builder.set_subject_name(&x509_name)?;

    req_builder.sign(&privkey, MessageDigest::sha256())?;
    let req = req_builder.build();
    Ok(req)
}

/// Make a certificate and private key signed by the given CA cert and private key
fn get_signed_cert(
    ca_cert: &X509Ref,
    ca_privkey: &PKeyRef<Private>,
    extra_ip_sans: &[String],
) -> Result<(X509, PKey<Private>), ErrorStack> {
    let rsa = Rsa::generate(2048)?;
    let privkey = PKey::from_rsa(rsa)?;

    let req = mk_request(&privkey)?;

    let mut cert_builder = X509::builder()?;
    cert_builder.set_version(2)?;
    let serial_number = {
        let mut serial = BigNum::new()?;
        serial.rand(159, MsbOption::MAYBE_ZERO, false)?;
        serial.to_asn1_integer()?
    };
    cert_builder.set_serial_number(&serial_number)?;
    cert_builder.set_subject_name(req.subject_name())?;
    cert_builder.set_issuer_name(ca_cert.subject_name())?;
    cert_builder.set_pubkey(&privkey)?;
    let not_before = Asn1Time::days_from_now(0)?;
    cert_builder.set_not_before(&not_before)?;
    let not_after = Asn1Time::days_from_now(365)?;
    cert_builder.set_not_after(&not_after)?;

    cert_builder.append_extension(BasicConstraints::new().build()?)?;

    cert_builder.append_extension(
        KeyUsage::new()
            .critical()
            .non_repudiation()
            .digital_signature()
            .key_encipherment()
            .build()?,
    )?;

    let subject_key_identifier =
        SubjectKeyIdentifier::new().build(&cert_builder.x509v3_context(Some(ca_cert), None))?;
    cert_builder.append_extension(subject_key_identifier)?;

    let auth_key_identifier = AuthorityKeyIdentifier::new()
        .keyid(false)
        .issuer(false)
        .build(&cert_builder.x509v3_context(Some(ca_cert), None))?;
    cert_builder.append_extension(auth_key_identifier)?;

    // SANs cover the SNI values libnet-rs derives at connect time:
    // loopback addresses resolve to "localhost"; non-loopback IPs
    // pass through as IP SANs. Always include 127.0.0.1 + localhost
    // + nodes.com for the local / fallback case, and append any
    // `extra_ip_sans` the caller knows about (populated via
    // `--node_ips` / `--client_ips` for multi-VM runs so the peer's
    // real address matches the cert).
    let mut san_builder = SubjectAlternativeName::new();
    san_builder.dns("localhost");
    san_builder.dns("nodes.com");
    san_builder.ip("127.0.0.1");
    for ip in extra_ip_sans {
        san_builder.ip(ip);
    }
    let subject_alt_name =
        san_builder.build(&cert_builder.x509v3_context(Some(ca_cert), None))?;
    cert_builder.append_extension(subject_alt_name)?;

    cert_builder.sign(&ca_privkey, MessageDigest::sha256())?;
    let cert = cert_builder.build();

    Ok((cert, privkey))
}

fn main() -> Result<(), Box<dyn Error>> {
    let yaml = load_yaml!("cli.yml");
    let m = App::from_yaml(yaml).get_matches();
    let num_nodes:usize =  m.value_of("num_nodes")
        .expect("number of nodes not specified")
        .parse::<usize>()
        .expect("unable to convert number of nodes into a number");
    let num_faults:usize = match m.value_of("num_faults") {
        Some(x) => x.parse::<usize>()
            .expect("unable to convert number of faults into a number"),
        None => (num_nodes-1)/2,
    };
    let delay:u64 = m.value_of("delay")
        .expect("delay value not specified")
        .parse::<u64>()
        .expect("unable to parse delay value into a number");
    let base_port: u16 = m.value_of("base_port")
        .expect("base_port value not specified")
        .parse::<u16>()
        .expect("failed to parse base_port into a number");
    let blocksize: usize = m.value_of("block_size")
        .expect("no block_size specified")
        .parse::<usize>()
        .expect("unable to convert blocksize into a number");
    let client_base_port:u16 = m.value_of("client_base_port")
        .expect("no client_base_port specified")
        .parse::<u16>()
        .expect("unable to parse client_base_port into an integer");
    let t:Algorithm = m.value_of("algorithm")
        .unwrap_or("ED25519")
        .parse::<Algorithm>()
        .unwrap_or(Algorithm::ED25519);
    let out = m.value_of("out_type")
        .unwrap_or("json");
    let target = m.value_of("target")
        .expect("target directory for the config not specified");
    let payload:usize = m.value_of("payload")
        .unwrap_or("0")
        .parse()
        .unwrap();
    let client_listen_port:u16 = m.value_of("client_listen_port")
        .expect("client_listen_port value not specified")
        .parse::<u16>()
        .expect("failed to parse client_listen_port into an integer");
    let mempool_base_port:u16 = m.value_of("mempool_base_port")
        .expect("mempool_base_port value not specified")
        .parse::<u16>()
        .expect("failed to parse mempool_base_port into an integer");

    // Parse `--node_ips` / `--client_ips` comma-separated lists into
    // IP SANs for the per-node / per-client certs. Empty list leaves
    // certs localhost-only (pre-existing behaviour).
    fn parse_ip_list(spec: Option<&str>) -> Vec<String> {
        spec.map(|s| s.split(',')
                     .map(|x| x.trim().to_string())
                     .filter(|x| !x.is_empty())
                     .collect())
            .unwrap_or_default()
    }
    let node_ip_sans: Vec<String> = parse_ip_list(m.value_of("node_ips"));
    let client_ip_sans: Vec<String> = parse_ip_list(m.value_of("client_ips"));

    let mut client = Client::new();
    client.block_size = blocksize;
    client.crypto_alg = t.clone();
    client.num_nodes = num_nodes;
    client.num_faults = num_faults;

    let mut node:Vec<Node> = Vec::with_capacity(num_nodes);

    let mut pk = HashMap::default();
    let mut ip = HashMap::default();
    let mut mempool_ip: HashMap<Replica, String> = HashMap::default();

    let (cert, privkey) = new_root_cert()?;

    // Write the root cert once; every node and the client references the
    // same path.
    let target_path = Path::new(target);
    fs::create_dir_all(target_path)?;
    let root_cert_pem = cert.to_pem()?;
    let root_cert_path = write_cert_file(target_path, "root-cert.pem", &root_cert_pem)?;

    for i in 0..num_nodes {
        node.push(Node::new());

        node[i].delta = delay;
        node[i].id = i as Replica;
        node[i].num_nodes = num_nodes;
        node[i].num_faults = num_faults;
        node[i].block_size = blocksize;
        node[i].payload = payload;
        node[i].client_port = client_base_port+(i as u16);

        node[i].mempool_port = mempool_base_port + (i as u16);

        node[i].crypto_alg = t.clone();
        match t {
            Algorithm::ED25519 => {
                let kp = ed25519::Keypair::generate().expect("ed25519 keypair generation");
                pk.insert(i as Replica, bincode::serialize(&kp.public()).expect("serialize pub"));
                node[i].secret_key_bytes = bincode::serialize(&kp).expect("serialize kp");
            }
            Algorithm::SECP256K1 => {
                let kp = secp256k1::Keypair::generate();
                pk.insert(i as Replica, bincode::serialize(kp.public()).expect("serialize pub"));
                node[i].secret_key_bytes = bincode::serialize(&kp).expect("serialize kp");
            }
            _ => (),
        };
        ip.insert(i as Replica,
        format!("{}:{}", "127.0.0.1", base_port+(i as u16))
        );
        mempool_ip.insert(i as Replica,
        format!("{}:{}", "127.0.0.1", mempool_base_port+(i as u16))
        );
        // The client submits transactions to each node's mempool
        // (plain TCP), so net_map on the client side points at
        // `client_base_port+i`, not the mempool peer port.
        client.net_map.insert(i as Replica,
        format!("127.0.0.1:{}", client_base_port+(i as u16))
        );

        let (new_cert, new_pkey) = get_signed_cert(&cert, &privkey, &node_ip_sans)?;

        // Write a chain PEM containing `[leaf, root CA]` in that order.
        // libnet-rs uses this file for both the server identity and the
        // client trust store -- having the root CA inline lets peers
        // (which libnet-rs verifies against our trust store) validate
        // cleanly, while presenting `[leaf, root]` is a standard TLS
        // server chain for self-signed roots. The vendored net reads
        // the chain as the server identity and resolves trust from its
        // own `root_cert_path`, so both code paths stay happy.
        let mut chain_pem = new_cert.to_pem()?;
        chain_pem.extend_from_slice(&root_cert_pem);
        let node_key_pem = new_pkey.private_key_to_pem_pkcs8()?;
        node[i].root_cert_path = root_cert_path.clone();
        node[i].my_cert_path = write_cert_file(target_path, &format!("node-{}.chain.pem", i), &chain_pem)?;
        node[i].my_cert_key_path = write_cert_file(target_path, &format!("node-{}.key.pem", i), &node_key_pem)?;
    }

    // Generate one client identity for the stress-test topology. Its
    // address is registered in every node's `client_net_map` so nodes
    // can push committed `ClientMsg` back.
    let client_id: u16 = 0;
    let client_addr = format!("127.0.0.1:{}", client_listen_port);
    let (client_cert, client_pkey) = get_signed_cert(&cert, &privkey, &client_ip_sans)?;
    let mut client_chain_pem = client_cert.to_pem()?;
    client_chain_pem.extend_from_slice(&root_cert_pem);
    let client_key_pem = client_pkey.private_key_to_pem_pkcs8()?;
    let client_cert_path = write_cert_file(target_path, "client-0.chain.pem", &client_chain_pem)?;
    let client_key_path = write_cert_file(target_path, "client-0.key.pem", &client_key_pem)?;

    let mut client_net_map: HashMap<u16, String> = HashMap::default();
    client_net_map.insert(client_id, client_addr.clone());

    client.my_id = client_id;
    client.my_listen_addr = client_addr;
    client.my_cert_path = client_cert_path;
    client.my_cert_key_path = client_key_path;
    client.root_cert_path = root_cert_path;

    for i in 0..num_nodes {
        node[i].pk_map = pk.clone();
        node[i].net_map = ip.clone();
        node[i].mempool_net_map = mempool_ip.clone();
        node[i].client_net_map = client_net_map.clone();
    }

    client.server_pk = pk;

    // Write all the files
    for i in 0..num_nodes {
        match out {
            "json" => {
                let filename = format!("{}/nodes-{}.json",target,i);
                write_json(filename, &node[i]);
            },
            "binary" => {
                let filename = format!("{}/nodes-{}.dat",target,i);
                write_bin(filename, &node[i]);
            },
            "toml" => {
                let filename = format!("{}/nodes-{}.toml",target,i);
                write_toml(filename, &node[i]);
            },
            "yaml" => {
                let filename = format!("{}/nodes-{}.yml",target,i);
                write_yaml(filename, &node[i]);
            },
            _ => (),
        }
        node[i].validate()
            .expect("failed to validate node config");
    }

    // Write the client file
    match out {
        "json" => {
            let filename = format!("{}/client.json",target);
            write_json(filename, &client);
        },
        "binary" => {
            let filename = format!("{}/client.dat",target);
            write_bin(filename, &client);
        },
        "toml" => {
            let filename = format!("{}/client.toml",target);
            write_toml(filename, &client);
        },
        "yaml" => {
            let filename = format!("{}/client.yml",target);
            write_yaml(filename, &client);
        },
        _ => (),
    }
    client.validate()
        .expect("failed to validate the client config");

    Ok(())
}

#[test]
fn test_codec() -> Result<(), Box<dyn Error>>{
    use rustls::{Certificate, ClientConfig};

    let (cert, _key) = new_root_cert()?;
    let data = cert.to_der()?;
    let ok = Certificate(data);
    let mut config = ClientConfig::new();
    config.root_store.add(&ok)?;
    Ok(())
}