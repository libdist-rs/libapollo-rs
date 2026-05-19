use clap::{load_yaml, App};
use config::Client;
use std::{error::Error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Raise the per-process FD soft limit before opening any sockets.
    // Scalability sweep at n=61 needs >7k FDs per process; macOS default
    // soft limit is 256.
    match fdlimit::raise_fd_limit() {
        Ok(fdlimit::Outcome::LimitRaised { from, to }) => {
            println!("Raised FD limit: {} -> {}", from, to);
        }
        Ok(fdlimit::Outcome::Unsupported) => {
            println!("FD limit raise: unsupported on this platform");
        }
        Err(e) => {
            eprintln!("FD limit raise failed: {e}; continuing with current limit");
        }
    }

    let yaml = load_yaml!("cli.yml");
    let m = App::from_yaml(yaml).get_matches();

    let conf_str = m.value_of("config")
        .expect("unable to convert config file into a string");
    let conf_file = std::path::Path::new(conf_str);
    let str = String::from(conf_str);
    let mut config = match conf_file
        .extension()
        .expect("Unable to get file extension")
        .to_str()
        .expect("Failed to convert the extension into ascii string") 
    {
        "json" => Client::from_json(str),
        "dat" => Client::from_bin(str),
        "toml" => Client::from_toml(str),
        "yaml" => Client::from_yaml(str),
        _ => panic!("Invalid config file extension"),
    };

    simple_logger::SimpleLogger::new().init().unwrap();
    let x = m.occurrences_of("debug");
    match x {
        0 => log::set_max_level(log::LevelFilter::Info),
        1 => log::set_max_level(log::LevelFilter::Debug),
        2 | _ => log::set_max_level(log::LevelFilter::Trace),
    }
    
    log::info!("using log level {}, got input {}", 
        log::max_level(), x);

    config
        .validate()
        .expect("The decoded config is not valid");
    if let Some(f) = m.value_of("ip") {
        config.update_config(util::io::file_to_ips(f.to_string()));
    }
    let config = config;
    let metrics:u64 = m.value_of("metrics").unwrap_or("500000")
        .parse().unwrap();
    let window:usize = m.value_of("window").unwrap_or("1000")
        .parse().unwrap();
    
    apollo::normal_client::start(
        &config, metrics, window).await;
    Ok(())
}
