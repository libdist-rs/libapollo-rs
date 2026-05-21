// Baseline stress test for the four consensus protocols shipped in
// libchatter-rs: Apollo, Artemis, Sync HotStuff, and Opt Sync.
//
// For each protocol, the harness:
//   1. Shells out to `genconfig` to produce a fresh Node/Client config set
//      (with `--num_clients C`, so each client j gets its own
//      `client-{j}.json` / listen port / cert pair).
//   2. Writes matching ip / cli_ip files for localhost loopback
//   3. Spawns N node binaries as child processes
//   4. Spawns C client binaries in parallel, each driving its own load
//      of `-m total_txs` until that many confirmations arrive
//   5. Parses `DP[Throughput]` / `DP[Latency]` per-client from each
//      client's stdio (simple_logger at INFO, printed by
//      `consensus::statistics`). The server's old per-window
//      `DP[Throughput]` emission has been removed; the canonical
//      number now comes solely from the clients.
//   6. Aggregates: throughput = sum across clients; latency = mean
//      across clients (each client confirms the same `-m total_txs`).
//   7. Kills nodes, reports result, moves on
//
// The output format mirrors libnet-rs's stress-test so the two baselines
// can sit side-by-side in README / CV material. The canonical run is
// captured in `baseline_results.txt` at the repo root.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

type BoxErr = Box<dyn Error + Send + Sync>;

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Apollo,
    Artemis,
    Synchs,
    Optsync,
}

impl Protocol {
    fn short(&self) -> &'static str {
        match self {
            Protocol::Apollo => "apollo",
            Protocol::Artemis => "artemis",
            Protocol::Synchs => "synchs",
            Protocol::Optsync => "optsync",
        }
    }
    fn label(&self) -> &'static str {
        match self {
            Protocol::Apollo => "Apollo",
            Protocol::Artemis => "Artemis",
            Protocol::Synchs => "Sync HotStuff",
            Protocol::Optsync => "Opt Sync",
        }
    }
    // Seconds the nodes wait at startup before entering the protocol loop.
    // Apollo and Artemis gate protocol entry on the `--sleep` arg; Sync HotStuff
    // and Opt Sync gate on `config::SLEEP_TIME`, which the binaries compute from
    // `--sleep` when present. A larger N needs more slack.
    fn bootstrap_secs(num_nodes: usize) -> u64 {
        5 + (num_nodes as u64).max(3)
    }

    // Apollo and Artemis have two client-notification paths: the default
    // (commit-driven) requires round > f before the leader ever tells the
    // client about a block, which deadlocks with a single initial-tx burst
    // because no node has enough txs left in its pool to propose round f+1.
    // The `-s` flag ("special_client") enables the fast path where the leader
    // multicasts proposals to clients immediately. This matches the canonical
    // `scripts/apollo-multi-node-test.sh` behaviour. Sync HotStuff and Opt Sync
    // always multicast blocks to clients, so they don't have this flag.
    fn wants_special_client(&self) -> bool {
        matches!(self, Protocol::Apollo | Protocol::Artemis)
    }
}

#[derive(Clone, Debug)]
struct BenchConfig {
    protocol: Protocol,
    num_nodes: usize,
    num_faults: usize,
    num_clients: u16,
    block_size: usize,
    payload: usize,
    total_txs: u64,
    window: usize,
}

struct BenchResult {
    throughput: f64, // aggregate tx / sec
    throughput_src: &'static str, // "server-median" or "client-sum"
    latency_ms: f64, // mean ms per tx across clients
    per_client: Vec<(Option<f64>, f64)>, // (throughput, latency_ms) per client; throughput is None for apollo/artemis
    wall_elapsed: Duration,
}

struct Harness {
    repo_root: PathBuf,
    runs_dir: PathBuf,
    run_idx: u16,
}

impl Harness {
    fn new(repo_root: PathBuf) -> Result<Self, BoxErr> {
        let runs_dir = repo_root.join("stress-test/runs");
        fs::create_dir_all(&runs_dir)?;
        Ok(Self {
            repo_root,
            runs_dir,
            run_idx: 0,
        })
    }

    // Allocate a non-overlapping port block per run: 300 ports each,
    // starting high enough that we don't collide with typical dev
    // services. Layout inside a run's block:
    //   base..base+n             node-to-node consensus (TLS)
    //   cli_base..cli_base+n     nodes' client-facing TxReceiver (TCP,
    //                            mempool-spawned)
    //   mempool_base..+n         nodes' peer-to-peer mempool sync (TCP)
    //   client_listen..+C        per-client listener for node-pushed
    //                            `ClientMsg` (TCP); client j binds
    //                            client_listen + j (cap C ≤ 25)
    fn alloc_ports(&mut self) -> (u16, u16, u16, u16) {
        let base = 21000 + self.run_idx * 300;
        let cli_base = base + 100;
        let mempool_base = base + 200;
        let client_listen = base + 275;
        self.run_idx += 1;
        (base, cli_base, mempool_base, client_listen)
    }

    async fn run(&mut self, cfg: &BenchConfig) -> Result<BenchResult, BoxErr> {
        let (base_port, cli_base_port, mempool_base_port, client_listen_port) = self.alloc_ports();
        let run_dir = self.runs_dir.join(format!(
            "{}-n{}-c{}-b{}-p{}-{}",
            cfg.protocol.short(),
            cfg.num_nodes,
            cfg.num_clients,
            cfg.block_size,
            cfg.payload,
            base_port
        ));
        if run_dir.exists() {
            fs::remove_dir_all(&run_dir).ok();
        }
        fs::create_dir_all(&run_dir)?;

        genconfig(&self.repo_root, &run_dir, cfg, base_port, cli_base_port, mempool_base_port, client_listen_port).await?;
        write_ip_files(&run_dir, cfg.num_nodes, base_port, cli_base_port)?;

        let bootstrap = Protocol::bootstrap_secs(cfg.num_nodes);
        let mut nodes: Vec<Child> = Vec::with_capacity(cfg.num_nodes);
        for i in 0..cfg.num_nodes {
            nodes.push(spawn_node(&self.repo_root, &run_dir, cfg, i, bootstrap).await?);
        }

        sleep(Duration::from_secs(bootstrap)).await;

        let started = Instant::now();
        let client_out = spawn_clients_and_aggregate(&self.repo_root, &run_dir, cfg).await;
        let wall_elapsed = started.elapsed();

        for mut n in nodes {
            let _ = n.kill().await;
        }
        // Give TCP listeners a moment to release their ports before the next run.
        sleep(Duration::from_millis(500)).await;

        let per_client = client_out?;

        // Throughput source: apollo/artemis clients no longer emit
        // `DP[Throughput]`; the per-second server emission in
        // `node-0.log` is canonical. Synchs/Optsync clients still emit
        // it, so per-client throughput is Some(_) there and we sum.
        let server_med = parse_server_throughput_median(&run_dir.join("node-0.log"));
        let (throughput, throughput_src) = if let Some(m) = server_med {
            (m, "server-median")
        } else {
            let sum: f64 = per_client.iter().filter_map(|(t, _)| *t).sum();
            (sum, "client-sum")
        };

        let latency_ms: f64 = if per_client.is_empty() {
            0.0
        } else {
            per_client.iter().map(|(_, l)| *l).sum::<f64>() / per_client.len() as f64
        };
        Ok(BenchResult {
            throughput,
            throughput_src,
            latency_ms,
            per_client,
            wall_elapsed,
        })
    }
}

/// Read `node-0.log`, parse every `DP[Throughput]: <f>` line emitted
/// by the apollo/artemis reactor's per-second sampler, return their
/// median. `None` if the file is missing or has no such lines (e.g.
/// run shorter than one window, or synchs/optsync nodes which do not
/// emit this line).
fn parse_server_throughput_median(node0_log: &Path) -> Option<f64> {
    let text = fs::read_to_string(node0_log).ok()?;
    let mut vals: Vec<f64> = text
        .lines()
        .filter_map(|l| extract_after(l, "DP[Throughput]: "))
        .collect();
    if vals.is_empty() {
        return None;
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = vals.len() / 2;
    Some(if vals.len() % 2 == 0 {
        (vals[mid - 1] + vals[mid]) / 2.0
    } else {
        vals[mid]
    })
}

async fn genconfig(
    repo_root: &Path,
    run_dir: &Path,
    cfg: &BenchConfig,
    base_port: u16,
    cli_base_port: u16,
    mempool_base_port: u16,
    client_listen_port: u16,
) -> Result<(), BoxErr> {
    let bin = repo_root.join("target/release/genconfig");
    let out = Command::new(&bin)
        .arg("-n")
        .arg(cfg.num_nodes.to_string())
        .arg("-f")
        .arg(cfg.num_faults.to_string())
        .arg("-d")
        .arg("50")
        .arg("--blocksize")
        .arg(cfg.block_size.to_string())
        .arg("--base_port")
        .arg(base_port.to_string())
        .arg("--client_base_port")
        .arg(cli_base_port.to_string())
        .arg("--mempool_base_port")
        .arg(mempool_base_port.to_string())
        .arg("--client_listen_port")
        .arg(client_listen_port.to_string())
        .arg("--num_clients")
        .arg(cfg.num_clients.to_string())
        .arg("--payload")
        .arg(cfg.payload.to_string())
        .arg("--target")
        .arg(run_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!("genconfig exited non-zero: {}", stderr).into());
    }
    Ok(())
}

fn write_ip_files(
    run_dir: &Path,
    n: usize,
    base_port: u16,
    cli_base_port: u16,
) -> Result<(), BoxErr> {
    let mut ip = String::new();
    let mut cli_ip = String::new();
    for i in 0..n {
        ip.push_str(&format!("127.0.0.1:{}\n", base_port as usize + i));
        cli_ip.push_str(&format!("127.0.0.1:{}\n", cli_base_port as usize + i));
    }
    fs::write(run_dir.join("ip_file"), ip)?;
    fs::write(run_dir.join("cli_ip_file"), cli_ip)?;
    Ok(())
}

async fn spawn_node(
    repo_root: &Path,
    run_dir: &Path,
    cfg: &BenchConfig,
    i: usize,
    bootstrap_secs: u64,
) -> Result<Child, BoxErr> {
    let bin = repo_root.join(format!("target/release/node-{}", cfg.protocol.short()));
    let config_file = run_dir.join(format!("nodes-{}.json", i));
    let ip_file = run_dir.join("ip_file");
    let log_path = run_dir.join(format!("node-{}.log", i));
    let stdout_fd = std::fs::File::create(&log_path)?;
    let stderr_fd = stdout_fd.try_clone()?;

    let mut cmd = Command::new(&bin);
    cmd.arg("-c")
        .arg(&config_file)
        .arg("-i")
        .arg(&ip_file)
        .arg("--sleep")
        .arg(bootstrap_secs.to_string())
        .arg("--delta")
        .arg("50");
    if cfg.protocol.wants_special_client() {
        cmd.arg("-s");
    }
    let child = cmd
        .stdout(Stdio::from(stdout_fd))
        .stderr(Stdio::from(stderr_fd))
        .spawn()?;
    Ok(child)
}

/// Spawn `cfg.num_clients` client processes in parallel, parse each
/// one's `DP[Throughput]` / `DP[Latency]` line, and return the per-client
/// pairs. Each client uses its own `client-{j}.json` (minted by
/// genconfig with `--num_clients`) so listen ports, ids, and certs don't
/// collide.
async fn spawn_clients_and_aggregate(
    repo_root: &Path,
    run_dir: &Path,
    cfg: &BenchConfig,
) -> Result<Vec<(Option<f64>, f64)>, BoxErr> {
    let mut tasks = Vec::with_capacity(cfg.num_clients as usize);
    for j in 0..cfg.num_clients {
        let repo_root = repo_root.to_path_buf();
        let run_dir = run_dir.to_path_buf();
        let cfg = cfg.clone();
        tasks.push(tokio::spawn(async move {
            spawn_one_client_and_parse(&repo_root, &run_dir, &cfg, j).await
        }));
    }

    let mut out: Vec<(Option<f64>, f64)> = Vec::with_capacity(tasks.len());
    let mut first_err: Option<BoxErr> = None;
    for (j, t) in tasks.into_iter().enumerate() {
        match t.await {
            Ok(Ok(pair)) => out.push(pair),
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(format!("client {}: {}", j, e).into());
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(format!("client {} task: {}", j, e).into());
                }
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(out)
}

async fn spawn_one_client_and_parse(
    repo_root: &Path,
    run_dir: &Path,
    cfg: &BenchConfig,
    j: u16,
) -> Result<(Option<f64>, f64), BoxErr> {
    let bin = repo_root.join(format!("target/release/client-{}", cfg.protocol.short()));
    let config_file = run_dir.join(format!("client-{}.json", j));
    let cli_ip_file = run_dir.join("cli_ip_file");

    let log_path = run_dir.join(format!("client-{}.log", j));
    let mut child = Command::new(&bin)
        .arg("-c")
        .arg(&config_file)
        .arg("-i")
        .arg(&cli_ip_file)
        .arg("-m")
        .arg(cfg.total_txs.to_string())
        .arg("-w")
        .arg(cfg.window.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stderr = child
        .stderr
        .take()
        .ok_or("client stderr not captured")?;
    let stdout = child
        .stdout
        .take()
        .ok_or("client stdout not captured")?;
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_lines = BufReader::new(stdout).lines();

    // Tee everything the client prints into a per-client log file so
    // post-mortems are possible.
    let mut log = std::fs::File::create(&log_path)?;

    let parse_window = Duration::from_secs(600);
    let mut throughput: Option<f64> = None;
    let mut latency: Option<f64> = None;

    // Read both streams until EOF, taking the LAST-seen value for each
    // DP line. Apollo/Artemis emit `DP[Latency]` per window (leto/zeus
    // convention) plus a final partial-window flush right before
    // `DP[Throughput]`; the last value seen is the steady-state, not
    // the first window's median. Synchs/Optsync still emit once at
    // end-of-run, which this loop also handles correctly.
    let parse = async {
        use std::io::Write;
        let mut stderr_eof = false;
        let mut stdout_eof = false;
        loop {
            tokio::select! {
                line = stderr_lines.next_line(), if !stderr_eof => {
                    match line {
                        Ok(Some(l)) => {
                            let _ = writeln!(log, "[stderr] {}", l);
                            if let Some(v) = extract_after(&l, "DP[Throughput]: ") {
                                throughput = Some(v);
                            }
                            if let Some(v) = extract_after(&l, "DP[Latency]: ") {
                                latency = Some(v);
                            }
                        }
                        Ok(None) | Err(_) => { stderr_eof = true; }
                    }
                }
                line = stdout_lines.next_line(), if !stdout_eof => {
                    match line {
                        Ok(Some(l)) => {
                            let _ = writeln!(log, "[stdout] {}", l);
                            if let Some(v) = extract_after(&l, "DP[Throughput]: ") {
                                throughput = Some(v);
                            }
                            if let Some(v) = extract_after(&l, "DP[Latency]: ") {
                                latency = Some(v);
                            }
                        }
                        Ok(None) | Err(_) => { stdout_eof = true; }
                    }
                }
            }
            if stderr_eof && stdout_eof {
                break;
            }
        }
    };

    match timeout(parse_window, parse).await {
        Ok(()) => {}
        Err(_) => {
            let _ = child.kill().await;
            return Err("client timed out before reporting DP stats".into());
        }
    }

    // `consensus::statistics` prints, then the `start` future returns, then
    // the tokio runtime drops the dangling tx-producer task which calls
    // `std::process::exit(0)`. Kill explicitly in case any of that gets stuck.
    let _ = child.kill().await;

    // `DP[Throughput]` is optional on the client side: apollo/artemis
    // clients no longer emit it (server is the source of truth), but
    // synchs/optsync clients still do via the legacy `statistics()`
    // path. `DP[Latency]` is required from every client.
    match latency {
        Some(l) => Ok((throughput, l)),
        None => Err(format!(
            "client {} produced no DP[Latency] line (check {})",
            j,
            log_path.display()
        )
        .into()),
    }
}

fn extract_after(line: &str, prefix: &str) -> Option<f64> {
    let idx = line.find(prefix)?;
    let rest = &line[idx + prefix.len()..];
    rest.trim().parse::<f64>().ok()
}

// ---- Reporting ----

const BOX_WIDTH: usize = 63;

fn box_line() -> String {
    "─".repeat(BOX_WIDTH)
}

fn print_header(cfg: &BenchConfig) {
    println!();
    println!("┌{}", box_line());
    println!(
        "│ {} (n={}, f={}, clients={}, blk={}, payload={}B, window={}, txs/cli={})",
        cfg.protocol.label(),
        cfg.num_nodes,
        cfg.num_faults,
        cfg.num_clients,
        cfg.block_size,
        cfg.payload,
        cfg.window,
        cfg.total_txs
    );
    println!("├{}", box_line());
}

fn print_result(r: &BenchResult) {
    println!("│ Throughput     : {:>12.2} tx/s  ({})", r.throughput, r.throughput_src);
    println!("│ Avg Latency    : {:>12.2} ms/tx (mean across {} client{})",
        r.latency_ms,
        r.per_client.len(),
        if r.per_client.len() == 1 { "" } else { "s" }
    );
    if r.per_client.len() > 1 {
        for (j, (t, l)) in r.per_client.iter().enumerate() {
            match t {
                Some(t) => println!("│   client {:>2}    : {:>12.2} tx/s   {:>8.2} ms/tx", j, t, l),
                None    => println!("│   client {:>2}    : {:>12}      {:>8.2} ms/tx", j, "—", l),
            }
        }
    }
    println!("│ Wall elapsed   : {:>12.2} s", r.wall_elapsed.as_secs_f64());
    println!("└{}", box_line());
}

fn print_failure(err: &(dyn Error + Send + Sync)) {
    println!("│ FAILED: {}", err);
    println!("└{}", box_line());
}

fn print_summary(results: &[(BenchConfig, Option<BenchResult>)]) {
    let w = 96;
    let line = "─".repeat(w);
    println!();
    println!("┌{}", line);
    println!(
        "│ {:<16} {:>4} {:>4} {:>4} {:>5} {:>7} {:>16} {:>16}",
        "Protocol", "N", "f", "C", "blk", "txs/c", "Throughput", "Latency"
    );
    println!("├{}", line);
    for (c, r) in results {
        match r {
            Some(r) => println!(
                "│ {:<16} {:>4} {:>4} {:>4} {:>5} {:>7} {:>10.2} tx/s {:>11.2} ms",
                c.protocol.label(),
                c.num_nodes,
                c.num_faults,
                c.num_clients,
                c.block_size,
                c.total_txs,
                r.throughput,
                r.latency_ms
            ),
            None => println!(
                "│ {:<16} {:>4} {:>4} {:>4} {:>5} {:>7} {:>16} {:>16}",
                c.protocol.label(),
                c.num_nodes,
                c.num_faults,
                c.num_clients,
                c.block_size,
                c.total_txs,
                "FAILED",
                "FAILED"
            ),
        }
    }
    println!("└{}", line);
}

// ---- Matrix ----

fn build_matrix() -> Vec<BenchConfig> {
    let mut v = Vec::new();
    let only = std::env::var("PROTO").ok();
    let selected: Vec<Protocol> = match only.as_deref() {
        Some("apollo") => vec![Protocol::Apollo],
        Some("artemis") => vec![Protocol::Artemis],
        Some("synchs") => vec![Protocol::Synchs],
        Some("optsync") => vec![Protocol::Optsync],
        _ => vec![
            Protocol::Apollo,
            Protocol::Artemis,
            Protocol::Synchs,
            Protocol::Optsync,
        ],
    };
    let num_clients: u16 = std::env::var("NUM_CLIENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1);
    for protocol in selected {
        for &(n, f) in &[(3usize, 1usize), (7, 3)] {
            v.push(BenchConfig {
                protocol,
                num_nodes: n,
                num_faults: f,
                num_clients,
                block_size: 400,
                payload: 0,
                total_txs: 50_000,
                window: 10_000,
            });
        }
    }
    v
}

#[tokio::main]
async fn main() -> Result<(), BoxErr> {
    let repo_root = std::env::current_dir()?;

    // Sanity-check binaries so the first failure is informative, not a cryptic ENOENT.
    for bin in &[
        "genconfig",
        "node-apollo",
        "client-apollo",
        "node-artemis",
        "client-artemis",
        "node-synchs",
        "client-synchs",
        "node-optsync",
        "client-optsync",
    ] {
        let p = repo_root.join(format!("target/release/{}", bin));
        if !p.exists() {
            return Err(format!(
                "missing target/release/{}. Run `cargo build --release` first.",
                bin
            )
            .into());
        }
    }

    let mut harness = Harness::new(repo_root)?;
    let matrix = build_matrix();

    println!("{:=^63}", " libchatter-rs baseline stress test ");
    println!(
        "Protocols: Apollo, Artemis, Sync HotStuff, Opt Sync.     Loopback (127.0.0.1), release build."
    );
    if let Some(c) = matrix.first() {
        if c.num_clients > 1 {
            println!("Multi-client: {} parallel clients per run", c.num_clients);
        }
    }

    let mut results: Vec<(BenchConfig, Option<BenchResult>)> = Vec::new();
    for cfg in matrix {
        print_header(&cfg);
        match harness.run(&cfg).await {
            Ok(r) => {
                print_result(&r);
                results.push((cfg, Some(r)));
            }
            Err(e) => {
                print_failure(&*e);
                results.push((cfg, None));
            }
        }
    }

    print_summary(&results);
    Ok(())
}
