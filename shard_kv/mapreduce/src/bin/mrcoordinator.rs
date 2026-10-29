use mr_rust_compat::master::MasterServer;
use std::env;
use std::process;
use std::time::Duration;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("mrcoordinator: {err}");
        process::exit(1);
    }
}

async fn run() -> std::io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 {
        eprintln!("Usage: mrcoordinator SOCKET INPUT...");
        process::exit(2);
    }
    let n_reduce = env::var("MR_N_REDUCE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let lease = env::var("MR_TASK_LEASE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let server = MasterServer::start_with_lease(
        &args[0],
        args[1..].to_vec(),
        n_reduce,
        Duration::from_millis(lease),
    )
    .await?;
    server.wait().await?;
    Ok(())
}
