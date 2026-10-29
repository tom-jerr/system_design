use mr_rust_compat::worker::run_worker;
use std::env;
use std::process;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 2 {
        eprintln!("Usage: mrworker APP SOCKET");
        process::exit(2);
    }
    if let Err(err) = run_worker(&args[0], &args[1]).await {
        eprintln!("mrworker: {err}");
        process::exit(1);
    }
}
