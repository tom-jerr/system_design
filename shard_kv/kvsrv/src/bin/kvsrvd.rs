use kvsrv::server::KvServer;
use std::io::{self, Write};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let address = args.next().ok_or("usage: kvsrvd ADDRESS LEASE_TTL_MS")?;
    let ttl_ms: u64 = args
        .next()
        .ok_or("usage: kvsrvd ADDRESS LEASE_TTL_MS")?
        .parse()?;
    if args.next().is_some() {
        return Err("usage: kvsrvd ADDRESS LEASE_TTL_MS".into());
    }
    let mut server = KvServer::start(address, Duration::from_millis(ttl_ms)).await?;
    println!("LISTENING {}", server.local_addr());
    io::stdout().flush()?;
    tokio::select! {
        result = server.wait() => result?,
        result = tokio::signal::ctrl_c() => {
            result?;
            server.shutdown().await?;
        }
    }
    Ok(())
}
