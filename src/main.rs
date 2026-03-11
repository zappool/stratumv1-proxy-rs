use anyhow::Result;
use clap::Parser;
use stratumv1_proxy_rs::{default_hooks, Proxy, ProxyConfig};

/// Stratum V1 Proxy - forwards mining protocol traffic between clients and upstream server
#[derive(Parser, Debug)]
#[command(name = "stratumv1-proxy-rs")]
#[command(version, about, long_about = None)]
struct Args {
    /// Address and port to listen on
    #[arg(short, long, default_value = "0.0.0.0:3333")]
    listen: String,

    /// Upstream Stratum V1 server address (host:port)
    #[arg(short, long, default_value = "127.0.0.1:3334")]
    upstream: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = ProxyConfig::new(args.listen, args.upstream);
    let proxy = Proxy::new(config, default_hooks());
    proxy.start().await?;
    // Keep the process, never exit
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await
    }
    // proxy.stop(true).await?;
    // Ok(())
}
