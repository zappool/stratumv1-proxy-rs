use anyhow::Result;
use stratumv1_proxy_rs::{ProxyConfig, run_proxy};

#[tokio::main]
async fn main() -> Result<()> {
    let config = ProxyConfig::from_env();
    run_proxy(config).await
}
