use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// A Stratum V1 client stub for testing.
pub struct ClientStub {
    server_addr: String,
    username: String,
    socket: Option<TcpStream>,
    id_counter: u32,
}

impl ClientStub {
    pub fn new(server_addr: &str, username: &str) -> Self {
        Self {
            server_addr: server_addr.to_string(),
            username: username.to_string(),
            socket: None,
            id_counter: 0,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let socket = TcpStream::connect(&self.server_addr)
            .await
            .context("Failed to connect to upstream server")?;
        self.socket = Some(socket);
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(socket) = &mut self.socket {
            let _ = socket.shutdown().await;
            self.socket = None;
            Ok(())
        } else {
            // not connected, no-op
            Ok(())
        }
    }

    pub async fn send_message(&mut self, method: String, params: Value) -> Result<()> {
        if let Some(socket) = &mut self.socket {
            // Forward the message (including newline)
            self.id_counter += 1;
            let id = self.id_counter;
            let msg = json![{
                "id": id,
                "method": method,
                "params": params,
            }]
            .to_string()
                + "\n";
            socket
                .write_all(msg.as_bytes())
                .await
                .context("Failed to write message")?;
            socket.flush().await.context("Failed to flush")?;
            println!(
                "Sent message, id {}, method {}, params {:?}",
                id,
                method,
                params.to_string()
            );
            Ok(())
        } else {
            Err(anyhow!("Not connected"))
        }
    }

    pub async fn send_mining_configure(&mut self) -> Result<()> {
        let params = json![[["version-rolling"],{"version-rolling.mask": "ffffffff"}]];
        self.send_message("mining.configure".to_string(), params)
            .await
    }

    pub async fn send_mining_subscribe(&mut self) -> Result<()> {
        let params = json![["bitaxe/BM1368/v2.8.1"]];
        self.send_message("mining.subscribe".to_string(), params)
            .await
    }

    pub async fn send_mining_authorize(&mut self) -> Result<()> {
        let params = json![[self.username, "password"]];
        self.send_message("mining.authorize".to_string(), params)
            .await
    }

    pub async fn send_mining_suggest_difficulty(&mut self, difficluty: u64) -> Result<()> {
        let params = json![[difficluty]];
        self.send_message("mining.suggest_difficulty".to_string(), params)
            .await
    }
}
