#[cfg(test)]
mod server_stub;
#[cfg(test)]
mod test_proxy;

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::env;
use std::fmt;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// A Stratum V1 message, with id, method, and custom parameters
#[derive(Clone)]
pub struct Message {
    pub id: String,
    pub method: String,
    pub params: Value,
}

impl Message {
    pub fn from_json(json: &Value) -> Result<Self> {
        let json_obj = json
            .as_object()
            .ok_or(anyhow!("Error: message should be a JSON object {}", json))?;
        let id = json_obj
            .get("id")
            .ok_or(anyhow!("Error: message should have an ID field {}", json))?;
        let method = json_obj.get("method").ok_or(anyhow!(
            "Error: message should have a METHOD field {}",
            json
        ))?;
        let method_str = method.as_str().ok_or(anyhow!(
            "Error: message should have a METHOD string field {}",
            method
        ))?;
        let params = json_obj.get("params").ok_or(anyhow!(
            "Error: message should have a PARAMS field {}",
            json
        ))?;
        Ok(Self {
            id: id.to_string(),
            method: method_str.to_string(),
            params: params.clone(),
        })
    }

    pub fn to_prerry_string(&self) -> String {
        // Pretty-print JSON
        if let Ok(pretty) = serde_json::to_string_pretty(&self.params) {
            format!("{} {} {}", self.id, self.method, pretty)
        } else {
            // coudln't pretty-print json
            self.to_string()
        }
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.id, self.method, self.params)
    }
}

#[derive(Clone, Copy)]
pub enum Direction {
    // #[debug("Client->Upstream")]
    ClientToUpstream,

    // #[debug("Upstream->Client")]
    UpstreamToClient,
}

impl fmt::Debug for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::ClientToUpstream => write!(f, "Client->Upstream"),
            Direction::UpstreamToClient => write!(f, "Upstream->Client"),
        }
    }
}

/// Configuration for the Stratum V1 proxy
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub listen_addr: String,
    pub upstream_addr: String,
}

/// Trait for (optional) hooks
pub trait Hook: Send + Sync {
    /// Hook to use/modify the contents of a message, before forwarding.
    /// Contents presented as Json.
    /// Return modified params (if modified)
    fn process(
        &self,
        dir: Direction,
        client_addr: std::net::SocketAddr,
        message: &Message,
    ) -> Result<Option<Value>>;
}

/// A built-in hook that prints out the content of the messages on stdout
struct PrintToStdoutHook {}

impl Hook for PrintToStdoutHook {
    fn process(
        &self,
        dir: Direction,
        client_addr: std::net::SocketAddr,
        message: &Message,
    ) -> Result<Option<Value>> {
        println!(
            "[{}] {:?}: {}",
            client_addr,
            dir,
            message.to_prerry_string(),
        );
        Ok(None)
    }
}

pub fn default_hooks() -> Arc<RwLock<Vec<Box<dyn Hook>>>> {
    Arc::new(RwLock::new(vec![Box::new(PrintToStdoutHook {})]))
}

impl ProxyConfig {
    /// Load configuration from environment variables or use defaults
    pub fn from_env() -> Self {
        let listen_addr =
            env::var("PROXY_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3333".to_string());
        let upstream_addr =
            env::var("UPSTREAM_ADDR").unwrap_or_else(|_| "127.0.0.1:3334".to_string());

        ProxyConfig {
            listen_addr,
            upstream_addr,
        }
    }

    /// Create a new configuration with explicit values
    pub fn new(listen_addr: String, upstream_addr: String) -> Self {
        ProxyConfig {
            listen_addr,
            upstream_addr,
        }
    }
}

/// Run the Stratum V1 proxy server with the given configuration
pub async fn run_proxy(config: ProxyConfig, hooks: Arc<RwLock<Vec<Box<dyn Hook>>>>) -> Result<()> {
    println!("=== Stratum V1 Proxy ===");
    println!("Listening on: {}", config.listen_addr);
    println!("Upstream: {}", config.upstream_addr);
    println!("========================\n");

    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .context("Failed to bind to listen address")?;

    println!("Proxy server started, waiting for connections...\n");

    loop {
        match listener.accept().await {
            Ok((client_socket, client_addr)) => {
                println!("[NEW CONNECTION] Client connected from: {}", client_addr);
                let config = config.clone();
                let hooks_clone = hooks.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_client(client_socket, config, hooks_clone).await {
                        eprintln!("[ERROR] Client {} error: {}", client_addr, e);
                    }
                    println!("[DISCONNECTED] Client {} disconnected", client_addr);
                });
            }
            Err(e) => {
                eprintln!("[ERROR] Failed to accept connection: {}", e);
            }
        }
    }
}

/// Handle a single client connection
async fn handle_client(
    client_socket: TcpStream,
    config: ProxyConfig,
    hooks: Arc<RwLock<Vec<Box<dyn Hook>>>>,
) -> Result<()> {
    let client_addr = client_socket.peer_addr()?;

    // Connect to upstream server
    println!(
        "[{}] Connecting to upstream: {}",
        client_addr, config.upstream_addr
    );

    let upstream_socket = TcpStream::connect(&config.upstream_addr)
        .await
        .context("Failed to connect to upstream server")?;

    println!("[{}] Connected to upstream server", client_addr);

    // Split sockets into read and write halves
    let (client_reader, client_writer) = client_socket.into_split();
    let (upstream_reader, upstream_writer) = upstream_socket.into_split();

    // Wrap writers in Arc<Mutex> for shared access
    let client_writer = Arc::new(Mutex::new(client_writer));
    let upstream_writer = Arc::new(Mutex::new(upstream_writer));

    // Spawn task to forward client -> upstream
    let upstream_writer_clone = upstream_writer.clone();
    let hooks_clone = hooks.clone();
    let client_to_upstream = tokio::spawn(async move {
        forward_messages(
            client_reader,
            upstream_writer_clone,
            client_addr,
            Direction::ClientToUpstream,
            hooks_clone,
        )
        .await
    });

    // Spawn task to forward upstream -> client
    let client_writer_clone = client_writer.clone();
    let upstream_to_client = tokio::spawn(async move {
        forward_messages(
            upstream_reader,
            client_writer_clone,
            client_addr,
            Direction::UpstreamToClient,
            hooks,
        )
        .await
    });

    // Wait for either direction to complete (which means connection closed)
    tokio::select! {
        result = client_to_upstream => {
            if let Err(e) = result {
                eprintln!("[{}] Client->Upstream task error: {}", client_addr, e);
            }
        }
        result = upstream_to_client => {
            if let Err(e) = result {
                eprintln!("[{}] Upstream->Client task error: {}", client_addr, e);
            }
        }
    }

    Ok(())
}

/// Forward messages from reader to writer, printing them to stdout
async fn forward_messages<R, W>(
    reader: R,
    writer: Arc<Mutex<W>>,
    client_addr: std::net::SocketAddr,
    direction: Direction,
    hooks: Arc<RwLock<Vec<Box<dyn Hook>>>>,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = buf_reader
            .read_line(&mut line)
            .await
            .context("Failed to read line")?;

        if bytes_read == 0 {
            // Connection closed
            break;
        }

        // Prepare to process hooks

        // Parse contents into Json.  Note: if messages come fast and are not newline-separated, multiple onec can get appended here
        if let Ok(json) = serde_json::from_str::<Value>(line.trim()) {
            match Message::from_json(&json) {
                Err(err) => {
                    eprintln!(
                        "[{}] {:?}: ERROR: couldn't parse, hooks not called: {} {}",
                        client_addr, direction, err, line
                    );
                }
                Ok(mut msg) => {
                    // Process hooks in order
                    for h in hooks.read().unwrap().iter() {
                        if let Ok(Some(new_params)) = h.process(direction, client_addr, &msg) {
                            msg.params = new_params;
                        }
                    }
                }
            }
        } else {
            // Couldn't parse into Json, can't call hooks
            eprintln!(
                "[{}] {:?}: ERROR: couldn't parse, hooks not called: {}",
                client_addr, direction, line
            );
        }

        // Forward the message (including newline)
        let mut writer_guard = writer.lock().await;
        writer_guard
            .write_all(line.as_bytes())
            .await
            .context("Failed to write to destination")?;
        writer_guard.flush().await.context("Failed to flush")?;
    }

    Ok(())
}
