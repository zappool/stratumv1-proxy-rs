#[cfg(test)]
mod client_stub;
#[cfg(test)]
mod server_stub;
#[cfg(test)]
mod test_proxy;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::env;
use std::fmt;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum Message {
    Command(CommandMessage),
    Response(ResponseMessage),
}

impl Message {
    pub fn new_command(id: Value, method: String, params: Value) -> Self {
        Self::Command(CommandMessage { id, method, params })
    }

    pub fn new_response(id: Value, error: Value, result: Value) -> Self {
        Self::Response(ResponseMessage { id, error, result })
    }

    pub fn from_json(json: &Value) -> Result<Self> {
        let json_obj = json
            .as_object()
            .ok_or(anyhow!("Error: message should be a JSON object {}", json))?;
        let id = json_obj
            .get("id")
            .ok_or(anyhow!("Error: message should have an ID field {}", json))?;
        let method = json_obj.get("method");
        let params = json_obj.get("params");
        let error = json_obj.get("error");
        let result = json_obj.get("result");
        if method.is_some() && params.is_some() {
            // This is a command
            let method = method
                .unwrap()
                .as_str()
                .ok_or(anyhow!("Method should be a string, {}", json))?
                .to_string();
            Ok(Self::new_command(
                id.clone(),
                method,
                params.unwrap().clone(),
            ))
        } else if error.is_some() && result.is_some() {
            // This is a response
            Ok(Self::new_response(
                id.clone(),
                error.unwrap().clone(),
                result.unwrap().clone(),
            ))
        } else {
            // None
            Err(anyhow!(
                "Could not parse, neither as command nor as response, '{}'",
                json
            ))
        }
    }

    pub fn id(&self) -> String {
        match self {
            Self::Command(cm) => cm.id(),
            Self::Response(rm) => rm.id(),
        }
    }

    pub fn method(&self) -> Option<&String> {
        match self {
            Self::Command(cm) => Some(&cm.method),
            Self::Response(_rm) => None,
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Self::Command(cm) => cm.to_json(),
            Self::Response(rm) => rm.to_json(),
        }
    }

    pub fn to_pretty_string(&self) -> String {
        match self {
            Self::Command(cm) => cm.to_pretty_string(),
            Self::Response(rm) => rm.to_pretty_string(),
        }
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Command(cm) => cm.fmt(f),
            Self::Response(rm) => rm.fmt(f),
        }
    }
}

/// A Stratum V1 command message, with id, method, and custom parameters
#[derive(Clone)]
pub struct CommandMessage {
    pub id: Value,
    pub method: String,
    pub params: Value,
}

impl CommandMessage {
    fn id(&self) -> String {
        self.id.to_string()
    }

    fn to_json(&self) -> Value {
        json![{
            "id": self.id,
            "method": self.method,
            "params": self.params,
        }]
    }

    pub fn to_pretty_string(&self) -> String {
        // Pretty-print JSON
        if let Ok(pretty) = serde_json::to_string_pretty(&self.params) {
            format!("{} {} {}", self.id, self.method, pretty)
        } else {
            // coudln't pretty-print json
            self.to_string()
        }
    }
}

impl std::fmt::Display for CommandMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.id, self.method, self.params)
    }
}

/// A Stratum V1 response message, with id, error, and result parameters
#[derive(Clone)]
pub struct ResponseMessage {
    pub error: Value,
    pub id: Value,
    pub result: Value,
}

impl ResponseMessage {
    fn id(&self) -> String {
        self.id.to_string()
    }

    fn to_json(&self) -> Value {
        json![{
            "error": self.error,
            "id": self.id,
            "result": self.result,
        }]
    }

    pub fn to_pretty_string(&self) -> String {
        // Pretty-print JSON
        if let Ok(pretty) = serde_json::to_string_pretty(&self.result) {
            format!("{} {} {}", self.id, self.error, pretty)
        } else {
            // coudln't pretty-print json
            self.to_string()
        }
    }
}

impl std::fmt::Display for ResponseMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.id, self.error, self.result)
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
    fn process_command(
        &self,
        dir: Direction,
        client_addr: std::net::SocketAddr,
        message: &CommandMessage,
    ) -> Result<Option<Value>>;
}

/// A built-in hook that prints out the content of the messages on stdout
struct PrintToStdoutHook {}

impl Hook for PrintToStdoutHook {
    fn process_command(
        &self,
        dir: Direction,
        client_addr: std::net::SocketAddr,
        message: &CommandMessage,
    ) -> Result<Option<Value>> {
        println!(
            "[{}] {:?}: {}",
            client_addr,
            dir,
            message.to_pretty_string(),
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
                Ok(msg) => {
                    match msg {
                        Message::Command(mut cmd) => {
                            // Process hooks in order
                            for h in hooks.read().unwrap().iter() {
                                if let Ok(Some(new_params)) =
                                    h.process_command(direction, client_addr, &cmd)
                                {
                                    cmd.params = new_params;
                                }
                            }
                        }
                        Message::Response(_resp) => {
                            // no-op
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
