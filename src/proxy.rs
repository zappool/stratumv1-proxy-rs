use crate::{Direction, Hook, Message, ProxyConfig};

use anyhow::{Context, Result};
use serde_json::Value;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

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
