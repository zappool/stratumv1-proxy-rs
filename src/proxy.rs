use crate::{Direction, Hook, Message, ProxyConfig};

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinHandle;

/// Stratum V1 proxy server
/// A Stratum V1 server stub for testing
#[derive(Clone)]
pub struct Proxy {
    config: ProxyConfig,
    stop_signal: Arc<Notify>,
    listener: Arc<RwLock<Option<TcpListener>>>,
    /// Background thread for processing incoming connections
    bg_thread: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Hooks to process passed messages
    hooks: Arc<RwLock<Vec<Box<dyn Hook>>>>,
    /// Received connections count
    connect_count: Arc<RwLock<usize>>,
}

impl Proxy {
    pub fn new(config: ProxyConfig, hooks: Vec<Box<dyn Hook>>) -> Self {
        Self {
            config,
            stop_signal: Arc::new(Notify::new()),
            listener: Arc::new(RwLock::new(None)),
            bg_thread: Arc::new(RwLock::new(None)),
            hooks: Arc::new(RwLock::new(hooks)),
            connect_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Listen, start processing in background.
    pub async fn start(&self) -> Result<()> {
        self.listen().await?;
        let self_clone = self.clone();
        let thread = tokio::spawn(async move {
            self_clone.run_background().await.unwrap();
        });
        *self.bg_thread.write().await = Some(thread);
        Ok(())
    }

    /// Stop the background thread, do a clean shutdown, unlisten.
    /// wait_for_first_connection: It can happen that stop is so fast that accept did
    /// not have a chance to run. If this is set to true, stop() will wait until at
    /// least on connection has been received.
    pub async fn stop(&self, wait_for_first_connection: bool) -> Result<()> {
        if wait_for_first_connection {
            while self.get_connect_count().await == 0 {
                let _ = tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        self.stop_signal.notify_one();
        // Wait until completion
        println!("Proxy: Waiting for completion of run thread");
        if let Some(bg_thread) = self.bg_thread.write().await.take() {
            bg_thread.await?;
        }
        // Unlisten
        self.unlisten().await?;
        println!("Proxy: bg thread completed, un-listened, stopping.");
        Ok(())
    }

    async fn listen(&self) -> Result<()> {
        println!("=== Stratum V1 Proxy ===");

        if self.listener.read().await.is_some() {
            return Err(anyhow!("Already listening!"));
        }

        let listener = Some(
            TcpListener::bind(&self.config.listen_addr)
                .await
                .context("Failed to bind to listen address")?,
        );
        *self.listener.write().await = listener;

        println!("Listening on: {}", self.config.listen_addr);
        println!("==============================");
        Ok(())
    }

    async fn unlisten(&self) -> Result<()> {
        if self.listener.read().await.is_none() {
            // Not listening, no-op
            return Ok(());
        }

        let listener = self.listener.write().await.take().unwrap();
        assert!(self.listener.read().await.is_none());
        std::mem::drop(listener);

        println!("Proxy: Stopped listening, un-bound from port");
        Ok(())
    }

    async fn run_background(&self) -> Result<()> {
        if self.listener.read().await.is_none() {
            return Err(anyhow!("Not listening!"));
        }
        println!("Proxy waiting for client connections ...");

        loop {
            let listener = self.listener.read().await;
            if listener.is_none() {
                return Err(anyhow!("Not listening!"));
            }
            tokio::select! {
                _ = self.stop_signal.notified() => {
                    println!("Proxy: received stop signal");
                    break;
                }
                accept_result = listener.as_ref().unwrap().accept() => {
                    match accept_result {
                        Ok((client_socket, client_addr)) => {
                            let count = *self.connect_count.read().await + 1;
                            *self.connect_count.write().await = count;
                            println!("[NEW CONNECTION] Client connected from: {}", client_addr);
                            let config_clone = self.config.clone();
                            let hooks_clone = self.hooks.clone();
                            let thread = tokio::spawn(async move {
                                if let Err(e) = Self::handle_client(client_socket, config_clone, hooks_clone).await {
                                    eprintln!("[ERROR] Client {} error: {}", client_addr, e);
                                }
                                println!("[DISCONNECTED] Client {} disconnected", client_addr);
                            });
                            // TODO Change this: we wait on this thread here
                            // This way no multiple clients can be handled
                            // Proper way would be to store these and join at the end
                            let _ = thread.await;
                        }
                        Err(e) => {
                            eprintln!("[ERROR] Failed to accept connection: {}", e);
                        }
                    }
                }
            }
        }
        println!("Proxy stopped");
        Ok(())
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
            Self::forward_messages(
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
            Self::forward_messages(
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
            let data_to_write = if let Ok(json) = serde_json::from_str::<Value>(line.trim()) {
                match Message::from_json(&json) {
                    Err(err) => {
                        eprintln!(
                            "[{}] {:?}: ERROR: couldn't parse, hooks not called: {} {}",
                            client_addr, direction, err, line
                        );
                        line.clone()
                    }
                    Ok(msg) => {
                        match msg {
                            Message::Command(mut cmd) => {
                                // Process hooks in order
                                for h in hooks.read().await.iter() {
                                    if let Ok(Some(new_params)) =
                                        h.process_command(direction, client_addr, &cmd).await
                                    {
                                        cmd.params = new_params;
                                    }
                                }
                                // Send processed command
                                cmd.to_json().to_string() + "\n"
                            }
                            Message::Response(resp) => {
                                // Process hooks in order
                                for h in hooks.read().await.iter() {
                                    h.process_response(direction, client_addr, &resp).await
                                }
                                // Send the orignal (no change)
                                line.clone()
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
                line.clone()
            };

            // Forward the message (including newline)
            let mut writer_guard = writer.lock().await;
            writer_guard
                .write_all(data_to_write.as_bytes())
                .await
                .context("Failed to write to destination")?;
            writer_guard.flush().await.context("Failed to flush")?;
        }

        Ok(())
    }

    pub async fn get_connect_count(&self) -> usize {
        *self.connect_count.read().await
    }
}
