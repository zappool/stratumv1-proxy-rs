use crate::server_stub::MessageStore;
use crate::ResponseMessage;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

/// A Stratum V1 client stub for testing.
#[derive(Clone)]
pub struct ClientStub {
    server_addr: String,
    username: String,
    stop_signal: Arc<Notify>,
    read_socket: Arc<RwLock<Option<OwnedReadHalf>>>,
    write_socket: Arc<RwLock<Option<OwnedWriteHalf>>>,
    id_counter: u32,
    /// Background thread for reading from the socket
    bg_thread: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Bytes read (from server)
    read_count: Arc<RwLock<usize>>,
    /// Received messages
    message_store: Arc<RwLock<MessageStore<ResponseMessage>>>,
}

impl ClientStub {
    pub fn new(server_addr: &str, username: &str) -> Self {
        Self {
            server_addr: server_addr.to_string(),
            username: username.to_string(),
            stop_signal: Arc::new(Notify::new()),
            read_socket: Arc::new(RwLock::new(None)),
            write_socket: Arc::new(RwLock::new(None)),
            id_counter: 0,
            bg_thread: Arc::new(RwLock::new(None)),
            read_count: Arc::new(RwLock::new(0)),
            message_store: Arc::new(RwLock::new(MessageStore::new())),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let socket = TcpStream::connect(&self.server_addr)
            .await
            .context("Failed to connect to upstream server")?;
        let (read_socket, write_socket) = socket.into_split();
        *self.read_socket.write().await = Some(read_socket);
        *self.write_socket.write().await = Some(write_socket);
        let self_clone = self.clone();
        let thread = tokio::spawn(async move {
            let _ = self_clone.run_background().await.unwrap();
        });
        *self.bg_thread.write().await = Some(thread);
        Ok(())
    }

    /// Stop the background thread, do a clean shutdown, close socket.
    /// wait_for_read: It can happen that stop is so fast that reading did
    /// not have a chance to run. If this is set to true, stop() will wait until at
    /// least on read has been received (from the server).
    pub async fn stop(&self, wait_for_read: bool) -> Result<()> {
        if wait_for_read {
            while self.get_read_count().await == 0 {
                let _ = tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        self.stop_signal.notify_one();
        // Wait until completion
        println!("Client Stub: Waiting for completion of run thread");
        if let Some(bg_thread) = self.bg_thread.write().await.take() {
            let _ = bg_thread.await?;
        }

        // Close socket
        let write_socket = self.write_socket.write().await.take();
        if let Some(mut write_socket) = write_socket {
            let _ = write_socket.shutdown().await;
        } else {
            // not connected, no-op
        }
        let _read_socket = self.read_socket.write().await.take();
        // no need to close read half, enough for the write half
        println!("Client Stub: bg thread completed, socket closed, stopping.");
        Ok(())
    }

    async fn run_background(&self) -> Result<()> {
        let mut read_socket_lock = self.read_socket.write().await;
        match read_socket_lock.as_mut() {
            None => {
                eprintln!("Client Stub: ERROR Not connected");
            }
            Some(read_socket) => {
                println!("Client Stub reading the socket ...");

                // Read contents
                let mut buf_reader = BufReader::new(read_socket);
                let mut line = String::new();

                loop {
                    line.clear();

                    tokio::select! {
                            _ = self.stop_signal.notified() => {
                                println!("Client Stub: received stop signal");
                                break;
                            }
                        read_res = buf_reader
                            .read_line(&mut line)
                             => {
                                    let bytes_read = read_res.context("Failed to read line")?;
                                    let read_so_far = *self.read_count.read().await + bytes_read;
                                    *self.read_count.write().await = read_so_far;

                                    if bytes_read == 0 {
                                        // Connection closed
                                        break;
                                    }

                                    // Parse contents into Json.  Note: if messages come fast and are not newline-separated, multiple onec can get appended here
                                    match serde_json::from_str::<Value>(&line) {
                                        Err(err) => {
                                            // Couldn't parse into Json
                                            eprintln!("Client Stub: ERROR: couldn't parse: {} {}", err, line);
                                        }
                                        Ok(json) => {
                                            match ResponseMessage::from_json(&json) {
                                                Err(err) => {
                                                    eprintln!("Client Stub: ERROR: couldn't parse: {} {}", err, line);
                                                }
                                                Ok(resp) => {
                                                    println!("Client Stub: Read response: {}", resp.to_string());
                                                    // Store the message
                                                    self.message_store.write().await.add(resp.id(), &resp);
                                                }
                                            }
                                        }
                            }
                        }
                    }
                }
            }
        }

        // println!("Server Stub stopped {}", self.stop_flag.read().await);
        println!("Client Stub stopped");
        Ok(())
    }

    pub async fn send_message(&mut self, method: String, params: Value) -> Result<()> {
        let mut write_socket_lock = self.write_socket.write().await;
        match write_socket_lock.as_mut() {
            None => Err(anyhow!("Not connected")),
            Some(write_socket) => {
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
                write_socket
                    .write_all(msg.as_bytes())
                    .await
                    .context("Failed to write message")?;
                write_socket.flush().await.context("Failed to flush")?;
                println!(
                    "Client Stub: Sent message, id {}, method {}, params {:?}",
                    id,
                    method,
                    params.to_string()
                );
                Ok(())
            }
        }
    }

    async fn get_read_count(&self) -> usize {
        *self.read_count.read().await
    }

    pub async fn get_message_count(&self) -> usize {
        self.message_store.read().await.count()
    }

    pub async fn get_message(&self, id: &str) -> Option<ResponseMessage> {
        self.message_store.read().await.get(id)
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

    pub async fn send_mining_submit(&mut self) -> Result<()> {
        let params = json![[
            self.username.clone(),
            "699f6b4c00008ff1",
            "010000000090ce3f",
            "69afeeea",
            "7a300274",
            "05eb4000"
        ]];
        self.send_message("mining.submit".to_string(), params).await
    }
}
