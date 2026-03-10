use crate::Message;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

struct MessageStore {
    rec_msgs: HashMap<String, Message>,
}

impl MessageStore {
    pub fn new() -> Self {
        Self {
            rec_msgs: HashMap::new(),
        }
    }
    pub fn add(&mut self, message: &Message) {
        self.rec_msgs
            .insert(message.id.to_string(), message.clone());
    }
    pub fn count(&self) -> usize {
        self.rec_msgs.len()
    }
    pub fn get(&self, id: &str) -> Option<Message> {
        self.rec_msgs.get(&id.to_string()).cloned()
    }
}

/// A Stratum V1 server stub for testing
#[derive(Clone)]
pub struct ServerStub {
    listen_addr: String,
    stop_flag: Arc<RwLock<bool>>,
    stop_signal: Arc<Notify>,
    listener: Arc<RwLock<Option<TcpListener>>>,
    run_thread: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Received connections count
    connect_count: Arc<RwLock<usize>>,
    /// Received messages
    message_store: Arc<RwLock<MessageStore>>,
}

impl ServerStub {
    pub fn new(listen_addr: &str) -> Self {
        Self {
            listen_addr: listen_addr.to_string(),
            stop_flag: Arc::new(RwLock::new(false)),
            stop_signal: Arc::new(Notify::new()),
            listener: Arc::new(RwLock::new(None)),
            run_thread: Arc::new(RwLock::new(None)),
            connect_count: Arc::new(RwLock::new(0)),
            message_store: Arc::new(RwLock::new(MessageStore::new())),
        }
    }

    /// Listen, start processing in background.
    pub async fn start(&self) -> Result<()> {
        let _ = self.listen().await?;
        let self_clone = self.clone();
        let thread = tokio::spawn(async move {
            let _ = self_clone.run_background().await.unwrap();
        });
        *self.run_thread.write().await.deref_mut() = Some(thread);
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
        *self.stop_flag.write().await.deref_mut() = true;
        self.stop_signal.notify_one();
        // Wait until completion
        println!("Waiting for completion of run thread");
        if let Some(run_thread) = self.run_thread.write().await.take() {
            let _ = run_thread.await?;
        }
        // Unlisten
        let _ = self.unlisten().await?;
        println!("Run thread completed, un-listened, stopping.");
        Ok(())
    }

    pub async fn listen(&self) -> Result<()> {
        println!("=== Stratum V1 Server Stub ===");

        if self.listener.read().await.is_some() {
            return Err(anyhow!("Already listening!").into());
        }

        let listener = Some(
            TcpListener::bind(&self.listen_addr)
                .await
                .context("Failed to bind to listen address")?,
        );
        *self.listener.write().await.deref_mut() = listener;

        println!("Listening on: {}", self.listen_addr);
        println!("==============================");
        Ok(())
    }

    pub async fn unlisten(&self) -> Result<()> {
        if self.listener.read().await.is_none() {
            // Not listening, no-op
            return Ok(());
        }

        let listener = self.listener.write().await.take().unwrap();
        assert!(self.listener.read().await.is_none());
        std::mem::drop(listener);

        println!("Stopped listening, un-bound from port");
        Ok(())
    }

    pub async fn run_background(&self) -> Result<()> {
        if self.listener.read().await.is_none() {
            return Err(anyhow!("Not listening!").into());
        }
        println!("Server Stub waiting for connections ...");

        // while !(self.stop_flag.read().await.deref()) {
        loop {
            let listener = self.listener.read().await;
            if listener.is_none() {
                return Err(anyhow!("Not listening!"));
            }
            tokio::select! {
                accept_result = listener.as_ref().unwrap().accept() => {
                    match accept_result {
                        Ok((client_socket, client_addr)) => {
                            let count = *self.connect_count.read().await + 1;
                            *self.connect_count.write().await = count;
                            println!("[NEW CONNECTION] Client connected from: {}", client_addr);
                            let store = self.message_store.clone();
                            let thread = tokio::spawn(async move {
                                if let Err(e) = Self::handle_client(client_socket, store).await {
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
                _ = self.stop_signal.notified() => {
                    println!("Server Stub received stop signal {}", self.stop_flag.read().await);
                    break;
                }
            }
        }
        println!("Server Stub stopped {}", self.stop_flag.read().await);
        Ok(())
    }

    /// Handle a single client connection
    async fn handle_client(
        client_socket: TcpStream,
        message_store: Arc<RwLock<MessageStore>>,
    ) -> Result<()> {
        let client_addr = client_socket.peer_addr()?;

        // Split sockets into read and write halves
        let (client_reader, _client_writer) = client_socket.into_split();

        // Read contents
        let mut buf_reader = BufReader::new(client_reader);
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

            // Parse contents into Json.  Note: if messages come fast and are not newline-separated, multiple onec can get appended here
            match serde_json::from_str::<Value>(&line) {
                Err(err) => {
                    // Couldn't parse into Json
                    eprintln!("[{}]: ERROR: couldn't parse: {} {}", client_addr, err, line);
                }
                Ok(json) => {
                    match Message::from_json(&json) {
                        Err(err) => {
                            eprintln!("[{}]: ERROR: couldn't parse: {} {}", client_addr, err, line);
                        }
                        Ok(msg) => {
                            // Store the message
                            message_store.write().await.add(&msg);
                            println!("[{}]: {}", client_addr, msg.to_string());
                        }
                    }
                }
            }
        }

        // TODO send replies

        Ok(())
    }

    pub async fn get_connect_count(&self) -> usize {
        *self.connect_count.read().await
    }

    pub async fn get_message_count(&self) -> usize {
        self.message_store.read().await.count()
    }

    pub async fn get_message(&self, id: &str) -> Option<Message> {
        self.message_store.read().await.get(id)
    }
}

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

    pub async fn connect(&mut self) -> Result<()> {
        let socket = TcpStream::connect(&self.server_addr)
            .await
            .context("Failed to connect to upstream server")?;
        self.socket = Some(socket);
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<()> {
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
