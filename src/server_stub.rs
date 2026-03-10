use crate::{Message, ResponseMessage};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

pub struct MessageStore<M>
where
    M: Clone,
{
    rec_msgs: HashMap<String, M>,
}

impl<M> MessageStore<M>
where
    M: Clone,
{
    pub fn new() -> Self {
        Self {
            rec_msgs: HashMap::new(),
        }
    }
    pub fn add(&mut self, id: String, message: &M) {
        self.rec_msgs.insert(id, message.clone());
    }
    pub fn count(&self) -> usize {
        self.rec_msgs.len()
    }
    pub fn get(&self, id: &str) -> Option<M> {
        self.rec_msgs.get(&id.to_string()).cloned()
    }
}

/// A Stratum V1 server stub for testing
#[derive(Clone)]
pub struct ServerStub {
    listen_addr: String,
    stop_signal: Arc<Notify>,
    listener: Arc<RwLock<Option<TcpListener>>>,
    /// Background thread for processing incoming connections
    bg_thread: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Received connections count
    connect_count: Arc<RwLock<usize>>,
    /// Received messages
    message_store: Arc<RwLock<MessageStore<Message>>>,
}

impl ServerStub {
    pub fn new(listen_addr: &str) -> Self {
        Self {
            listen_addr: listen_addr.to_string(),
            stop_signal: Arc::new(Notify::new()),
            listener: Arc::new(RwLock::new(None)),
            bg_thread: Arc::new(RwLock::new(None)),
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
        println!("Server Stub: Waiting for completion of run thread");
        if let Some(bg_thread) = self.bg_thread.write().await.take() {
            let _ = bg_thread.await?;
        }
        // Unlisten
        let _ = self.unlisten().await?;
        println!("Server Stub: bg thread completed, un-listened, stopping.");
        Ok(())
    }

    async fn listen(&self) -> Result<()> {
        println!("=== Stratum V1 Server Stub ===");

        if self.listener.read().await.is_some() {
            return Err(anyhow!("Already listening!"));
        }

        let listener = Some(
            TcpListener::bind(&self.listen_addr)
                .await
                .context("Failed to bind to listen address")?,
        );
        *self.listener.write().await = listener;

        println!("Listening on: {}", self.listen_addr);
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

        println!("Stopped listening, un-bound from port");
        Ok(())
    }

    async fn run_background(&self) -> Result<()> {
        if self.listener.read().await.is_none() {
            return Err(anyhow!("Not listening!"));
        }
        println!("Server Stub waiting for connections ...");

        loop {
            let listener = self.listener.read().await;
            if listener.is_none() {
                return Err(anyhow!("Not listening!"));
            }
            tokio::select! {
                _ = self.stop_signal.notified() => {
                    println!("Server Stub: received stop signal");
                    break;
                }
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
            }
        }
        println!("Server Stub stopped");
        Ok(())
    }

    /// Handle a single client connection
    async fn handle_client(
        client_socket: TcpStream,
        message_store: Arc<RwLock<MessageStore<Message>>>,
    ) -> Result<()> {
        let client_addr = client_socket.peer_addr()?;

        // Split sockets into read and write halves
        let (client_reader, mut client_writer) = client_socket.into_split();

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
                            println!("[{}]: {}", client_addr, msg.to_string());
                            // Store the message
                            message_store.write().await.add(msg.id(), &msg);
                            // Send reply
                            if let Ok(Some(response)) = Self::handle_message(&msg) {
                                let _ = Self::send_response(&mut client_writer, &response).await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(message: &Message) -> Result<Option<ResponseMessage>> {
        match message.method.as_str() {
            "mining.configure" => {
                let mut result = serde_json::map::Map::<String, Value>::new();
                if let Some(params) = message.params.as_array() {
                    for p in params {
                        if p.is_array() {
                            for pp in p.as_array().unwrap().iter() {
                                if pp.to_string() == "version-rolling" {
                                    result.insert("version.rolling".to_string(), Value::Bool(true));
                                }
                            }
                        } else if p.is_object() {
                            let pp = &p.as_object().unwrap();
                            if let Some(_mask) = pp.get("version-rolling.mask") {
                                result
                                    .insert("version-rolling.mask".to_string(), "1fffe000".into());
                            }
                        }
                    }
                }
                Ok(Some(ResponseMessage {
                    error: Value::Null,
                    id: message.id.clone(),
                    result: Value::Object(result),
                }))
            }
            "mining.subscribe" => {
                let result = json![[[["mining.notify", "6a92c32a"]], "2ef38e6a", 8]];
                Ok(Some(ResponseMessage {
                    error: Value::Null,
                    id: message.id.clone(),
                    result,
                }))
            }
            "mining.authorize" => {
                let result = json![true];
                Ok(Some(ResponseMessage {
                    error: Value::Null,
                    id: message.id.clone(),
                    result,
                }))
            }
            "mining.submit" => {
                let result = json![true];
                Ok(Some(ResponseMessage {
                    error: Value::Null,
                    id: message.id.clone(),
                    result,
                }))
            }
            &_ => Ok(None),
        }
    }

    async fn send_response<W>(writer: &mut W, message: &ResponseMessage) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let msg = json![{
            "id": message.id,
            "error": message.error,
            "result": message.result,
        }]
        .to_string()
            + "\n";
        writer
            .write_all(msg.as_bytes())
            .await
            .context("Failed to write message")?;
        writer.flush().await.context("Failed to flush")?;
        println!(
            "Sent response, id {}, error  {}, result {:?}",
            message.id,
            message.error,
            message.result.to_string()
        );
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
