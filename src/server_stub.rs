use crate::Message;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;

pub struct MessageStore {
    rec_msgs: Vec<(String, Message)>,
}

impl MessageStore {
    pub fn new() -> Self {
        Self {
            rec_msgs: Vec::new(),
        }
    }

    pub fn add(&mut self, id: String, message: &Message) {
        self.rec_msgs.push((id, message.clone()));
    }

    pub fn count(&self) -> usize {
        self.rec_msgs.len()
    }

    pub fn get_by_id(&self, id: &str) -> Option<Message> {
        for (p, m) in &self.rec_msgs {
            if p == id {
                return Some(m.clone());
            }
        }
        None
    }

    pub fn get_by_index(&self, index: usize) -> Option<Message> {
        self.rec_msgs.get(index).map(|(_id, m)| m.clone())
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
    message_store: Arc<RwLock<MessageStore>>,
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
        message_store: Arc<RwLock<MessageStore>>,
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
                            let responses = &Self::handle_message(&msg)?;
                            for resp in responses {
                                let _ = Self::send_message(&mut client_writer, resp).await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_message(message: &Message) -> Result<Vec<Message>> {
        match message {
            Message::Response(_resp) => {
                // no-op
                Ok(Vec::new())
            }
            Message::Command(message) => match message.method.as_str() {
                "mining.configure" => {
                    let mut result = serde_json::map::Map::<String, Value>::new();
                    if let Some(params) = message.params.as_array() {
                        for p in params {
                            if p.is_array() {
                                for pp in p.as_array().unwrap().iter() {
                                    if pp.to_string() == "version-rolling" {
                                        result.insert(
                                            "version.rolling".to_string(),
                                            Value::Bool(true),
                                        );
                                    }
                                }
                            } else if p.is_object() {
                                let pp = &p.as_object().unwrap();
                                if let Some(_mask) = pp.get("version-rolling.mask") {
                                    result.insert(
                                        "version-rolling.mask".to_string(),
                                        "1fffe000".into(),
                                    );
                                }
                            }
                        }
                    }
                    Ok(vec![Message::new_response(
                        message.id.clone(),
                        Value::Null,
                        Value::Object(result),
                    )])
                }
                "mining.subscribe" => {
                    let result = json![[[["mining.notify", "6a92c32a"]], "2ef38e6a", 8]];
                    Ok(vec![Message::new_response(
                        message.id.clone(),
                        Value::Null,
                        result,
                    )])
                }
                "mining.authorize" => {
                    // Send multiple messages, response and command mixed
                    let resp_auth =
                        Message::new_response(message.id.clone(), Value::Null, json![true]);
                    let cmd_diff = Message::new_command(
                        Value::Null, // null id, this is not a response to a command
                        "mining.set_difficulty".to_string(),
                        json![1000],
                    );
                    let cmd_notify = Message::new_command(
                        Value::Null, // null id, this is not a response to a command
                        "mining.notify".to_string(),
                        json![[
                            "699f6b4c00008ff0",
                            "92d2bc49c382a3d9e3c185e744f7f57064f3dff10001c9d50000000000000000",
                            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff310349580e0004cceeaf69047c5a3e190c",
                            "0a636b706f6f6c0d2f62726169696e73736f6c6f2fffffffff03c0edb71200000000160014b52957d98722f1c252ac18bcea4680623dc0ca0c7d1418000000000016001451ed61d2f6aa260cc72cdf743e4e436a82c010270000000000000000266a24aa21a9edce7274d0dcfcf606b67b7c7b9429325d376c59f62f8f9625401dd89dc4574a1000000000",
                            [
                                "324b9df1ea1c71e6842306f15827ef9da0f6faa53b3a5385a21d9bdfe5d9c8e3",
                                "5c0d5750e887c75e9c810daf85ec2d7b5b7f7ec64c1be603f0469e2d8a53bdf2",
                                "b4b7cac8bcdfd3ff1ba1fbf09957cd957f773b82eca843556348164b05031dec",
                                "943329656b6626ed3e141fda81420776e25a01deb83519e459e63d769b40ca47",
                                "b432488e2918b38ff4b5a39850eee546022a810f12ba8fea7fafecefa645034f",
                                "bf825ef1e301a64477e4900cfe308e15e8d76ad3682d52ded916926485a28076",
                                "10bc47103a59b7507e8a5f5ca4598a928b29ad42f55f1a03c83e0cf106f105df",
                                "82a4aff85fd47b3350f60e07bcab0227b8da8a22a62330721d9d98b030360528",
                                "0ab69d017b7b6e98900a2b1dfdea47d315d310e2b5a16a962ac41aca60467dc2",
                                "89cf8af43bd9e590a2d8d0e5f05e662392cb109037aea9f4ba663d5bce2e4c0d",
                                "9080b20802786d0a7cc76d74897aece1dd1ba140861777f6b1f7aa6c7f971804",
                                "5667e303f72c9fecd8d477b0f48c347e68309ccc909926e546034fd8862474b8"
                            ],
                            "20000000",
                            "1701f0cc",
                            "69afeeca",
                            true
                        ]],
                    );
                    Ok(vec![resp_auth, cmd_diff, cmd_notify])
                }
                "mining.submit" => Ok(vec![Message::new_response(
                    message.id.clone(),
                    Value::Null,
                    json![true],
                )]),
                &_ => Ok(Vec::new()),
            },
        }
    }

    async fn send_message<W>(writer: &mut W, message: &Message) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let msg = message.to_json().to_string() + "\n";
        writer
            .write_all(msg.as_bytes())
            .await
            .context("Failed to write message")?;
        writer.flush().await.context("Failed to flush")?;
        println!("Sent response, {}", message.to_string());
        Ok(())
    }

    pub async fn get_connect_count(&self) -> usize {
        *self.connect_count.read().await
    }

    pub async fn get_message_count(&self) -> usize {
        self.message_store.read().await.count()
    }

    pub async fn get_message_by_id(&self, id: &str) -> Option<Message> {
        self.message_store.read().await.get_by_id(id)
    }
}
