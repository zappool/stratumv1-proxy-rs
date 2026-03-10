#[cfg(test)]
mod client_stub;
mod proxy;
#[cfg(test)]
mod server_stub;
#[cfg(test)]
mod test_proxy;

pub use proxy::run_proxy;

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::env;
use std::fmt;
use std::sync::{Arc, RwLock};

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
        if let Some(method) = method {
            if let Some(params) = params {
                // This is a command
                let method = method
                    .as_str()
                    .ok_or(anyhow!("Method should be a string, {}", json))?
                    .to_string();
                return Ok(Self::new_command(id.clone(), method, params.clone()));
            }
        }
        if let Some(result) = result {
            if let Some(error) = error {
                // This is a response
                return Ok(Self::new_response(
                    id.clone(),
                    error.clone(),
                    result.clone(),
                ));
            }
        }
        // None
        Err(anyhow!(
            "Could not parse, neither as command nor as response, '{}'",
            json
        ))
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
