# Stratum V1 Proxy

A high-performance Stratum V1 proxy written in Rust for Bitcoin mining. This proxy accepts Stratum V1 TCP connections from mining clients and forwards all commands to an upstream Stratum V1 server, while logging all traffic to stdout.

## Features

- ✅ Accepts multiple concurrent Stratum V1 TCP connections
- ✅ Forwards all commands unmodified to upstream server
- ✅ Bidirectional message forwarding (client ↔ upstream)
- ✅ Real-time logging of all Stratum V1 commands to stdout
- ✅ Pretty-printed JSON output for better readability
- ✅ Configurable via environment variables
- ✅ Async/await architecture using Tokio for high performance

## Configuration

The proxy can be configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `PROXY_LISTEN_ADDR` | Address and port to listen on | `0.0.0.0:3333` |
| `UPSTREAM_ADDR` | Upstream Stratum V1 server address (host:port) | `127.0.0.1:3334` |

## Building

```bash
cargo build --release
```

## Running

### Using default configuration
```bash
cargo run --release
```

### Using custom configuration
```bash
PROXY_LISTEN_ADDR="0.0.0.0:3333" \
UPSTREAM_ADDR="pool.example.com:3333" \
cargo run --release
```

### Running the compiled binary
```bash
# With defaults
./target/release/stratumv1-proxy-rs

# With custom configuration
UPSTREAM_ADDR="pool.example.com:3333" ./target/release/stratumv1-proxy-rs
```

## Usage Example

1. Start the proxy pointing to your upstream Stratum V1 server:
```bash
UPSTREAM_ADDR="stratum.pool.com:3333" cargo run --release
```

2. Configure your mining software to connect to the proxy:
```bash
# Example with cpuminer
cpuminer -a sha256d -o stratum+tcp://localhost:3333 -u username -p password
```

3. Watch the proxy log all Stratum V1 messages in real-time:
```
=== Stratum V1 Proxy ===
Listening on: 0.0.0.0:3333
Upstream: stratum.pool.com:3333
========================

Proxy server started, waiting for connections...

[NEW CONNECTION] Client connected from: 127.0.0.1:54321
[127.0.0.1:54321] Connecting to upstream: stratum.pool.com:3333
[127.0.0.1:54321] Connected to upstream server
[127.0.0.1:54321] CLIENT -> UPSTREAM: {"id":1,"method":"mining.subscribe","params":[]}
  └─ Parsed: {
       "id": 1,
       "method": "mining.subscribe",
       "params": []
     }
[127.0.0.1:54321] UPSTREAM -> CLIENT: {"id":1,"result":[[["mining.notify","..."]],"...",4],"error":null}
...
```

## Stratum V1 Protocol

The proxy handles all standard Stratum V1 mining protocol messages, including:

- `mining.subscribe` - Subscribe to mining notifications
- `mining.authorize` - Authorize a worker
- `mining.submit` - Submit a share
- `mining.notify` - Mining job notification (from server)
- `mining.set_difficulty` - Difficulty adjustment (from server)
- And all other Stratum V1 commands

All messages are forwarded unmodified, ensuring full protocol compatibility.

## Architecture

The proxy uses Tokio's async runtime for efficient handling of multiple concurrent connections:

1. **Main Loop**: Accepts incoming client connections
2. **Per-Client Handler**: For each client:
   - Establishes connection to upstream server
   - Spawns two async tasks:
     - Client → Upstream forwarding
     - Upstream → Client forwarding
3. **Message Forwarding**: Each task reads line-delimited JSON messages, logs them, and forwards them

## Development

### Dependencies

- `tokio` - Async runtime with full features
- `serde` & `serde_json` - JSON parsing and serialization
- `anyhow` - Error handling

### Testing

To test the proxy without a real mining setup, you can use `netcat` or `telnet`:

```bash
# Terminal 1: Start the proxy
cargo run --release

# Terminal 2: Connect as a client
nc localhost 3333
# Then type Stratum V1 commands:
{"id":1,"method":"mining.subscribe","params":[]}
```

## License

This project is open source and available under your chosen license.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.
