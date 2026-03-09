# Stratum V1 Proxy

A Stratum V1 (Bitcoin mining protocol) proxy, written in Rust. This proxy accepts Stratum V1 TCP connections from mining clients and forwards all commands to an upstream Stratum V1 server, while logging all traffic to stdout.

## Features

- Accepts multiple concurrent connections
- Async/await architecture using Tokio for high performance

## Configuration

The proxy can be configured using command-line arguments:

| Argument | Short | Description | Default |
|----------|-------|-------------|---------|
| `--listen` | `-l` | Address and port to listen on | `0.0.0.0:3333` |
| `--upstream` | `-u` | Upstream Stratum V1 server address (host:port) | `127.0.0.1:3334` |

Run `stratumv1-proxy-rs --help` to see all available options.

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
cargo run --release -- --listen 0.0.0.0:3333 --upstream pool.example.com:3333
```

Or using short options:
```bash
cargo run --release -- -l 0.0.0.0:3333 -u pool.example.com:3333
```

### Running the compiled binary
```bash
# With defaults
./target/release/stratumv1-proxy-rs

# With custom configuration
./target/release/stratumv1-proxy-rs --upstream pool.example.com:3333

# Or with both options
./target/release/stratumv1-proxy-rs -l 0.0.0.0:3333 -u pool.example.com:3333
```

## Usage Example

1. Start the proxy pointing to your upstream Stratum V1 server:
```bash
cargo run --release -- --upstream stratum.pool.com:3333
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

## Development

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

This project available under MIT License.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.
