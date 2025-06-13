# Skylight - Bluesky Social Graph Analyzer

## Commands
- **Build**: `cargo build`
- **Run**: `cargo run`
- **Test**: `cargo test`
- **Check**: `cargo check`

## Architecture
- **Core system**: Bluesky/ATProto network crawler and analyzer
- **Components**: Account crawler, processing pipeline, DID resolver, PDS manager
- **Data flow**: Crawls PDS endpoints → downloads repos → processes CAR files → extracts social graph data
- **Modules**: `main.rs` (coordinator), `pds.rs` (PDS management), `did.rs` (DID resolution), `processor.rs` (repo processing), `utils.rs` (auth/logging)

## Code Style
- Always use `cargo add` for dependencies (per Claude.md)
- Use `anyhow::Result` for error handling
- Use `tracing` for logging (info!, error!, warn!)
- Use `tokio` for async operations
- Use `Arc` and `Mutex` for shared state between workers
- Import style: Standard library → external crates → local modules
- No redundant type annotations when they can be inferred
- Use structured logging with context
