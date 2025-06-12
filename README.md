# Skylight - Bluesky Social Graph Visualizer

Real-time visualization of the Bluesky social graph using D3.js.

## Features
- Crawls Bluesky repositories and extracts social graph data
- Streams updates via WebSocket without blocking the main processing pipeline
- Interactive D3.js force-directed graph visualization
- Real-time updates as new repositories are processed

## Usage

1. Set your Bluesky credentials:
```bash
export BSKY_USERNAME="your-username"
export BSKY_PASSWORD="your-password"
```

2. Run the application:
```bash
cargo run
```

3. Open http://127.0.0.1:3000 in your browser to view the social graph

## Visualization Controls
- Drag nodes to reposition them
- Scroll to zoom in/out
- Green edges represent "follows" relationships
- Blue nodes represent users