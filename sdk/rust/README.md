# drive9 Rust SDK

Simple Rust client for drive9 API.

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
drive9 = { path = "sdk/rust" }
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use drive9::Drive9Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize client (get API key from 'drive9 create' or console)
    let client = Drive9Client::new(
        "https://api.drive9.ai",
        "your-api-key"
    );

    // Write file
    client.write("/data/hello.txt", b"Hello, drive9!".to_vec()).await?;
    println!("✓ Written");

    // Read file
    let data = client.read_text("/data/hello.txt").await?;
    println!("✓ Read: {}", data);

    // List directory
    let entries = client.list("/data/").await?;
    println!("✓ Found {} entries", entries.len());
    for e in entries {
        println!("  - {} ({} bytes)", e.name, e.size);
    }

    // Copy (zero-copy)
    client.copy("/data/hello.txt", "/data/hello-copy.txt").await?;
    println!("✓ Copied");

    // Rename
    client.rename("/data/hello-copy.txt", "/data/hello-renamed.txt").await?;
    println!("✓ Renamed");

    // Delete
    client.delete("/data/hello-renamed.txt").await?;
    println!("✓ Deleted");

    Ok(())
}
```

## Features

- Async/await support
- Write/Read files (auto-handles small/large files)
- List directories
- Stat metadata
- Copy/Rename (zero-copy, metadata only)
- Delete
- Mkdir

## Running Example

```bash
cd sdk/rust
cargo run --example basic
```
