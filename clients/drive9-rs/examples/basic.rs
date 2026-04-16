use drive9::Client;

#[tokio::main]
async fn main() -> Result<(), drive9::Drive9Error> {
    // Auto-load config from ~/.drive9/config or use explicit URL.
    let client = Client::default_client();
    // Or: let client = Client::new("http://127.0.0.1:9009", "your-api-key");

    let path = "/examples/hello.txt";

    // Write a file
    client.write(path, b"hello from drive9-rs").await?;
    println!("Wrote {}", path);

    // Read it back
    let data = client.read(path).await?;
    println!("Read back: {}", String::from_utf8_lossy(&data));

    // Stat the file
    let info = client.stat(path).await?;
    println!("Stat: size={} revision={} is_dir={}", info.size, info.revision, info.is_dir);

    // List directory
    let entries = client.list("/examples/").await?;
    println!("Entries in /examples/:");
    for e in entries {
        println!("  {} ({} bytes)", e.name, e.size);
    }

    // Conditional write
    client.write_with_revision(path, b"updated content", info.revision).await?;
    println!("Conditional write succeeded");

    // Copy and rename
    client.copy(path, "/examples/hello-copy.txt").await?;
    client.rename("/examples/hello-copy.txt", "/examples/hello-moved.txt").await?;

    // Clean up
    client.delete(path).await?;
    client.delete("/examples/hello-moved.txt").await?;
    println!("Cleaned up");

    Ok(())
}
