use drive9::Client;
use std::io::Cursor;
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> Result<(), drive9::Drive9Error> {
    let client = Client::default_client();
    // Or: let client = Client::new("http://127.0.0.1:9009", "your-api-key");

    let path = "/examples/stream.bin";
    let data: Vec<u8> = (0..1_000_000u64)
        .flat_map(|i| i.to_le_bytes().to_vec())
        .collect();
    let size = data.len() as i64;

    // Stream upload using write_stream (v2 with v1 fallback for small files)
    let reader: Box<dyn drive9::transfer::SeekableReader> = Box::new(Cursor::new(data.clone()));
    client.write_stream(path, reader, size).await?;
    println!("Stream-uploaded {} bytes to {}", size, path);

    // Read back as an async stream
    let mut stream = client.read_stream(path).await?;
    let mut read_buf = Vec::new();
    stream.read_to_end(&mut read_buf).await?;
    println!("Read back {} bytes via stream", read_buf.len());

    // Range read
    let mut range_stream = client.read_stream_range(path, 0, 1024).await?;
    let mut range_buf = Vec::new();
    range_stream.read_to_end(&mut range_buf).await?;
    println!("Range read {} bytes", range_buf.len());

    // StreamWriter (manual part-by-part upload)
    let writer = client.new_stream_writer(path, size);
    let part_size = 256_000usize;
    let mut part_num = 1;
    for chunk in data.chunks(part_size) {
        writer.write_part(part_num, chunk.to_vec()).await?;
        part_num += 1;
    }
    writer.complete(part_num - 1, vec![]).await?;
    println!("Completed via StreamWriter");

    // Clean up
    client.delete(path).await?;
    println!("Cleaned up");

    Ok(())
}
