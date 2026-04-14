# drive9-rs

Rust SDK for [drive9](https://github.com/mem9-ai/drive9) — an agent-native filesystem with semantic search.

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
drive9 = { path = "../clients/drive9-rs" }
```

Or via git:

```toml
[dependencies]
drive9 = { git = "https://github.com/mem9-ai/drive9.git" }
```

## Quick start

```rust
use drive9::Client;

#[tokio::main]
async fn main() -> Result<(), drive9::Drive9Error> {
    let client = Client::new("http://127.0.0.1:9009", "your-api-key");

    // Write & read
    client.write("/hello.txt", b"hello world").await?;
    let data = client.read("/hello.txt").await?;
    assert_eq!(data, b"hello world");

    // List directory
    let entries = client.list("/").await?;
    for e in entries {
        println!("{} ({} bytes)", e.name, e.size);
    }

    // Stat
    let info = client.stat("/hello.txt").await?;
    println!("size={} revision={}", info.size, info.revision);

    Ok(())
}
```

## Config auto-loading

`Client::default_client()` reads `~/.drive9/config` automatically:

```rust
let client = Client::default_client();
```

Expected config format:

```json
{
  "server": "http://127.0.0.1:9009",
  "current_context": "default",
  "contexts": {
    "default": { "api_key": "d9_..." }
  }
}
```

Environment variables `DRIVE9_SERVER` and `DRIVE9_API_KEY` override values from the config file when set.

## Supported operations

| Operation | Method |
|-----------|--------|
| Write file | `client.write(path, data).await` |
| Conditional write | `client.write_with_revision(path, data, revision).await` |
| Read file | `client.read(path).await` |
| List directory | `client.list(path).await` |
| Stat | `client.stat(path).await` |
| Delete | `client.delete(path).await` |
| Copy | `client.copy(src, dst).await` |
| Rename | `client.rename(old, new).await` |
| Mkdir | `client.mkdir(path).await` |
| SQL query | `client.sql(query).await` |
| Grep | `client.grep(query, prefix, limit).await` |
| Find | `client.find(prefix, params).await` |

### Streaming & multipart

| Operation | Method |
|-----------|--------|
| Stream upload (v2) | `client.write_stream(path, reader, size, progress, revision).await` |
| Resume upload | `client.resume_upload(path, reader, size, progress).await` |
| Read stream | `client.read_stream(path).await` |
| Read range stream | `client.read_stream_range(path, offset, length).await` |
| Stream writer | `client.new_stream_writer(path, total_size)` |

### Patch (partial update)

```rust
client.patch_file(
    path,
    new_size,
    dirty_parts,
    read_part_fn,
    progress_fn,
    part_size,
).await?;
```

### Vault

| Operation | Method |
|-----------|--------|
| Create secret | `client.create_vault_secret(name, fields).await` |
| Update secret | `client.update_vault_secret(name, fields).await` |
| Delete secret | `client.delete_vault_secret(name).await` |
| List secrets | `client.list_vault_secrets().await` |
| Issue token | `client.issue_vault_token(name, ttl, perms).await` |
| Revoke token | `client.revoke_vault_token(token_id).await` |
| Read secret | `client.read_vault_secret(name).await` |
| Read field | `client.read_vault_secret_field(name, field).await` |

## Testing

Unit / integration tests (mocked HTTP):

```bash
cd clients/drive9-rs
cargo test
```

## License

Apache-2.0
