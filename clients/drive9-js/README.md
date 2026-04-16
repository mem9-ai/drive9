# drive9-js

TypeScript SDK for [drive9](https://github.com/mem9-ai/drive9) — an agent-native filesystem with semantic search.

## Installation

```bash
npm install drive9
```

## Quick start

```typescript
import { Client } from "drive9";

async function main() {
  const client = Client.defaultClient();
  // Or: const client = new Client("http://127.0.0.1:9009", "your-api-key");

  await client.write("/hello.txt", new TextEncoder().encode("hello world"));
  const data = await client.read("/hello.txt");
  console.log(new TextDecoder().decode(data));

  const info = await client.stat("/hello.txt");
  console.log(`size=${info.size} revision=${info.revision}`);
}

main().catch(console.error);
```

## Config auto-loading

`Client.defaultClient()` reads `~/.drive9/config` automatically:

```typescript
const client = Client.defaultClient();
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

Environment variables `DRIVE9_SERVER` and `DRIVE9_API_KEY` take priority over the config file.

## Supported operations

| Operation | Method |
|-----------|--------|
| Write file | `client.write(path, data)` |
| Conditional write | `client.write(path, data, expectedRevision)` |
| Read file | `client.read(path)` |
| List directory | `client.list(path)` |
| Stat | `client.stat(path)` |
| Delete | `client.delete(path)` |
| Copy | `client.copy(src, dst)` |
| Rename | `client.rename(old, new)` |
| Mkdir | `client.mkdir(path)` |
| SQL query | `client.sql(query)` |
| Grep | `client.grep(query, prefix, limit)` |
| Find | `client.find(prefix, params)` |

### Streaming & multipart

| Operation | Method |
|-----------|--------|
| Stream upload | `client.writeStream(path, stream, size, expectedRevision?)` |
| Resume upload | `client.resumeUpload(path, stream, totalSize)` |
| Read stream | `client.readStream(path)` |
| Read range stream | `client.readStreamRange(path, offset, length)` |
| Stream writer | `client.newStreamWriter(path, totalSize, expectedRevision?)` |

### Patch (partial update)

```typescript
await client.patchFile(path, newSize, dirtyParts, readPartFn, progressFn, partSize?);
```

### Vault

| Operation | Method |
|-----------|--------|
| Create secret | `client.createVaultSecret(name, fields)` |
| Update secret | `client.updateVaultSecret(name, fields)` |
| Delete secret | `client.deleteVaultSecret(name)` |
| List secrets | `client.listVaultSecrets()` |
| Issue token | `client.issueVaultToken(agentId, taskId, scope, ttlSeconds)` |
| Revoke token | `client.revokeVaultToken(tokenId)` |
| Read secret | `client.readVaultSecret(name)` |
| Read field | `client.readVaultSecretField(name, field)` |

## Testing

```bash
npm test
```

## License

Apache-2.0
