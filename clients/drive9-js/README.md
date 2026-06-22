# drive9-js

TypeScript SDK for [drive9](https://github.com/mem9-ai/drive9) — an agent-native filesystem with semantic search.

For the full integration guide, see
[`docs/guides/typescript-sdk-integration.md`](../../docs/guides/typescript-sdk-integration.md).

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

Environment variables `DRIVE9_SERVER` or `DRIVE9_BASE` and `DRIVE9_API_KEY` take priority over the config file.
`DRIVE9_CONFIG` can point to an alternate config file.

## Supported operations

| Operation | Method |
|-----------|--------|
| Write file | `client.write(path, data)` |
| Conditional write | `client.write(path, data, expectedRevision)` |
| Read file | `client.read(path)` |
| List directory | `client.list(path)` |
| Stat | `client.stat(path)` |
| Metadata stat | `client.statMetadataCompat(path)` |
| Batch stat | `client.batchStat(paths)` |
| Batch read-small | `client.batchReadSmall(paths, maxBytes)` |
| Delete | `client.delete(path)` |
| Recursive delete | `client.removeAll(path)` |
| Copy | `client.copy(src, dst)` |
| Rename | `client.rename(old, new)` |
| Mkdir | `client.mkdir(path)` |
| Chmod | `client.chmod(path, mode)` |
| Symlink | `client.symlink(target, linkPath)` |
| Hardlink | `client.hardlink(src, dst)` |
| SQL query | `client.sql(query)` |
| Grep | `client.grep(query, prefix, limit)` |
| Layer grep | `client.grepWithLayer(query, prefix, limit, layerRef)` |
| Find | `client.find(prefix, params)` |

### Streaming & multipart

| Operation | Method |
|-----------|--------|
| Stream upload | `client.writeStream(path, stream, size, options?)` |
| Stream upload summary | `client.writeStreamWithSummary(path, stream, size, options?)` |
| Resume upload | `client.resumeUpload(path, stream, totalSize)` |
| Read stream | `client.readStream(path)` |
| Read range stream | `client.readStreamRange(path, offset, length)` |
| Read range bytes | `client.readAt(path, offset, length)` |
| Download to local file | `client.downloadToFile(remotePath, localPath)` |
| Stream writer | `client.newStreamWriter(path, totalSize, options?)` |
| Append | `client.append(path, data)` |

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
| Issue grant | `client.issueVaultGrant(request)` |
| Revoke grant | `client.revokeVaultGrant(grantId, request)` |
| Read secret | `client.readVaultSecret(name)` |
| Read field | `client.readVaultSecretField(name, field)` |
| Owner read secret | `client.readVaultSecretAsOwner(name)` |
| Owner read field | `client.readVaultSecretFieldAsOwner(name, field)` |

### Tokens, events, layers, git, and journals

The SDK also includes typed helpers for:

- filesystem-scoped tokens: `issueScopedToken`, `revokeScopedToken`, `revokeScopedTokenByAPIKey`
- server-sent events: `watchEvents`, `watchEventsWithLifecycle`
- LayerFS: `createFSLayer`, `diffFSLayer`, `commitFSLayer`, and related entry/checkpoint APIs
- git workspace records: `upsertGitWorkspace`, tree, state, object-pack, and overlay APIs
- journals: `createJournal`, `appendJournalEntries`, `readJournalEntries`, `searchJournal`, `verifyJournal`

## Testing

```bash
npm run lint
npm test
npm run build
```

## License

Apache-2.0
