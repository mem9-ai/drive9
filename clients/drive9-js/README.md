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

### Client lifecycle & tuning

| Operation | Method |
|-----------|--------|
| Default client (env/config) | `Client.defaultClient()` |
| Explicit constructor | `new Client(baseUrl?, apiKey?)` |
| Small-file threshold builder | `client.withSmallFileThreshold(n)` |
| Set `X-Dat9-Actor` header | `client.setActor(actor)` |
| Base URL | `client.baseURL()` |
| Auth + actor headers | `client.authHeaders(init?)` |
| Build `/v1/fs/<path>` URL | `client.fsUrl(path)` |
| Build `/v1/vault/<path>` URL | `client.vaultUrl(path)` |
| Prime status cache (best-effort) | `await client.warm()` |
| Tenant status | `await client.status()` |
| Max single-upload bytes | `await client.maxUploadBytes()` |
| Server inline threshold | `await client.smallFileThresholdValue()` |
| Cached inline threshold | `client.cachedSmallFileThreshold()` |
| Raw GET | `await client.rawGet(endpoint)` |
| Raw POST | `await client.rawPost(endpoint, body?)` |
| Raw DELETE | `await client.rawDelete(endpoint, body?)` |

## Supported operations

### Filesystem

| Operation | Method |
|-----------|--------|
| Write file | `await client.write(path, data, options?)` |
| Write returning revision | `await client.writeWithRevision(path, data, options?)` |
| Read file | `await client.read(path)` |
| Create empty file | `await client.createFile(path)` |
| List directory | `await client.list(path)` |
| Stat (HEAD) | `await client.stat(path)` |
| Enriched stat | `await client.statMetadata(path)` |
| Enriched stat with fallback | `await client.statMetadataCompat(path)` |
| Batch stat | `await client.batchStat(paths)` |
| Batch read-small | `await client.batchReadSmall(paths, maxBytes)` |
| Delete | `await client.delete(path)` |
| Delete file (kind hint) | `await client.deleteFile(path)` |
| Delete dir (kind hint) | `await client.deleteDir(path)` |
| Recursive delete | `await client.removeAll(path)` |
| Copy | `await client.copy(src, dst)` |
| Rename | `await client.rename(old, new)` |
| Mkdir | `await client.mkdir(path, mode?)` |
| Chmod | `await client.chmod(path, mode)` |
| Symlink | `await client.symlink(target, linkPath)` |
| Hardlink | `await client.hardlink(src, dst)` |

### Search & SQL

| Operation | Method |
|-----------|--------|
| SQL query | `await client.sql(query)` |
| Grep | `await client.grep(query, prefix, limit)` |
| Layer grep | `await client.grepWithLayer(query, prefix, limit, layerRef)` |
| Find | `await client.find(prefix, params?)` |

### Streaming & multipart

| Operation | Method |
|-----------|--------|
| Stream upload | `await client.writeStream(path, stream, size, options?)` |
| Stream upload with summary | `await client.writeStreamWithSummary(path, stream, size, options?)` |
| Resume upload | `await client.resumeUpload(path, stream, totalSize)` |
| Read stream | `await client.readStream(path)` |
| Read range stream | `await client.readStreamRange(path, offset, length)` |
| Read range bytes | `await client.readAt(path, offset, length)` |
| Download to local file | `await client.downloadToFile(remotePath, localPath)` |
| Stream writer | `client.newStreamWriter(path, totalSize, options?, abortOnError?)` |
| Append bytes | `await client.append(path, data, options?)` |
| Append stream | `await client.appendStream(path, stream, size, options?)` |

`append` / `appendStream` use the native server append flow for existing S3-backed
files. When native append is unavailable, the SDK only uses a bounded small-file
rewrite and rejects large read/merge/write rewrites.

### Download a directory tree

```typescript
// Recursively download a remote directory to a local directory.
// Existing local destinations are never overwritten; a file source is rejected.
await client.downloadDir("/project", "./local-project");
```

### Archive (tar.gz / zip)

```typescript
// Stream a remote directory as a tar.gz (default) or zip archive.
const stream = await client.archive("/project", { format: "zip" });

// Or write the archive straight to a local file.
await client.archiveToFile("/project", "./project.tar.gz", {
  exclude: ["**/node_modules/**"],
  include: ["src/**"],
  flat: false,
  jobs: 16,
});
```

`ArchiveOptions` supports `format` (`"tar.gz"` | `"zip"`), `exclude`/`include`/`profile`
pattern filters (same three pattern forms as the Go CLI: `**/x/**`, `prefix/**`, name
glob like `*.log`), `flat` (strip directory hierarchy), `jobs` (concurrent downloads),
and `signal` (AbortSignal).

### Patch (partial update)

```typescript
await client.patchFile(path, newSize, dirtyParts, readPartFn, progressFn?, partSize?);
```

### Vault

| Operation | Method |
|-----------|--------|
| Create secret | `await client.createVaultSecret(name, fields)` |
| Update secret | `await client.updateVaultSecret(name, fields)` |
| Delete secret | `await client.deleteVaultSecret(name)` |
| List secrets | `await client.listVaultSecrets()` |
| Issue token | `await client.issueVaultToken(agentId, taskId, scope, ttlSeconds)` |
| Revoke token | `await client.revokeVaultToken(tokenId)` |
| Issue grant | `await client.issueVaultGrant(request)` |
| Revoke grant | `await client.revokeVaultGrant(grantId, request?)` |
| Query audit | `await client.queryVaultAudit(secretName?, limit?)` |
| List readable secrets | `await client.listReadableVaultSecrets()` |
| Read secret (scoped) | `await client.readVaultSecret(name)` |
| Read field (scoped) | `await client.readVaultSecretField(name, field)` |
| Owner read secret | `await client.readVaultSecretAsOwner(name)` |
| Owner read field | `await client.readVaultSecretFieldAsOwner(name, field)` |

### Scoped tokens

| Operation | Method |
|-----------|--------|
| Issue fs-scoped token | `await client.issueScopedToken(request)` |
| Revoke by token id | `await client.revokeScopedToken(tokenId)` |
| Revoke by API key | `await client.revokeScopedTokenByAPIKey(apiKey)` |

### Events (SSE)

| Operation | Method |
|-----------|--------|
| Watch change/reset events | `await client.watchEvents(actor, handler, options?)` |
| Watch with lifecycle | `await client.watchEventsWithLifecycle(actor, handler, lifecycle, options?)` |

`WatchEventsOptions` accepts `signal` (AbortSignal) to stop the watcher and
`initialSince` / `initialBackoffMs` for the reconnect loop. `EventLifecycle`
provides `onDisconnected(err)` and `onCurrent(seq)` callbacks.

### LayerFS

| Operation | Method |
|-----------|--------|
| Create layer | `await client.createFSLayer(request)` |
| List layers | `await client.listFSLayers()` |
| Get layer | `await client.getFSLayer(layerId)` |
| Diff layer | `await client.diffFSLayer(layerId, maxSeq?)` |
| Replay layer | `await client.replayFSLayer(layerId, maxSeq?)` |
| Upsert entry | `await client.upsertFSLayerEntry(layerId, request)` |
| Upload layer file | `await client.uploadFSLayerFile(layerId, path, data, opts?)` |
| Read layer file | `await client.readFSLayerFile(layerId, path, maxSeq?)` |
| Read layer file stream | `await client.readFSLayerFileStream(layerId, path, maxSeq?)` |
| Get layer entry | `await client.getFSLayerEntry(layerId, path, maxSeq?)` |
| Checkpoint layer | `await client.checkpointFSLayer(layerId, request)` |
| Get checkpoint | `await client.getFSLayerCheckpoint(checkpointId)` |
| List layer events | `await client.listFSLayerEvents(layerId, since?)` |
| Rollback layer | `await client.rollbackFSLayer(layerId)` |
| Commit layer | `await client.commitFSLayer(layerId)` |

### Git workspaces

| Operation | Method |
|-----------|--------|
| Upsert workspace | `await client.upsertGitWorkspace(request)` |
| Get by root path | `await client.getGitWorkspaceByRoot(rootPath)` |
| Get by id | `await client.getGitWorkspace(workspaceId)` |
| Delete workspace | `await client.deleteGitWorkspace(workspaceId)` |
| List workspaces | `await client.listGitWorkspaces()` |
| Replace tree | `await client.replaceGitTree(workspaceId, request)` |
| List tree | `await client.listGitTree(workspaceId, commitSHA)` |
| Upsert git state | `await client.upsertGitState(workspaceId, request)` |
| Get git state | `await client.getGitState(workspaceId)` |
| Put object pack | `await client.putGitObjectPack(workspaceId, request)` |
| List object packs | `await client.listGitObjectPacks(workspaceId)` |
| Get object pack | `await client.getGitObjectPack(workspaceId, packId)` |
| Put overlay entry | `await client.putGitOverlayEntry(workspaceId, request)` |
| Get overlay entry | `await client.getGitOverlayEntry(workspaceId, relPath)` |
| List overlay entries | `await client.listGitOverlayEntries(workspaceId)` |

### Journals

| Operation | Method |
|-----------|--------|
| Create journal | `await client.createJournal(request)` |
| Append entries | `await client.appendJournalEntries(journalId, appendId, entries)` |
| Read entries | `await client.readJournalEntries(journalId, afterSeq?, limit?)` |
| Search | `await client.searchJournal(request)` |
| Verify | `await client.verifyJournal(journalId)` |

### StreamWriter

`client.newStreamWriter(path, totalSize, options?, abortOnError?)` returns a
resumable v2 multipart writer:

```typescript
const sw = client.newStreamWriter("/big.bin", 16 * 1024 * 1024);
await sw.writePart(1, part1); // lazily initiates the upload
await sw.complete(finalPartNum, finalPartData);
// or: await sw.abort();
```

## Testing

```bash
npm run lint
npm test
npm run build
```

The integration suite (`tests/integration.test.ts`) runs against a live
`drive9-server-local` and is gated by the `DRIVE9_INTEGRATION` env var; see
[`scripts/sdk-integration-tests.sh`](../../scripts/sdk-integration-tests.sh)
for the one-click cross-SDK runner.

## License

Apache-2.0