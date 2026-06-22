# drive9 TypeScript SDK integration guide

This guide shows how to integrate the drive9 TypeScript SDK from a Node.js
program, how to test the SDK locally, and which SDK calls map to common drive9
filesystem workflows.

For the Go SDK reference point, see
[`docs/guides/go-sdk-integration.md`](go-sdk-integration.md) and
[`docs/guides/go-sdk-cli-parity.md`](go-sdk-cli-parity.md).

## Prerequisites

- Node.js 18 or newer.
- A reachable drive9 server URL.
- A tenant owner API key or an `fs_scoped` API key for filesystem operations.
- Either set `DRIVE9_SERVER` or `DRIVE9_BASE` with `DRIVE9_API_KEY`, or use an
  owner/fs_scoped current context in `~/.drive9/config`.

The package is designed for Node.js. Browser use is not a supported target
today because large-file transfer helpers and config loading depend on Node
runtime APIs.

## Install

In an external TypeScript project:

```bash
npm install drive9
```

Import the SDK:

```typescript
import { Client } from "drive9";
```

## Create a client

Use an explicit server URL and API key when embedding drive9 into services:

```typescript
import { Client } from "drive9";

const client = new Client(process.env.DRIVE9_SERVER, process.env.DRIVE9_API_KEY);
await client.warm();
```

Use `Client.defaultClient()` when you want CLI-like credential discovery:

```typescript
const client = Client.defaultClient();
```

Credential resolution order:

- `DRIVE9_SERVER`, then `DRIVE9_BASE`, overrides the server URL.
- `DRIVE9_API_KEY` overrides the API key.
- `DRIVE9_CONFIG` overrides the config file path.
- Otherwise the SDK reads `~/.drive9/config`.
- From the config, the current owner or `fs_scoped` context is preferred. If the
  current context is delegated or unusable, the SDK falls back to the first
  usable owner/fs_scoped context by sorted name.
- If no server is found, the SDK falls back to `https://api.drive9.ai`.

Supported config shape:

```json
{
  "server": "https://api.drive9.ai",
  "current_context": "default",
  "contexts": {
    "default": {
      "type": "owner",
      "server": "https://api.drive9.ai",
      "api_key": "drive9_..."
    }
  }
}
```

## Run SDK tests

From this repository:

```bash
cd clients/drive9-js
npm install
npm run lint
npm test
npm run build
```

The unit tests use MSW and do not require a live drive9 server. The cookbook
coverage test compares `Client.prototype` and `StreamWriter.prototype` against
`clients/drive9-js/examples/cookbook.ts`, so adding a public SDK method without
adding cookbook coverage fails tests.

## Paths

drive9 paths are absolute UTF-8 paths.

- Directories end with `/`.
- Files do not end with `/`.
- Do not use backslashes or `..` segments.
- Keep file and directory paths distinct in your own code.

```typescript
const root = "/agents/run-42/";
const readme = `${root}README.md`;
```

## Basic filesystem operations

```typescript
const client = Client.defaultClient();
await client.warm();

await client.mkdir("/agents/run-42/");

await client.write("/agents/run-42/README.md", new TextEncoder().encode("hello drive9\n"));

const data = await client.read("/agents/run-42/README.md");
const entries = await client.list("/agents/run-42/");
const stat = await client.stat("/agents/run-42/README.md");

await client.delete("/agents/run-42/README.md");
```

Additional filesystem helpers:

```typescript
await client.createFile("/agents/run-42/empty.txt");
await client.symlink("README.md", "/agents/run-42/latest");
await client.hardlink("/agents/run-42/README.md", "/agents/run-42/README-hardlink.md");
await client.chmod("/agents/run-42/README.md", 0o640);
await client.copy("/agents/run-42/README.md", "/agents/run-42/README-copy.md");
await client.rename("/agents/run-42/README-copy.md", "/agents/run-42/README-final.md");
await client.removeAll("/agents/run-42/");
```

Use `deleteFile` and `deleteDir` when you want the server to enforce a
file-vs-directory delete hint.

## Uploads

For byte slices:

```typescript
await client.write("/artifacts/result.json", new TextEncoder().encode("{}"));
```

For production uploads, use `writeStreamWithSummary` so the same call works for
direct PUT and multipart uploads:

```typescript
const payload = new TextEncoder().encode("drive9 sdk upload\n");
const summary = await client.writeStreamWithSummary(
  "/agents/run-42/payload.txt",
  payload,
  payload.length,
  {
    tags: { owner: "agent-a", kind: "note" },
    description: "agent handoff note"
  }
);

console.log(summary.mode); // direct_put, multipart_v2, or multipart_v1
```

Compare-and-set upload semantics use `expectedRevision`:

```typescript
const stat = await client.stat("/agents/run-42/state.json");

await client.write("/agents/run-42/state.json", nextStateBytes, {
  expectedRevision: stat.revision
});
```

Expected revision semantics:

- `-1` or omitted: unconditional write.
- `0`: create only if the path does not exist.
- Positive revision: update only if the current file revision matches.

Use `writeWithRevision` when you need the committed revision returned by the
server.

## Downloads

For small files:

```typescript
const data = await client.read("/artifacts/result.json");
```

For streaming:

```typescript
const stream = await client.readStream("/artifacts/large.bin");
```

For range reads:

```typescript
const bytes = await client.readAt("/artifacts/large.bin", 0, 8 << 20);
const rangeStream = await client.readStreamRange("/artifacts/large.bin", 0, 8 << 20);
```

For Node local files:

```typescript
await client.downloadToFile("/artifacts/large.bin", "./large.bin");
```

## Metadata

Use `stat` for lightweight HEAD metadata:

```typescript
const stat = await client.stat("/agents/run-42/README.md");
console.log(stat.size, stat.revision, stat.mode);
```

Use `statMetadataCompat` when you need tags, content type, semantic text, or a
compatibility fallback for older servers:

```typescript
const meta = await client.statMetadataCompat("/agents/run-42/README.md");
console.log(meta.tags, meta.semantic_text, meta.degraded);
```

## Batch calls

Batch calls avoid one HTTP request per file for common agent workflows.

```typescript
const stats = await client.batchStat([
  "/agents/run-42/README.md",
  "/agents/run-42/config.json"
]);

const smallFiles = await client.batchReadSmall([
  "/agents/run-42/README.md",
  "/agents/run-42/config.json"
], 1 << 20);
```

Limits:

- `batchStat`: up to `MaxBatchStatPaths`.
- `batchReadSmall`: up to `MaxBatchReadSmallPaths`.

Per-path failures are returned inside each result. Transport or malformed
response errors fail the whole method.

## Search and find

Semantic/text search:

```typescript
const matches = await client.grep("deployment checklist", "/agents/", 20);
```

Layer-aware grep:

```typescript
const matches = await client.grepWithLayer("deployment checklist", "/agents/", 20, "layer_123");
```

Structured find:

```typescript
const matches = await client.find("/agents/", {
  name: "*.md",
  tag: "kind=note"
});
```

Tag matching semantics:

- `tag=key=value` is an exact key/value match.
- `tag=key` checks whether the tag key exists.
- Tags do not support fuzzy, prefix, contains, or regex matching.

## Append

Use `append` or `appendStream` for small to moderate log-like writes:

```typescript
await client.append("/agents/run-42/events.log", new TextEncoder().encode("step completed\n"));
```

The TypeScript SDK implements append as a revision-guarded read/merge/write
rewrite. This gives a safe SDK-level API, but it is not the optimized large-file
append path used by the Go SDK and CLI.

## Error handling

The SDK throws `StatusError` for HTTP API errors and `ConflictError` for HTTP
409 conflicts:

```typescript
try {
  await client.write("/agents/run-42/state.json", nextStateBytes, { expectedRevision: 10 });
} catch (err) {
  if (err instanceof ConflictError) {
    // Re-read, merge, and retry.
  }
  if (err instanceof StatusError) {
    console.error(err.statusCode, err.message);
  }
  throw err;
}
```

## Scoped filesystem tokens

Owner clients can issue narrower filesystem-scoped tokens:

```typescript
const issued = await client.issueScopedToken({
  subject: "agent-a",
  ttl_seconds: 30 * 60,
  scopes: [
    { prefix: "/agents/run-42/", ops: ["read", "write"] }
  ]
});

const agentClient = new Client(client.baseUrl, issued.token);
```

Revoke by token id:

```typescript
await client.revokeScopedToken(issued.token_id!);
```

Or revoke by presenting the target API key to an owner-authenticated client:

```typescript
await client.revokeScopedTokenByAPIKey(issued.token);
```

## Vault helpers

Owner API key clients can manage vault secrets:

```typescript
await client.createVaultSecret("prod-db", {
  DB_URL: "postgres://..."
});

const fields = await client.readVaultSecretAsOwner("prod-db");
const dbURL = await client.readVaultSecretFieldAsOwner("prod-db", "DB_URL");
```

Delegated vault tokens use the consumption API:

```typescript
const vaultClient = new Client(client.baseUrl, delegatedVaultToken);
const value = await vaultClient.readVaultSecretField("prod-db", "DB_URL");
```

Grant helpers:

```typescript
const grant = await client.issueVaultGrant({
  agent: "agent-a",
  scope: ["prod-db"],
  perm: "read",
  ttl_seconds: 30 * 60
});

await client.revokeVaultGrant(grant.grant_id, {
  revoked_by: "owner",
  reason: "task complete"
});
```

## Events

Use server-sent events to invalidate caches or react to remote filesystem
changes. Abort the signal to stop watching.

```typescript
const abort = new AbortController();

void client.watchEventsWithLifecycle(
  "agent-a",
  (change, reset) => {
    if (change) {
      console.log(change.path, change.op, change.seq);
    }
    if (reset) {
      // Drop local caches and resync.
    }
  },
  {
    onCurrent(seq) {
      console.log("caught up through", seq);
    },
    onDisconnected(err) {
      console.warn("event stream disconnected", err);
    }
  },
  { signal: abort.signal }
);
```

## Layer filesystem API

The SDK exposes typed LayerFS calls for agent sandbox workflows that need an
overlay before committing to the base filesystem:

```typescript
const layer = await client.createFSLayer({
  base_root_path: "/agents/run-42/",
  name: "attempt-1",
  actor_id: "agent-a"
});

await client.upsertFSLayerEntry(layer.layer_id, {
  path: "/agents/run-42/README.md",
  op: "upsert",
  kind: "file",
  content: new TextEncoder().encode("updated\n")
});

const diff = await client.diffFSLayer(layer.layer_id);
const checkpoint = await client.checkpointFSLayer(layer.layer_id, { label: "before-commit" });
const commit = await client.commitFSLayer(layer.layer_id);
```

Use ordinary `/v1/fs` methods when you do not need an explicit layer.

## Git workspace API

The SDK exposes typed server-side git workspace records, tree state, object
packs, and overlay entries:

```typescript
const workspace = await client.upsertGitWorkspace({
  root_path: "/repos/drive9/",
  repo_url: "https://github.com/mem9-ai/drive9.git",
  remote_name: "origin",
  branch_name: "main"
});

await client.replaceGitTree(workspace.workspace_id, {
  commit_sha: "a".repeat(40),
  nodes: []
});

await client.putGitOverlayEntry(workspace.workspace_id, {
  path: "dirty.txt",
  op: "upsert",
  kind: "file",
  content: new TextEncoder().encode("dirty")
});

const packs = await client.listGitObjectPacks(workspace.workspace_id);
const overlay = await client.listGitOverlayEntries(workspace.workspace_id);
```

This is not a one-shot `git clone --fast` workflow. Local git process
orchestration, worktree setup, `.git` hydration, and CLI output remain CLI
workflow concerns.

## Journal API

```typescript
const journal = await client.createJournal({
  journal_id: "run-42",
  kind: "agent",
  title: "agent run 42"
});

await client.appendJournalEntries(journal.journal_id, "append-1", [
  { type: "tool.call.completed", status: "ok" }
]);

const entries = await client.readJournalEntries(journal.journal_id, 0, 100);
const matches = await client.searchJournal({ type: "tool.call.completed", entries: true });
const verification = await client.verifyJournal(journal.journal_id);
```

## Raw helpers

Use `rawPost` and `rawDelete` for endpoints that do not yet have a typed SDK
method:

```typescript
const resp = await client.rawPost("/v1/provision", { name: "tenant-name" });
const body = await resp.json();
```

Prefer typed methods when they exist.

## TypeScript SDK vs CLI parity

The TypeScript SDK is strongest at typed server API calls. The CLI adds local
workflow orchestration around those calls.

Typed SDK coverage includes:

- Filesystem CRUD, chmod, recursive remove, symlink, hardlink.
- Batch stat and batch read-small.
- Direct PUT, multipart upload, resumable upload, patch, range reads, streaming
  writer, and Node download helper.
- Metadata, grep, layer-aware grep, find, and SQL.
- Filesystem-scoped token issue/revoke.
- Vault secret, grant, delegated read, owner read, and audit APIs.
- Server-sent events with reconnect lifecycle callbacks.
- LayerFS, git workspace, and journal APIs.
- `rawPost` and `rawDelete` escape hatches.

Out of scope for the SDK:

- `drive9 mount`, `umount`, FUSE/WebDAV lifecycle, and local mount state files.
- `drive9 doctor`, `drive9 update`, CLI output formatting, and profile parsing.
- Local recursive copy orchestration for every local/remote matrix.
- `vault with` child-process environment injection.
- One-shot `git clone --fast` local workflow orchestration.

## Operational notes

- Call `warm()` once before upload-heavy workflows so the first upload uses the
  server-advertised inline threshold.
- Keep API keys out of logs and command arguments. Prefer environment variables
  or a secret manager.
- Cross-host read redirects are followed without forwarding drive9 credentials.
- Use scoped filesystem tokens for agent workloads that only need a prefix.
- The SDK keeps wire-compatible names such as `X-Dat9-*` headers for protocol
  compatibility.
