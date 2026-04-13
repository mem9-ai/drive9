# Obsidian drive9 Plugin — Design Document

**Status**: Draft v1
**Date**: 2026-04-13
**Tracking Issue**: [#174](https://github.com/mem9-ai/drive9/issues/174)

---

## 1. Overview

A native Obsidian plugin that syncs a local vault to drive9, bringing semantic search and AI-agent accessibility to Obsidian users — without requiring FUSE mount or CLI.

**Core positioning:** FUSE mount is the power user path; the Obsidian plugin is the first-class experience for all users.

### Why Plugin Instead of FUSE Mount?

| | FUSE mount | Obsidian plugin |
|---|---|---|
| Install friction | macFUSE/FUSE3 + CLI | One-click in Obsidian |
| Mobile | ❌ iOS/Android not supported | ✅ Cross-platform |
| Offline editing | No network = no read/write | ✅ Local vault + background sync |
| Semantic search | Need terminal + API | ✅ In-plugin search |

### Unique Value vs Obsidian Sync / LiveSync

| | Obsidian Sync | LiveSync (CouchDB) | drive9 Plugin |
|---|---|---|---|
| Semantic search | ❌ | ❌ | **✅ Vector + fulltext** |
| AI Agent accessible | ❌ | ❌ | **✅ REST API** |
| Image/PDF understanding | ❌ | ❌ | **✅ Auto extract + embed** |
| Offline editing | ✅ | ✅ | ✅ |
| Mobile | ✅ | ✅ | ✅ |
| Open source | ❌ | ✅ | ✅ |

**The moment you finish writing a note, AI can semantically search, understand, and use it.**

---

## 2. Architecture

**Principle: Local-first, cloud-sync.** Obsidian vault stays on local disk. Plugin listens to Vault events → async sync to drive9.

```
Obsidian App
├── Local Vault (files on disk) ← zero-latency editing
├── drive9 Plugin
│   ├── Drive9Client     HTTP REST client for drive9 API
│   ├── SyncEngine       vault events → SyncState → batch scheduler
│   ├── SSEWatcher       real-time remote change detection
│   ├── ConflictResolver shadow store + 3-way merge + user dialog
│   └── UI               semantic search modal + sync status bar + conflict dialog
└── HTTPS → drive9 server (files + S3 + embeddings + semantic index)
```

### Data Flow

```
Local Edit → Vault Event → Debounce (2s) → SyncEngine → Drive9Client.write()
                                                  ↑
Remote Edit → SSE Event → SyncEngine → conflict? ─┤─ no  → Drive9Client.read() → vault.write()
                                                   └─ yes → ConflictResolver → user decision
```

---

## 3. Drive9Client

Wraps drive9 server REST API using Obsidian's built-in `requestUrl` (bypasses CORS).

```typescript
class Drive9Client {
  constructor(serverUrl: string, apiKey: string);

  // File CRUD — /v1/fs/{path}
  stat(path: string): Promise<StatResult>;      // HEAD → revision + size + mtime
  read(path: string): Promise<ArrayBuffer>;      // GET
  write(path: string, data: ArrayBuffer,
        expectedRevision?: number): Promise<void>; // PUT with CAS
  delete(path: string): Promise<void>;           // DELETE
  rename(oldPath: string, newPath: string): Promise<void>;
  mkdir(path: string): Promise<void>;
  list(path: string): Promise<FileInfo[]>;       // GET ?list=1

  // Search — /v1/search (see §7)
  search(query: string, mode?: SearchMode): Promise<SearchResult[]>;

  // Large file — /v2/uploads/ (see §6)
  writeStream(path: string, data: ArrayBuffer,
              onProgress?: ProgressFn): Promise<void>;
}
```

**API surface notes:**
- `list()` returns `{name, size, isDir, mtime}` — no revision or checksum
- `stat()` returns `{size, isDir, revision, mtime}` — has revision, no checksum
- CAS writes use `X-Dat9-Expected-Revision` header
- Small files (<50KB) use PUT `/v1/fs/{path}`; large files use multipart `/v2/uploads/`

---

## 4. Sync Engine

### 4.1 First-run Reconciliation

**Safety invariant: never silent overwrite remote data.**

On first launch (no SyncState exists):

| Scenario | Action |
|----------|--------|
| Remote empty, local has content | Start one-way push |
| Remote has content, local empty | Prompt "Download from drive9?" → full pull |
| Both have content | Reconcile: list + stat comparison. Same → mark synced. Only-local → push. Only-remote → pull. **Both-different → stop and ask user** |

### 4.2 One-way Push (Phase 1)

Register vault events after layout ready (avoids init-time create storm):

```typescript
this.app.workspace.onLayoutReady(() => {
  this.registerEvent(this.app.vault.on('create', ...));
  this.registerEvent(this.app.vault.on('modify', ...));
  this.registerEvent(this.app.vault.on('delete', ...));
  this.registerEvent(this.app.vault.on('rename', ...));
});
```

Push flow:
1. Vault event → mark file `local_dirty` in SyncState
2. Debounce 2 seconds
3. Batch read dirty files: `vault.readBinary(file)`
4. Upload: `client.write(path, data, expectedRevision)` (CAS)
5. Revision conflict → mark `conflict`, do not overwrite
6. Update SyncState

### 4.3 SSE Remote Change Detection (Phase 2A)

Uses `GET /v1/events?since={seq}` SSE endpoint (PR #153):

```typescript
class SSEWatcher {
  private lastSeq: number = 0;
  private actorId: string;  // unique per plugin instance

  handleFileChanged(event: ChangeEvent): void {
    if (event.actor === this.actorId) return;  // self-filtering
    this.syncEngine.onRemoteChange(event.path, event.op);
  }

  handleReset(event: ResetEvent): void {
    this.syncEngine.fullSync();  // structural change → full re-sync
  }
}
```

- SSE disconnect → degrade to 30s polling with exponential backoff
- `X-Dat9-Actor` header for self-filtering
- Structural ops (rename/delete/mkdir/copy) send `reset` event

### 4.4 SyncState

```typescript
interface SyncState {
  path: string;
  localMtime: number;
  localSize: number;
  remoteRevision: number;
  lastSyncedContentHash?: string;  // sha256 → shadow store lookup
  syncedAt: number;
  status: 'synced' | 'local_dirty' | 'remote_dirty' | 'conflict';
}
```

Persisted to `plugin data.json`, loaded on startup.

### 4.5 Pull/Push State Machine

| Local State | Remote State | Action |
|-------------|-------------|--------|
| — | New file | Pull (download) |
| synced | Modified | Safe pull |
| synced | Deleted | Move to `.trash/` (never silent delete) |
| local_dirty | Unchanged | Push |
| local_dirty | Modified | → Conflict resolution (§5) |

---

## 5. Conflict Resolution (Phase 2B)

### Shadow Store

On successful sync, save a shadow copy for future 3-way merge:

```
.obsidian/plugins/drive9/shadow/
  {sha256-of-content}.bin   # deduplicated by content hash
```

Periodic GC removes unreferenced shadow files.

### Text Files (.md, .txt, .json)

```
base   = shadow store (last synced version)
ours   = local vault current content
theirs = remote content (client.read())

→ Attempt 3-way merge (diff-match-patch / diff3)
→ Success: auto-apply merged result + push
→ Failure: show ConflictModal
```

### Binary Files (.png, .pdf, .mp4)

Cannot merge → show ConflictModal directly.

### ConflictModal Actions

- **Keep local** — overwrite remote with local version
- **Keep remote** — overwrite local with remote version
- **Keep both** — save remote as `{name}.conflict.{ext}`

### Delete Safety

Remote delete is the most irreversible sync action. Rules:
- Remote deleted file → move to Obsidian `.trash/` (never `adapter.remove()`)
- Polling fallback: require stable absence across 2 consecutive polls before treating as deleted

---

## 6. Large File & Media Sync (Phase 2C)

Files ≥50KB use `/v2/uploads/` multipart protocol:

```
1. POST /v2/uploads/initiate → uploadId + presigned part URLs
2. PUT each part (5MB chunks)
3. POST /v2/uploads/{id}/complete
```

**Mobile safety:** Files >100MB skipped on mobile (avoid ArrayBuffer OOM). Configurable via `maxFileSize` setting.

---

## 7. Semantic Search UI (Phase 3)

### /v1/search API (Server-side — #179)

Plugin should call a stable search endpoint, not raw SQL:

```
POST /v1/search
{
  "query": "string",
  "mode": "semantic" | "fulltext" | "hybrid",
  "limit": 20
}
→ { results: [{ path, name, score, snippet, highlight_offsets }] }
```

### SemanticSearchModal

```typescript
class SemanticSearchModal extends SuggestModal<SearchResult> {
  getSuggestions(query: string): Promise<SearchResult[]>;   // hybrid search
  renderSuggestion(result: SearchResult, el: HTMLElement);  // path + snippet
  onChooseSuggestion(result: SearchResult);                 // open file
}
```

Hotkey: `Cmd+Shift+S` (semantic search), separate command for fulltext grep fallback.

---

## 8. Settings & Configuration

```typescript
interface Drive9PluginSettings {
  serverUrl: string;          // e.g. "https://api.drive9.ai"
  apiKey: string;             // drive9 API key (password field in UI)
  pushDebounce: number;       // default 2000ms
  ignorePaths: string[];      // default [".obsidian/**", ".trash/**", "*.tmp", ".DS_Store"]
  maxFileSize: number;        // skip files > this (default 100MB)
}
```

### Ignore Rules

Default exclusions:
```
.obsidian/**    # Obsidian config/plugins (includes API key in data.json)
.trash/**       # Obsidian trash
*.tmp           # temp files
.DS_Store       # macOS metadata
```

### Status Bar

```
✓ drive9: synced | ↑ drive9: uploading 3 files | ✗ drive9: error
```

---

## 9. Auth & Security (#185)

**Pre-release minimum:**
- API key in `data.json`, excluded from sync via ignore rules
- Password field in settings UI
- Never log API key
- Warn if `.gitignore` doesn't cover `.obsidian/`
- Validate key on save (test connection)

**Long-term:**
- OAuth device flow / short-lived tokens
- Scoped tokens (read-only / path restrictions)

---

## 10. Implementation Phases

```
Phase 1 (#175): MVP — Safe one-way push + first-run reconciliation
    ↓
Phase 2A (#176): Bidirectional sync engine (SSE + SyncState + pull/push)
    ↓
Phase 2B (#183): Conflict resolution (shadow store + 3-way merge + delete policy)
Phase 2C (#184): Large file sync (multipart upload + mobile safety)
    ↓
Phase 3 (#177): Semantic search UI (depends on #179 server search API)
    ↓
Phase 4 (#178): Mobile polish + community plugin release
       (#185): Auth & security hardening
```

### Server-side Prerequisites

| Issue | What | Required by |
|-------|------|------------|
| #153 | SSE change notification stream | Phase 2A |
| #151 | Revision guard (optimistic locking) | Phase 1 |
| #179 | Stable `/v1/search` API + snippets | Phase 3 |

---

## 11. Tech Stack

```
Language:    TypeScript
Build:       esbuild (Obsidian official recommendation)
Template:    obsidianmd/obsidian-sample-plugin
HTTP:        Obsidian requestUrl API (no CORS issues)
Storage:     plugin data.json (sync state)
Test:        vitest + mock Vault API
```

---

## 12. Key Design Decisions

1. **Local-first** — vault stays on disk, not replaced by remote DataAdapter
2. **SSE-first for remote detection** — with polling fallback
3. **Revision-based CAS** — `X-Dat9-Expected-Revision` for conflict detection
4. **Shadow copy for 3-way merge** — store base version locally
5. **Debounce 2s** — avoid flooding server during fast typing
6. **Remote delete → .trash** — never silent delete; always recoverable
7. **Stop-and-ask on first run** — detect remote state before any push
8. **Stable search API** — plugin calls `/v1/search`, not raw SQL
