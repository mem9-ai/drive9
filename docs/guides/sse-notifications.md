# drive9 SSE notifications guide

This guide shows how to receive real-time filesystem change notifications from
a drive9 server over Server-Sent Events (SSE). It is intended for external
clients — integrations, sync agents, UIs, and non-Go SDK consumers — that need
to react to file changes without polling.

For the authoritative cache-invalidation semantics that this stream drives on
FUSE mounts, see
[`docs/specs/cache-invalidation.md`](../specs/cache-invalidation.md). For Go SDK
usage, see [`docs/guides/go-sdk-integration.md`](go-sdk-integration.md).

## What SSE gives you

A single long-lived HTTP stream per tenant that delivers every filesystem
mutation as it commits: writes, uploads, creates, symlinks, hardlinks, renames,
deletes, mkdirs, and copies. The server assigns a monotonic `seq` to each event
so clients can resume after a disconnect without missing events.

SSE is the lowest-latency notification channel drive9 exposes. There is no
webhook, no message queue, and no per-path subscription — the stream is
per-tenant and carries every path event for that tenant. Clients filter
client-side.

## Endpoint

```
GET /v1/events?since=<uint64>
Accept: text/event-stream
Authorization: Bearer <api_key>
X-Dat9-Actor: <unique_client_id>   (optional, recommended)
```

- `since` is a `uint64` event sequence cursor. `since=0` means "initial
  connection" — the server sends a `reset` event first (see below), then a
  `heartbeat` marking the current head. `since=N` means "replay everything
  after N, then keep streaming new events".
- `Authorization` is the same tenant API key used for filesystem calls. The key
  must be an owner key or an `fs_scoped` key with read access. Scoped keys with
  no read permission are rejected.
- `X-Dat9-Actor` is a client-chosen identifier (mount id, host id, agent id).
  When set, the client uses it to self-filter events it produced locally, so a
  client that both writes and watches does not double-process its own writes.
  **Each client instance must use a unique actor id** — two clients sharing an
  id will silently filter each other's events (see "Multi-client isolation").

The response is `Content-Type: text/event-stream` and follows the standard SSE
wire format: each event is one or more `field: value` lines terminated by a
blank line.

## Event types

Three event types are sent on the stream. Exactly one `event:` line per
message.

### `file_changed` — a single-path mutation

```
event: file_changed
data: {"seq":123,"path":"/notes/a.md","op":"write","actor":"mount_abc","ts":1719000000000}

```

Fields:

| Field | Type | Meaning |
|---|---|---|
| `seq` | uint64 | Server-assigned monotonic event sequence. Use as the resume cursor. |
| `path` | string | Absolute tenant path of the changed file. Always starts with `/`. |
| `op` | string | Operation that produced the event. See the routing table below. |
| `actor` | string | The `X-Dat9-Actor` header value of the client that performed the op. Empty for server-internal ops. Omitted from JSON when empty. |
| `ts` | int64 | Server-side timestamp in milliseconds since epoch. |

`op` values delivered as `file_changed`: `write`, `upload_complete`, `create`,
`symlink`, `hardlink`. A `hardlink` emits **two** `file_changed` events — one
for the source path, one for the new link path.

### `reset` — invalidate everything and resync

```
event: reset
data: {"seq":200,"reason":"seq_too_old","path":"/a","op":"rename","actor":"mount_xyz"}

```

A `reset` tells the client: "the server cannot reconstruct the events you
missed between your cursor and now — drop all cached state and resync." The
client must respond by clearing any local cache and treating the next read as
a cold read.

Fields:

| Field | Type | Meaning |
|---|---|---|
| `seq` | uint64 | The server's current head sequence after this reset. Advance your cursor to this. |
| `reason` | string | Why the reset was sent. See below. |
| `path` | string | For `structural_change` only: the path the structural op touched. Omitted otherwise. |
| `op` | string | For `structural_change` only: the structural op (`rename`, `delete`, `mkdir`, `copy`). Omitted otherwise. |
| `actor` | string | The actor that performed the structural op. Omitted when empty. |

`reason` values:

| Reason | When | Client action |
|---|---|---|
| `initial_sync` | First connection with `since=0` | Clear cache, set cursor to `seq`, wait for the post-reset `heartbeat`. |
| `seq_too_old` | Reconnect with a `since` the server can no longer replay (events pruned) | Clear cache, set cursor to `seq`, wait for `heartbeat`. |
| `server_restart` | Reconnect with `since` greater than the server's current head (server restarted and lost in-memory state) | Clear cache, set cursor to `seq`, wait for `heartbeat`. |
| `structural_change` | A structural op (rename/delete/mkdir/copy) was just committed | Clear cache. `path`/`op`/`actor` describe the op for optional targeted invalidation, but the safe default is full clear. |

**Why structural ops are resets, not targeted events**: rename(A, B) affects
the read cache for A, the stat cache for A and B, the directory cache for
`parent(A)` and `parent(B)`, the negative cache for `(parent(B), basename(B))`,
and every entry under A if A is a directory. Single-path invalidation cannot
cover all of those reliably, so the server issues a full reset. The `path` and
`op` fields are provided for clients that want to attempt targeted
invalidation as an optimization, but the spec-mandated safe behavior is full
clear.

### `heartbeat` — stream is current

```
event: heartbeat
data: {"seq":250}

```

A `heartbeat` carries the server's current head `seq` and means "you are caught
up to this point." The server sends one immediately after the initial
replay/reset phase, then every 30 seconds while the stream is idle. Heartbeats
are not invalidation events — they exist so the client can advance its cursor
during quiet periods and mark its cache as "verified" after a reconnect.

## Operation → event routing

Every server mutation produces exactly one event on the stream. This is the
authoritative mapping:

| Server operation | Event | Notes |
|---|---|---|
| `write` | `file_changed` | File content changed. |
| `upload_complete` | `file_changed` | Multipart upload finalized. |
| `create` | `file_changed` | New file created. |
| `symlink` | `file_changed` | New symbolic link. |
| `hardlink` | `file_changed` ×2 | One event for the source path, one for the new link path. |
| `rename` | `reset` (`structural_change`) | Carries `path`, `op="rename"`, `actor`. |
| `delete` | `reset` (`structural_change`) | Carries `path`, `op="delete"`, `actor`. |
| `mkdir` | `reset` (`structural_change`) | Carries `path`, `op="mkdir"`, `actor`. |
| `copy` | `reset` (`structural_change`) | Carries `path`, `op="copy"`, `actor`. |
| `chmod` | **none** | Known gap: `chmod` does not emit an event. Remote chmod changes are invisible to SSE clients until they re-stat. See `docs/specs/cache-invalidation.md` §1.1. |

## Client implementation requirements

A correct SSE client must do four things. The Go SDK's `pkg/client/events.go`
is a reference implementation of all four.

### 1. Track the cursor with `seq`

Advance your cursor on **every** event — `file_changed`, `reset`, and
`heartbeat` all carry a `seq`. Use the maximum seen:

```go
if change != nil && change.Seq > lastSeq { lastSeq = change.Seq }
if reset   != nil && reset.Seq   > lastSeq { lastSeq = reset.Seq   }
// heartbeat: lastSeq = max(lastSeq, heartbeat.Seq)
```

On reconnect, pass `?since=lastSeq`. Never use wall-clock time or event arrival
order as the cursor — only `seq`.

### 2. Reconnect with exponential backoff

Network connections drop. A client that does not reconnect loses events
permanently after the server's retention window prunes them. The Go SDK uses
1s initial backoff, doubles on each failure, caps at 30s, and resets to 1s
after a successful connect:

```
backoff = 1s
loop:
    connect with ?since=lastSeq
    stream until disconnect (error or clean EOF)
    wait backoff
    if disconnect was an error: backoff = min(backoff * 2, 30s)
    else: backoff = 1s
```

Always backoff even on a clean EOF — a connection that ends without an error
still requires a reconnect. Cancel the wait on context cancellation to stop.

### 3. Handle `reset` by clearing cache

Any `reset` reason means "drop cached state." Do not attempt partial recovery
based on `reason`. The safe, spec-mandated behavior is a full clear. If you
maintain a path → metadata cache, drop every entry. If you cache file content,
drop every blob. Set your cursor to `reset.seq` and wait for the post-reset
`heartbeat` before trusting the stream again.

### 4. Set a unique `X-Dat9-Actor` and self-filter

If your client also writes to the filesystem (not just watches), set
`X-Dat9-Actor` to a stable unique id per client instance. When you receive a
`file_changed` whose `actor` equals your own id, you may skip cache
invalidation for that path — you already invalidated it when you performed the
write locally. This prevents double-processing.

**Each client instance must use a unique actor id.** Two clients sharing an
actor id will filter each other's events and serve stale data. Generate the id
once per process/mount and reuse it across reconnects.

## Disconnect semantics

When the SSE connection drops, the server keeps committing events to its
durable `fs_events` table. Your job is to reconnect with `?since=lastSeq` and
let the server replay what you missed. Three outcomes on reconnect:

1. **Server can replay** (`lastSeq` is within the retained window): the server
   streams every missed `file_changed`/`reset` event in `seq` order, then a
   `heartbeat`. Apply each event normally. This is the common case.
2. **Cursor too old** (`lastSeq` is behind the oldest retained event, i.e.
   events were pruned by retention): the server sends a single `reset` with
   `reason=seq_too_old`, then a `heartbeat`. Clear cache and resync.
3. **Server restarted** (`lastSeq` is ahead of the server's current head): the
   server sends a single `reset` with `reason=server_restart`, then a
   `heartbeat`. Clear cache and resync.

During the disconnect window, the client may serve stale cached data. The
staleness is bounded: once reconnected, either replay or full reset eliminates
it. Clients that cannot tolerate any staleness should re-stat on every read
while disconnected rather than trusting cached state.

The retention window for `fs_events` is controlled server-side (the leader
periodically deletes events older than a configured age). The longer your
client stays disconnected, the higher the chance of a `seq_too_old` reset.
Reconnect promptly.

## Multi-client isolation

Multiple clients watching the same tenant all receive the same stream. Client
A writes `/a.txt` → server commits the event → all connected clients (B, C,
…) receive the `file_changed` for `/a.txt` and invalidate their cache for that
path.

Two edge cases to design for:

- **Delivery latency**: SSE delivery is not instantaneous. Client B may read
  stale data in the window between A's write and B receiving the event. This
  is bounded by SSE delivery latency (typically <100 ms on a LAN). If your
  application cannot tolerate this, use explicit coordination (e.g. `fsync` +
  an out-of-band signal) rather than relying on SSE alone.
- **Concurrent writes**: the server's revision-based conflict detection (HTTP
  409) handles simultaneous writes to the same path. SSE is not involved in
  write conflict resolution.

## Deployment: sticky routing

The drive9 event bus is **per-process**. In a multi-pod deployment, each pod
owns the SSE connections it serves and fans out events to other pods via a
shared notification table in the central meta DB (polled at ~200 ms) plus a
pod-to-pod push. This works, but the lowest-latency and most reliable
configuration is **tenant-sticky routing**: ensure the load balancer routes
all of a tenant's SSE connections (and ideally its writes) to the same pod.

If you deploy multiple drive9-server instances behind a non-sticky load
balancer, SSE still works, but cross-pod event delivery adds latency and
depends on the inter-pod notification path. For production notification
workloads, configure tenant-sticky routing at the ingress layer.

## Using the Go SDK

If your client is in Go, use the SDK directly — it handles the cursor,
reconnect, backoff, and event dispatch for you.

```go
import drive9 "github.com/mem9-ai/drive9/pkg/client"

c := drive9.New("<server-url>", "<api-key>")
c.SetActor("my-sync-agent-01") // unique per client instance

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

c.WatchEvents(ctx, "my-sync-agent-01", func(change *drive9.ChangeEvent, reset *drive9.ResetEvent) {
    if change != nil {
        fmt.Printf("changed: %s op=%s seq=%d\n", change.Path, change.Op, change.Seq)
        // invalidate cache for change.Path
    }
    if reset != nil {
        fmt.Printf("reset: reason=%s seq=%d\n", reset.Reason, reset.Seq)
        // clear all cached state
    }
})
```

`WatchEvents` blocks until `ctx` is canceled and reconnects automatically. For
lifecycle hooks (disconnect notification, current-seq callbacks), use
`WatchEventsWithLifecycle`:

```go
c.WatchEventsWithLifecycle(ctx, "my-sync-agent-01", handler, drive9.EventLifecycle{
    OnDisconnected: func(err error) { log.Printf("sse disconnected: %v", err) },
    OnCurrent:      func(seq uint64) { log.Printf("caught up to seq=%d", seq) },
})
```

See `pkg/client/events.go` for the full implementation and
`docs/guides/go-sdk-integration.md` for broader SDK usage.

## Implementing a non-Go client

The protocol is plain SSE over HTTP. Any language with an HTTP client and an
SSE parser (or a raw line reader) can implement it. The minimum viable client:

1. Open `GET /v1/events?since=<cursor>` with `Accept: text/event-stream` and
   the `Authorization` header.
2. Read the response body line by line. Parse `event: <type>` and
   `data: <json>` lines; dispatch on the blank-line boundary.
3. For each event: advance `cursor = max(cursor, event.seq)`. On
   `file_changed`, invalidate the cached entry for `path`. On `reset`, clear
   all cached state. Ignore `heartbeat` for invalidation (use it only to
   advance the cursor and mark the stream as caught up).
4. On disconnect, reconnect with `?since=cursor` and exponential backoff.

Reference wire format (one event per blank-line-separated block):

```
event: file_changed
data: {"seq":123,"path":"/a.md","op":"write","actor":"agent-01","ts":1719000000000}

event: heartbeat
data: {"seq":124}

```

JSON field names and the `event:` types are stable. The `op` and `reason`
vocabularies may grow over time — clients must ignore unknown `op`/`reason`
values gracefully (treat unknown `op` as "something changed at `path`" and
unknown `reason` as a full reset).

## Limitations and known gaps

- **`chmod` emits no event.** A remote `chmod` is invisible to SSE clients
  until they re-stat the file. This is a tracked server-side gap, not a client
  bug.
- **No per-path subscription.** The stream is per-tenant. To watch a subset of
  paths, filter client-side on the `path` field.
- **No ordering guarantee across tenants.** Within a single tenant, events are
  delivered in `seq` order. There is no cross-tenant ordering.
- **Event payload is metadata only.** `file_changed` tells you a path changed;
  it does not include the new content, size, or revision. To get the new state,
  issue a `GET /v1/fs/<path>` or `HEAD /v1/fs/<path>` after invalidating.
- **Retention is finite.** If a client stays disconnected longer than the
  server's `fs_events` retention, it will receive a `seq_too_old` reset on
  reconnect. Reconnect promptly and design for the reset case.

## Reference

- `docs/specs/cache-invalidation.md` — authoritative SSE semantics and the
  cache-invalidation contract this stream drives.
- `pkg/client/events.go` — reference Go client implementation.
- `pkg/server/sse.go` — server-side stream handler and event routing.
- `docs/guides/go-sdk-integration.md` — broader Go SDK usage.