# Agent Journal Design Spec

**Status**: Draft / Proposed
**Date**: 2026-05-11
**Last reviewed**: 2026-05-13
**Scope**: CLI, HTTP API, filesystem projection, TiDB schema, integrity model

---

## 1. Summary

drive9 should provide a general-purpose append-only **journal** primitive for
recording agent activity, workflow execution, tool calls, policy decisions,
approvals, filesystem mutations, and other durable facts.

The product surface should be small and Unix-like:

```bash
drive9 journal new
drive9 journal append <journal>
drive9 journal cat <journal>
drive9 journal find
drive9 journal verify <journal>
```

The core abstraction is not "tool audit". It is:

```text
journal = append-only stream of structured entries
entry   = one observed fact in that stream
subject = indexed object(s) the entry is about
artifact = large evidence attached to an entry
seal    = integrity checkpoint over the stream head
```

TiDB is the source of truth for journals and query indexes. The drive9
filesystem may expose a read/write projection under `/.journal/`, but the
journal tables enforce the append-only and integrity invariants.

The recommended first-class tables are:

```text
journals
journal_labels
journal_append_requests
journal_entries
journal_entry_subjects
journal_artifacts
journal_entry_artifacts
journal_seals
```

---

## 2. Goals

1. Provide a durable, queryable record of agent and workflow activity.
2. Keep the CLI minimal, pipe-friendly, and compatible with JSONL tooling.
3. Avoid binding the model to tools; tool calls are only one entry type.
4. Support Plan 9 style file namespace semantics for creation, append, read,
   control, and verification.
5. Make high-frequency writes efficient with batched append.
6. Make common queries fast without scanning JSON payloads.
7. Preserve integrity with server-assigned sequence numbers, hash chains, and
   optional seals.
8. Separate self-reported logs from gateway/server-observed audit evidence.
9. Keep hot-path records bounded so write throughput is controlled by entry
   count, not unbounded payload size.

---

## 3. Non-Goals

1. Do not build a full SIEM product.
2. Do not expose separate CLI objects for every entry type, such as
   `journal tool`, `journal approval`, or `journal artifact`.
3. Do not store large stdout, stderr, prompts, or tool payloads inline in the
   hot journal entry table.
4. Do not rely on ordinary drive9 files as the authoritative store for journal
   integrity.
5. Do not make the first version POSIX-complete for the `/.journal/`
   projection.

---

## 4. Design Principles

### 4.1 Unix Principles

- Entries are JSONL on stdin/stdout.
- `append` appends to a stream.
- `cat` reads a stream.
- `find` searches indexed metadata.
- `verify` checks stream integrity.
- The CLI should compose with `jq`, `grep`, `sort`, `less`, and shell pipes.

### 4.2 Plan 9 Principles

Expose journal resources as a namespace:

```text
/.journal/clone
/.journal/<journal_id>/entries
/.journal/<journal_id>/ctl
/.journal/<journal_id>/head
/.journal/index
```

The namespace is the primitive. The CLI is a convenience layer over it.

### 4.3 Data Model Principles

- The stable table is `journal_entries`, not `tool_calls`.
- Entry type and schema version carry domain-specific semantics.
- Subjects are many-to-one with entries, because one entry can concern a tool,
  file, command, secret, repo, PR, and policy at the same time.
- Journal metadata that must be queried should be promoted to labels, not left
  only inside a JSON blob.
- Large evidence is stored as artifacts and referenced by hash/ref.
- Queries should use typed columns and subject indexes, not JSON scans.
- Evidence source is assigned or constrained by the authenticated server/gateway
  context. Untrusted clients cannot make weak evidence look stronger by setting
  `source=gateway_observed`.

---

## 5. Concepts

### 5.1 Journal

A journal is an append-only stream for one agent run, workflow, build, deploy,
or other bounded unit of work.

Examples:

```text
kind=agent       one Codex/Claude/OpenCode task run
kind=workflow    one CI or orchestration workflow
kind=deploy      one deployment operation
kind=build       one build attempt
kind=session     one interactive shell/session
```

### 5.2 Entry

An entry is one observed fact.

Example entry types:

```text
agent.message
tool.call.started
tool.call.completed
tool.call.failed
approval.requested
approval.granted
approval.denied
policy.decision
fs.write
fs.delete
secret.materialized
command.exec
github.pr.comment
```

The first version should not hard-code these as separate commands or tables.
They are values in the journal entry schema.

Entry type names should be stable, lowercase, dot-separated strings:

```text
<domain>.<object>[.<phase>]
```

Examples:

```text
tool.call.started
tool.call.completed
approval.requested
policy.decision
fs.write
```

The tuple `(type, schema_version)` defines the entry payload contract. New
fields may be added compatibly, but incompatible payload changes require a new
schema version.

Common status values should stay small and queryable:

```text
started
ok
error
denied
cancelled
```

Not every entry needs a status, but high-volume operational entries should use
these values so `journal find --status ...` remains useful.

### 5.3 Subject

A subject is an indexed object associated with an entry.

Examples:

```text
tool:exec_command
file:/pkg/backend/store.go
path:/docs/design/agent-journal-design.md
repo:github.com/mem9-ai/drive9
secret:OPENAI_API_KEY
command:go test ./...
pr:mem9-ai/drive9#123
policy:network.escalation
```

Subject syntax is:

```text
<type>:<id>
```

`type` is lowercase ASCII and is normalized before storage. `id` is UTF-8 NFC
and otherwise preserves case and path spelling. `subject_hash` is computed from
the canonical pair, not from the raw submitted string:

```text
subject_hash = "sha256:" + hex(sha256(subject_type + "\0" + subject_id))
```

Subject IDs are indexable metadata and may be visible to readers with journal
search access. Sensitive subjects should use opaque IDs, such as
`secret:sec_01J...`, rather than embedding secret values, token fragments, or
customer-private text directly in the subject string.
Subject search is exact search by normalized `(subject_type, subject_id)` in the
first implementation. Prefix, contains, fuzzy, and regex subject searches should
not be added by reusing plaintext subject indexes; they need a separate
privacy-reviewed design.

### 5.4 Artifact

An artifact is large or sensitive evidence attached to an entry:

- stdout/stderr
- full tool request/response
- model prompt/completion snapshots
- generated files
- command traces
- screenshots

Artifacts are addressed by content hash and storage ref. The content hash is
over canonical evidence bytes after redaction and before storage encryption.
An artifact object is the stored bytes; an artifact reference is one entry's
attachment metadata for those bytes, such as `name`, declared `content_type`,
and `size_bytes`.
Small summaries can remain inline in the entry.

### 5.5 Seal

A seal records a checkpoint over the current journal head:

```text
tenant_id, journal_id, seal_id, seq, head_hash, seal_type, sealed_at, signer, signer_key_id, signature_alg, signature
```

A seal makes later tampering detectable. For stronger evidence, the signature
key should live outside the tenant data store, such as KMS or another control
plane key service.

The signed seal payload should bind the checkpoint to its context:

```text
tenant_id, journal_id, seal_id, seq, head_hash, seal_type, sealed_at, signer, signer_key_id, signature_alg
```

Signing only `head_hash` is not enough because it allows replaying a valid
checkpoint into a different tenant or journal context.

### 5.6 Label

Labels are indexed journal metadata used by `journal find -m key=value`.

Label keys are lowercase ASCII identifiers. Label values are UTF-8 NFC strings.
`label_hash` uses the same bounded-key pattern as subjects:

```text
label_hash = "sha256:" + hex(sha256(label_key + "\0" + label_value))
```

Labels are projection metadata, not evidence by themselves. If a label change
must be auditable, write a `journal.control.label` entry and update
`journal_labels` in the same transaction. Labels are searchable metadata, so
secret values or customer-private text should not be used as label values.
Label search is exact key/value search in the first implementation. Prefix,
contains, fuzzy, and regex label searches should require an explicit future
index design and privacy review.

---

## 6. CLI Spec

### 6.1 Command Set

```bash
drive9 journal new [flags]
drive9 journal append <journal> [flags]
drive9 journal cat <journal> [flags]
drive9 journal find [flags]
drive9 journal verify <journal> [flags]
drive9 journal seal <journal> [flags]
```

`seal` can be added after the basic append/read/find/verify path. The core CLI
should still be useful without sealing.

### 6.2 `journal new`

Create a journal and print its ID.

```bash
drive9 journal new -k agent -m agent=codex -m repo=github.com/mem9-ai/drive9
```

Flags:

```text
--id <journal_id>       Optional caller-provided journal ID for supervised retries.
-k, --kind <kind>       Journal kind. Default: agent.
-m, --meta k=v         Repeatable metadata pair.
--title <text>         Optional human-readable title.
--json                 Print the full journal object as JSON.
```

Each `-m/--meta` pair is stored in `journal_labels` for indexed lookup. A
single-value compatibility map may be kept in `journals.meta` for display, but
the API representation that preserves repeated keys is a `labels` array such
as `[{"key":"...","value":"..."}]`.

For retry-safe creation, the CLI should generate a journal ID before sending
the create request. `--id` is mainly for supervisors that need to retry
`journal new` across process boundaries. Reusing the same ID with the same
normalized create envelope is idempotent; reusing it with different create
metadata is a conflict.
Journal creation records are genesis/self-reported metadata in Phase 1.
Stronger evidence sources such as `gateway_observed` and `server_observed`
belong on entries appended by trusted writers, not on the create request.

Default output:

```text
jrn_01J...
```

JSON output:

```json
{
  "journal_id": "jrn_01J...",
  "kind": "agent",
  "created_at": "2026-05-11T08:00:00Z",
  "meta": {
    "agent": "codex",
    "repo": "github.com/mem9-ai/drive9"
  },
  "labels": [
    {"key":"agent","value":"codex"},
    {"key":"repo","value":"github.com/mem9-ai/drive9"}
  ]
}
```

### 6.3 `journal append`

Append entries from stdin. Input is JSONL by default.

```bash
printf '%s\n' \
  '{"type":"tool.call.completed","status":"ok","subjects":["tool:exec_command"],"summary":{"cmd":"go test ./..."}}' \
  | drive9 journal append "$jid"
```

Flags:

```text
-t, --type <type>         Default type for entries without a type field.
-s, --subject <subject>   Repeatable subject added to every entry.
--json-array              Read a JSON array instead of JSONL.
--artifact @path          Upload an artifact and attach it to the entry.
--source <source>         gateway_observed, server_observed, or self_reported.
--idempotency-key <key>   Explicit append id for retry-safe batch append.
--batch-count <n>         Flush after this many entries. Default: implementation-defined.
--batch-bytes <n>         Flush after this many buffered bytes. Default: implementation-defined.
--linger <duration>       Wait briefly for more entries before flushing.
```

The CLI should generate one idempotency key per invocation when
`--idempotency-key` is omitted, because the default server API rejects keyless
append. Writers that retry across process boundaries must use a stable
`--idempotency-key` for the retried batch. An external supervisor that re-runs
the CLI must pass an explicit key to get duplicate-safe retry semantics across
processes.

For audit-sensitive use, server-side append without an idempotency key should
be rejected. If a deployment chooses to accept keyless appends for convenience,
the caller should have to opt in explicitly and the response must set
`"idempotent": false` so callers do not assume safe retry.

Output should be compact and scriptable:

```json
{"journal_id":"jrn_01J...","append_id":"app_01J...","first_seq":1,"last_seq":3,"count":3,"head_hash":"sha256:...","idempotent":false}
```

Important behavior:

- The server assigns `seq`.
- The server assigns `observed_at`.
- The server computes `prev_hash` and `entry_hash`.
- The client or gateway sends one append id per batch so retries are
  idempotent.
- Fresh appends return `"idempotent": false`; duplicate retries that return a
  previously committed sequence range return `"idempotent": true`.
- The server validates or overrides `source` based on caller identity. Ordinary
  CLI appends are `self_reported` unless they come from a trusted runner.
- Append is atomic per request: all entries in a batch are committed, or none
  are committed.
- Appending to a sealed or closed journal fails unless the seal policy permits
  new segments.

### 6.4 `journal cat`

Read entries as JSONL.

```bash
drive9 journal cat "$jid"
drive9 journal cat "$jid" --after 100 --limit 200
drive9 journal cat "$jid" -f
```

Flags:

```text
--after <seq>       Start after this sequence number.
--limit <n>         Maximum entries.
-f, --follow        Follow appended entries.
--with-artifacts    Include artifact metadata.
--raw               Include raw payload refs and hashes only, not inline fetch.
```

`cat` should not fetch large artifacts by default.

### 6.5 `journal find`

Search journals and entries by indexed fields.

```bash
drive9 journal find -t tool.call.completed -s tool:exec_command --since 1h
drive9 journal find -s file:/pkg/backend/store.go
drive9 journal find --kind agent -m repo=github.com/mem9-ai/drive9
```

Flags:

```text
-t, --type <type>          Entry type filter.
-s, --subject <subject>    Subject filter. Repeatable.
--kind <kind>              Journal kind filter.
--actor <actor>            Actor filter, for example agent:codex.
--status <status>          Entry status filter.
--since <duration|time>    Lower observed-time bound.
--until <time>             Upper observed-time bound.
--occurred-since <time>    Optional event-time lower bound.
--occurred-until <time>    Optional event-time upper bound.
-m, --meta k=v             Journal metadata filter.
--limit <n>                Result limit.
--cursor <token>           Resume a previous search page.
--entries                  Emit full entry JSONL. Requires journal:read.
--json                     Emit structured JSONL match objects.
```

Default output should be compact match references that are easy to pipe, not
full entries:

```text
jrn_01J...	128	tool.call.completed	2026-05-11T08:01:00Z
```

`--json` should emit JSONL match objects. `--entries` should emit full entry
JSONL and requires `journal:read`. For durable pagination, callers should use
`--json` and pass the last match object's `cursor` back with `--cursor`,
repeating the original filters exactly. The Phase 1 cursor is a keyset token
for a query shape, not a stored server-side query handle; `--cursor` alone is
not enough to resume a filtered search. Repeated `--subject` and `--meta`
filters are AND filters by default.

### 6.6 `journal verify`

Verify the hash chain and seals.

```bash
drive9 journal verify "$jid"
```

Default output:

```text
ok journal=jrn_01J... entries=128 head=sha256:...
```

JSON output:

```json
{
  "ok": true,
  "journal_id": "jrn_01J...",
  "entries": 128,
  "head_hash": "sha256:...",
  "hash_chain_ok": true,
  "projection_ok": true
}
```

`ok` is true only when the requested verification scope succeeds. For example,
`mode=full` with artifact verification enabled should fail `ok` if referenced
artifact bytes are unavailable; plain hash-chain verification can still return
`hash_chain_ok=true` with `artifact_bytes_available=false`. Fields for scopes
that were not checked, such as seals in a Phase 1 deployment without seal
support, should be omitted rather than reported as successful.

### 6.7 `journal seal`

Create an integrity checkpoint over the current head.

```bash
drive9 journal seal "$jid"
```

The server records:

```text
seal_id, seq, head_hash, seal_type, sealed_at, signer, signer_key_id, signature_alg, signature
```

The first implementation may support soft seals that detect tampering but do
not prevent appends. A stricter policy can close the journal on seal.

---

## 7. Filesystem Projection

The proposed Plan 9 style projection is:

```text
/.journal/clone
/.journal/<journal_id>/entries
/.journal/<journal_id>/ctl
/.journal/<journal_id>/head
/.journal/index
```

### 7.1 `clone`

Writing journal metadata creates a journal. Reading from the same open handle
returns the new ID.

This is a Plan 9 style interface, not a portable POSIX shell contract. True
`clone` semantics depend on per-open state:

```text
open /.journal/clone
write {"kind":"agent","meta":{"agent":"codex"}}
read  -> jrn_01J...
close
```

Protocols that cannot preserve that state, such as a simple WebDAV write
followed by a separate read, should use the HTTP API or CLI instead. The HTTP
API remains authoritative.

### 7.2 `entries`

Append-only JSONL file.

```bash
printf '{"type":"tool.call.completed","status":"ok"}\n' >> /.journal/$jid/entries
cat /.journal/$jid/entries
tail -f /.journal/$jid/entries
```

Semantics:

- Writes append entries.
- Reads stream entries in sequence order.
- Random overwrite and truncate are rejected.
- Large artifacts are referenced, not embedded by default.

### 7.3 `ctl`

Control file for low-frequency commands:

```text
seal
close
label key=value
```

Control commands that change journal state should either be represented as
entries, such as `journal.control.close` or `journal.control.label`, or be
documented as non-evidentiary projection updates. Audit-sensitive state changes
should use the entry path.

### 7.4 `head`

Read-only current head:

```json
{"seq":128,"head_hash":"sha256:...","head_sealed":false,"latest_seal_seq":100}
```

### 7.5 `index`

Query interface for filesystem clients. The CLI should normally use the HTTP
query API instead of requiring users to manually write query expressions here.

---

## 8. HTTP API Spec

### 8.1 Create Journal

```text
POST /v1/journals
Content-Type: application/json
```

Request:

```json
{
  "journal_id": "jrn_01J...",
  "kind": "agent",
  "title": "Implement issue 123",
  "meta": {
    "agent": "codex",
    "repo": "github.com/mem9-ai/drive9"
  }
}
```

`journal_id` is optional for convenience but recommended for retry-safe clients.
If supplied, it must be globally unique within the tenant. Repeating the same
create request with the same `journal_id` and normalized `create_hash`
returns the existing journal. Reusing the ID with different kind, title, meta,
actor, source, or retention returns `409 conflict`. If `journal_id` is omitted,
the server generates one, but callers cannot safely retry after an ambiguous
timeout without risking a duplicate journal.

Response:

```json
{
  "journal_id": "jrn_01J...",
  "kind": "agent",
  "created_at": "2026-05-11T08:00:00Z"
}
```

### 8.2 Append Entries

```text
POST /v1/journals/{journal_id}/entries
Content-Type: application/x-ndjson
Idempotency-Key: app_01J...
```

or:

```text
Content-Type: application/json
Idempotency-Key: app_01J...
```

For `application/json`, the request body may be one entry object or an array of
entry objects. For `application/x-ndjson`, each line is one entry.

If `occurred_at` is omitted, the server stores `observed_at` as `occurred_at`.
If `actor` is omitted, the server derives it from the authenticated caller or
the journal default actor. If `source` is omitted, the default is
`self_reported` unless the authenticated writer has a stronger source role.

Request entry:

```json
{
  "type": "tool.call.completed",
  "schema_version": 1,
  "occurred_at": "2026-05-11T08:01:00Z",
  "status": "ok",
  "actor": {
    "type": "agent",
    "id": "codex"
  },
  "source": "gateway_observed",
  "subjects": [
    "tool:exec_command",
    "command:go test ./..."
  ],
  "summary": {
    "cmd": "go test ./...",
    "exit_code": 0,
    "duration_ms": 842
  },
  "artifacts": [
    {
      "name": "stdout",
      "hash": "sha256:...",
      "content_type": "text/plain",
      "size_bytes": 1024
    }
  ]
}
```

Response:

```json
{
  "journal_id": "jrn_01J...",
  "first_seq": 1,
  "last_seq": 1,
  "count": 1,
  "head_hash": "sha256:...",
  "append_id": "app_01J...",
  "idempotent": true
}
```

`Idempotency-Key` is required for the public audit-safe append API. A missing
key should return `400 Bad Request`. Keyless append, if implemented at all,
should be limited to explicitly configured import or local-development clients;
it must not be a silent fallback for normal SDK/CLI writers. Compatibility-mode
responses must set `"idempotent": false`.

Append requests that reference artifacts must reference artifact byte objects
that already exist, or use a future multipart append API that stores artifact
bytes before committing the entries. The server must validate each
`artifact_ref` before commit:

```text
artifact_hash exists for tenant
artifact_ref.size_bytes == journal_artifacts.size_bytes
artifact_ref.name is unique within the entry
artifact_ref metadata is within hot-path limits
```

Missing artifacts or size mismatches should fail the append with
`422 validation_failed`; the journal must not commit an entry that points at
missing evidence.

### 8.2.1 Upload Artifact Bytes

The first artifact implementation can expose a content-addressed upload API:

```text
PUT /v1/journal-artifacts/{artifact_hash}
Content-Type: application/octet-stream
Content-Length: <bytes>
```

The server must compute the hash of the canonical bytes it receives and reject
the upload if it does not match `{artifact_hash}`. Re-uploading the same bytes
for the same tenant is idempotent and returns the existing artifact metadata.

Response:

```json
{
  "artifact_hash": "sha256:...",
  "size_bytes": 1024,
  "detected_content_type": "text/plain",
  "storage_type": "s3"
}
```

This upload creates a byte object, not journal evidence. The byte object becomes
evidence only when an append commits an `artifact_ref` that points to it.
Upload requires `journal:append` or a narrower artifact-write credential for
the tenant. Upload permission does not imply read permission.

Unreferenced artifacts should count against a tenant or writer temporary
artifact quota until they are referenced by a committed entry or garbage
collected. If the quota is exceeded, the server should apply backpressure or
reject uploads with `429 rate_limited` or `413 payload_too_large`.

### 8.2.2 Fetch Artifact Bytes

Artifact bytes should be fetched through a journal entry attachment path, not by
hash alone:

```text
GET /v1/journals/{journal_id}/entries/{seq}/artifacts/{name}
```

The server resolves `{name}` through the committed `artifact_refs` for that
entry, then fetches the referenced artifact hash. This makes the authorization
check naturally journal-scoped: the caller needs `journal:read` on the journal
and must be allowed to read artifact bytes for that entry. A raw
`GET /v1/journal-artifacts/{artifact_hash}` endpoint, if exposed at all, should
be admin/internal only; an artifact hash is an identifier, not a read
capability.

If the entry reference exists but the artifact bytes were archived or deleted
by retention policy, the fetch should return a stable unavailable/gone error
instead of pretending the entry does not reference an artifact.

### 8.3 Read Entries

```text
GET /v1/journals/{journal_id}/entries?after_seq=0&limit=1000
```

Response is JSONL by default for streaming clients.

### 8.4 Follow Entries

```text
GET /v1/journals/{journal_id}/entries?after_seq=128&follow=true
```

The first version can use long polling. Server-Sent Events or WebSocket support
can be added later if needed.

### 8.5 Search Entries

```text
GET /v1/journal-entries?type=tool.call.completed&subject=tool:exec_command&since=2026-05-11T00:00:00Z&limit=100
```

Search results should include journal ID and sequence number so callers can
resume with `cat`. Full summaries should be omitted unless the caller has
`journal:read`.

Default search output should be JSONL match records, not full entries:

```json
{"journal_id":"jrn_01J...","seq":128,"type":"tool.call.completed","status":"ok","observed_at":"2026-05-11T08:01:00Z","matched_subjects":["tool:exec_command"],"cursor":"eyJvYnNlcnZlZF9hdCI6..."}
```

An explicit `include=entry` option can return full entry envelopes, and should
require `journal:read`.

Search results should not dump all subjects or labels by default. They may
include only the matched subject/label values that the caller already supplied,
or values allowed by the deployment's metadata visibility policy. Full subject
lists, label values, summaries, and artifact metadata require `journal:read` or
an explicitly defined metadata-read policy.

Search pagination should use keyset cursors, not offset pagination. The cursor
should encode the last returned ordering tuple and the query shape. A cursor
from one query must be rejected if reused with different filters or tenant
context. Servers may also expose the next cursor in a response header such as
`X-Drive9-Next-Cursor` for clients that do not want per-row cursors.

Cursors should be opaque, versioned, and integrity-protected, for example a
base64url-encoded JSON envelope plus HMAC:

```json
{"v":1,"order":["2026-05-11T08:01:00Z","jrn_01J...",128],"query_hash":"sha256:...","tenant_id":"tn_..."}
```

Clients must not depend on cursor internals. The server should reject malformed,
expired, tenant-mismatched, or query-mismatched cursors with a `400`-class
error rather than silently returning an inconsistent page.

Cursors must not embed raw subject values, label values, summaries, credentials,
or other sensitive query literals. They should carry a `query_hash` and ordering
state, not the query itself. If a deployment needs to put sensitive state in a
cursor, the cursor must be encrypted as well as integrity-protected.

### 8.6 Verify Journal

```text
GET /v1/journals/{journal_id}/verify?mode=full
```

Modes:

```text
full        recompute from genesis
from_seal   verify latest seal, then recompute entries after it
quick       compare stored head metadata only; not proof
```

Optional verification scopes:

```text
chain       validate genesis + entry hash chain
seals       validate seal envelopes/signatures for the checked range
projection  validate query projection rows against verified canonical entries
artifacts   fetch artifact bytes and validate artifact hashes
```

`verify` is read-only but can be CPU and database intensive. Servers may reject
expensive full verification for very large journals unless the caller has
`journal:verify`, or may return `202 Accepted` and run verification as a
background job in a later implementation.

### 8.7 Seal Journal

```text
POST /v1/journals/{journal_id}/seal
```

### 8.8 Error Semantics

The API should return stable machine-readable errors:

```text
400 bad_request          malformed JSON, malformed cursor, missing Idempotency-Key
401 unauthenticated      missing or invalid authentication
403 forbidden            authenticated but lacks journal permission
404 not_found            journal or entry is not visible to caller
409 conflict             idempotency key reused with different request/writer/source
410 gone                 artifact reference exists but bytes are unavailable by policy
413 payload_too_large    batch, summary, subject, or artifact limit exceeded
422 validation_failed    invalid journal/entry semantics, missing artifact, size mismatch
429 rate_limited         tenant or writer backpressure
503 unavailable          transient backend failure; retry with same Idempotency-Key
```

Error responses should use one JSON shape:

```json
{
  "error": {
    "code": "conflict",
    "message": "idempotency key reused with different request",
    "retryable": false,
    "request_id": "req_01J..."
  }
}
```

Append clients should retry only retryable failures, and must reuse the same
`Idempotency-Key` for a retried batch. `429` and retryable `503` responses
should include `Retry-After` when the server has a useful backoff estimate.

---

## 9. Entry Envelope

The server stores a normalized envelope. Clients submit a smaller entry shape.

Stored envelope:

```json
{
  "tenant_id": "tenant_123",
  "journal_id": "jrn_01J...",
  "seq": 1,
  "entry_id": "jre_01J...",
  "type": "tool.call.completed",
  "schema_version": 1,
  "status": "ok",
  "occurred_at": "2026-05-11T08:01:00Z",
  "observed_at": "2026-05-11T08:01:01Z",
  "actor": {
    "type": "agent",
    "id": "codex"
  },
  "source": "gateway_observed",
  "parent_entry_id": null,
  "correlation_id": "corr_...",
  "subjects": [
    "tool:exec_command",
    "command:go test ./..."
  ],
  "summary": {
    "cmd": "go test ./...",
    "exit_code": 0,
    "duration_ms": 842
  },
  "artifact_refs": [
    {
      "name": "stdout",
      "hash": "sha256:...",
      "content_type": "text/plain",
      "size_bytes": 1024
    }
  ],
  "prev_hash": "sha256:...",
  "entry_hash": "sha256:..."
}
```

`entry_id` is server-assigned. Clients should use `correlation_id` for
cross-system correlation and `Idempotency-Key` / `append_id` for retry
deduplication.

### 9.1 Source Semantics

`source` distinguishes evidentiary strength:

```text
gateway_observed    Tool gateway or command runner observed the operation.
server_observed     drive9 server observed the operation.
self_reported       Agent or client reported the operation directly.
imported            Entry was imported from another system.
```

This field matters. A self-reported entry is useful for debugging, but it is not
equivalent to a gateway-observed entry for audit evidence.

### 9.2 Source Assignment

The submitted `source` is a request, not an authority. The server decides the
stored source from the authenticated caller:

```text
ordinary CLI / SDK client        -> self_reported
trusted tool gateway / runner    -> gateway_observed
drive9 server middleware         -> server_observed
import endpoint with provenance  -> imported
```

If a caller requests a stronger source than its credential allows, the server
must either downgrade the source or reject the append. It must not silently
store untrusted client data as gateway- or server-observed evidence.

One append request should resolve to one effective source class. If a writer
needs to mix `self_reported`, `gateway_observed`, and `imported` records, it
should split them into separate append requests. This keeps idempotency records,
authorization, and audit interpretation simple.

### 9.3 Canonical Hash Input

The hash input should be a canonical JSON representation of the normalized
entry after server-side defaulting and source assignment, excluding fields that
cannot be known before hashing:

```text
entry_hash = "sha256:" + hex(sha256(canonical(
  tenant_id,
  journal_id,
  seq,
  entry_id,
  type,
  schema_version,
  status,
  occurred_at,
  observed_at,
  actor_type,
  actor_id,
  source,
  parent_entry_id,
  correlation_id,
  subjects,
  summary,
  artifact_refs,
  prev_hash
)))
```

Subject order should be canonicalized. The Phase 1 canonical JSON form is
server-defined and deliberately small: recursively sort object keys
lexicographically, preserve array order, emit strings with standard JSON
escaping, preserve valid JSON number tokens, and emit no insignificant
whitespace. Timestamps and string fields are normalized before canonicalization.
Clients may submit ordinary JSON; server-computed hashes are authoritative.
All stored hash strings should be algorithm-prefixed lowercase hex, such as
`sha256:<64 lowercase hex chars>`, so future algorithms can coexist without
schema changes.

All wire timestamps should be RFC 3339 UTC. The server should normalize
timestamps before hashing and storage, using the same precision as the schema
(`DATETIME(3)` in the initial TiDB/MySQL DDL). Database sessions must use UTC,
or the implementation must bind UTC values explicitly, so `observed_at`,
`created_at`, cursor ordering, and hash verification do not depend on server or
database local time zones. If a client submits higher precision timestamps, the
server-normalized value is the canonical evidence value returned to readers and
used for hashing.

`artifact_refs` in the hash input should include the canonical attachment name,
artifact hash, content type, and size. Hashing only the artifact hash is not
enough, because attachment names and declared metadata are part of the evidence
readers interpret.

Append idempotency uses a separate `request_hash` over the submitted entries
after request-level normalization but before sequence assignment, observed time,
and source elevation. That keeps retries stable even when server-assigned fields
would otherwise differ between attempts. The idempotency record also binds the
authenticated writer identity and effective source class; an append key replayed
by a different credential must be rejected rather than treated as a read handle
for an existing sequence range.

`writer_id` should be a stable authenticated principal ID, service account ID,
or gateway ID. It must not store bearer tokens, session IDs, API keys, or other
credential material.

Create idempotency uses `create_hash`, which is distinct from `genesis_hash`:

```text
create_hash  = hash(normalized create request before server-created timestamp)
genesis_hash = hash(immutable genesis document after server-created timestamp)
```

This lets a caller retry `journal new --id ...` safely without requiring the
caller to know the server-assigned `created_at`, while still letting full
verification recompute the initial journal head from immutable stored data.

### 9.4 Hot-Path Size Limits

The append API must keep hot rows bounded:

```text
max_entries_per_batch       implementation limit, default target: 500
max_batch_bytes             implementation limit, default target: 1-4 MiB
max_inline_summary_bytes    implementation limit, default target: 16-64 KiB
max_subjects_per_entry      implementation limit, default target: 64
max_subject_value_bytes     implementation limit, default target: 8 KiB
max_artifacts_per_entry     implementation limit, default target: 32
max_artifact_ref_bytes      implementation limit, default target: 4 KiB
max_labels_per_journal      implementation limit, default target: 64
max_label_value_bytes       implementation limit, default target: 4 KiB
```

Values over these limits should become artifacts or be rejected with a clear
error. The exact defaults can be tuned after load testing, but the invariant is
that `journal_entries`, `journal_labels`, and `journal_entry_subjects` stay
small enough for predictable TiDB write and index performance.

---

## 10. TiDB Schema

The DDL below is TiDB/MySQL-shaped. Provider-specific schema files may need
small type/index translations, but the logical keys and invariants should stay
the same.

### 10.1 `journals`

```sql
CREATE TABLE journals (
    tenant_id      VARCHAR(64)  NOT NULL,
    journal_id     VARCHAR(64)  NOT NULL,
    kind           VARCHAR(64)  NOT NULL,
    title          VARCHAR(255) NULL,
    actor_type     VARCHAR(64)  NULL,
    actor_id       VARCHAR(255) NULL,
    source         VARCHAR(64)  NULL,
    meta           JSON         NULL,
    retention      JSON         NULL,
    next_seq       BIGINT       NOT NULL DEFAULT 1,
    genesis        JSON         NOT NULL,
    create_hash    VARCHAR(128) NOT NULL,
    genesis_hash   VARCHAR(128) NOT NULL,
    head_hash      VARCHAR(128) NOT NULL,
    created_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    closed_at      DATETIME(3)  NULL,
    PRIMARY KEY (tenant_id, journal_id),
    KEY idx_kind_created (tenant_id, kind, created_at, journal_id),
    KEY idx_actor_created (tenant_id, actor_type, actor_id, created_at, journal_id)
);
```

On journal creation, the server canonicalizes an immutable genesis document and
stores it in `genesis`, with its hash in both `genesis_hash` and `head_hash`.
The genesis document should include at least `tenant_id`, `journal_id`, `kind`,
initial title/meta, actor, source, retention policy, and `created_at`.
Verification recomputes `genesis_hash` from the immutable `genesis` document,
not from mutable projection columns such as `title`, `meta`, or `retention`.
`next_seq` and `head_hash` are updated transactionally on append. They provide
the serialization point for a single journal.

The journal-list indexes include `journal_id` after `created_at` so listing by
kind or actor can use deterministic keyset pagination:

```text
created_at DESC, journal_id DESC
```

If the client supplies `journal_id`, create is an idempotent insert. The server
must compare `create_hash`, not only the ID. `create_hash` is computed from the
normalized create request after server-side defaults and source assignment, but
before server-assigned `created_at`. `genesis_hash` is computed after
`created_at` is assigned. If the same ID already exists with a different
`create_hash`, create returns `409 conflict`.

Mutable display metadata should be treated as projection state. If label or
metadata changes need audit strength, the server should append a
`journal.control.*` entry and then update the projection in the same
transaction.

### 10.2 `journal_entries`

```sql
CREATE TABLE journal_entries (
    tenant_id        VARCHAR(64)  NOT NULL,
    journal_id       VARCHAR(64)  NOT NULL,
    seq              BIGINT       NOT NULL,
    entry_id         VARCHAR(64)  NOT NULL,
    type             VARCHAR(128) NOT NULL,
    schema_version   INT          NOT NULL DEFAULT 1,
    status           VARCHAR(64)  NULL,
    occurred_at      DATETIME(3)  NOT NULL,
    observed_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    actor_type       VARCHAR(64)  NULL,
    actor_id         VARCHAR(255) NULL,
    source           VARCHAR(64)  NOT NULL,
    parent_entry_id  VARCHAR(64)  NULL,
    correlation_id   VARCHAR(128) NULL,
    subjects         JSON         NULL,
    summary          JSON         NULL,
    artifact_refs    JSON         NULL,
    prev_hash        VARCHAR(128) NOT NULL,
    entry_hash       VARCHAR(128) NOT NULL,
    PRIMARY KEY (tenant_id, journal_id, seq),
    UNIQUE KEY uk_entry_id (tenant_id, entry_id),
    KEY idx_type_observed (tenant_id, type, observed_at, journal_id, seq),
    KEY idx_type_status_observed (tenant_id, type, status, observed_at, journal_id, seq),
    KEY idx_status_observed (tenant_id, status, observed_at, journal_id, seq),
    KEY idx_actor_observed (tenant_id, actor_type, actor_id, observed_at, journal_id, seq),
    KEY idx_parent_observed (tenant_id, parent_entry_id, observed_at, journal_id, seq),
    KEY idx_correlation_observed (tenant_id, correlation_id, observed_at, journal_id, seq)
);
```

The `(observed_at, journal_id, seq)` suffix gives search results a stable
ordering tuple for keyset pagination. `observed_at` alone is not enough because
many entries can share the same millisecond timestamp.

`subjects` and `artifact_refs` are bounded canonical entry data, not query
indexes. They are stored in `journal_entries` so verification and projection
rebuild can be driven from the hash-chain table. Search still uses
`journal_entry_subjects` and `journal_entry_artifacts`, not JSON scans.

### 10.3 `journal_labels`

```sql
CREATE TABLE journal_labels (
    tenant_id    VARCHAR(64)  NOT NULL,
    label_key    VARCHAR(128) NOT NULL,
    label_hash   VARCHAR(128) NOT NULL,
    label_value  TEXT         NOT NULL,
    journal_id   VARCHAR(64)  NOT NULL,
    created_at   DATETIME(3)  NOT NULL,
    source_seq   BIGINT       NULL,
    PRIMARY KEY (tenant_id, label_key, label_hash, created_at, journal_id),
    UNIQUE KEY uk_label_journal (tenant_id, journal_id, label_key, label_hash),
    KEY idx_journal (tenant_id, journal_id)
);
```

`journals.meta` is useful for display, but `journal_labels` is the indexed path
for `journal find -m key=value`. Labels are multi-valued by default: one journal
may have several values for the same label key. An application that wants
single-valued labels must express replacement as a `journal.control.*` entry and
update this projection transactionally. `label_hash` is the algorithm-prefixed
hash over `label_key` and the normalized label value; it keeps the hot key
bounded while the original value remains available for collision checks and
output. `created_at` is the server time when the label projection became
visible, usually journal creation time for initial labels or the observed time
of the control entry that changed labels. The `created_at` component lets label
searches return recent journals without an extra sort over all matching
journals. `source_seq` is null for genesis labels and points to the
`journal.control.*` entry that last changed this projection when labels are
updated after creation.

There is intentionally no default plaintext prefix index on `label_value`.
Exact lookup uses `(tenant_id, label_key, label_hash)` and keeps `label_value`
only for collision checks and authorized output. Prefix or contains search over
labels should be a separate opt-in projection, not an accidental property of
the core audit schema.

### 10.4 `journal_append_requests`

```sql
CREATE TABLE journal_append_requests (
    tenant_id        VARCHAR(64)  NOT NULL,
    journal_id       VARCHAR(64)  NOT NULL,
    append_id        VARCHAR(128) NOT NULL,
    request_hash     VARCHAR(128) NOT NULL,
    writer_type      VARCHAR(64)  NOT NULL,
    writer_id        VARCHAR(255) NOT NULL,
    effective_source VARCHAR(64)  NOT NULL,
    first_seq        BIGINT       NOT NULL,
    last_seq         BIGINT       NOT NULL,
    count            INT          NOT NULL,
    head_hash        VARCHAR(128) NOT NULL,
    created_at       DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    expires_at       DATETIME(3)  NULL,
    PRIMARY KEY (tenant_id, journal_id, append_id),
    KEY idx_created (tenant_id, created_at),
    KEY idx_expires (tenant_id, expires_at)
);
```

This table provides idempotent retry for append batches. If a client retries the
same append with the same `Idempotency-Key`, request hash, writer identity, and
effective source, the server returns the original sequence range. If the key is
reused with different content, writer identity, or effective source, the server
returns a conflict. Authorization must be checked before consulting this table,
and an append key must not become a capability to discover another writer's
sequence range. `expires_at` allows old idempotency records to be
garbage-collected after the retry window without deleting the journal entries.
After expiry, the server may no longer detect a late retry as duplicate, so
clients must generate globally unique append IDs and treat the retry window as
part of the durability contract.

### 10.5 `journal_entry_subjects`

```sql
CREATE TABLE journal_entry_subjects (
    tenant_id      VARCHAR(64)  NOT NULL,
    subject_type   VARCHAR(64)  NOT NULL,
    subject_hash   VARCHAR(128) NOT NULL,
    subject_id     TEXT         NOT NULL,
    occurred_at    DATETIME(3)  NOT NULL,
    observed_at    DATETIME(3)  NOT NULL,
    journal_id     VARCHAR(64)  NOT NULL,
    seq            BIGINT       NOT NULL,
    entry_id       VARCHAR(64)  NOT NULL,
    PRIMARY KEY (
        tenant_id,
        subject_type,
        subject_hash,
        observed_at,
        journal_id,
        seq
    ),
    KEY idx_entry (tenant_id, entry_id),
    KEY idx_journal_seq (tenant_id, journal_id, seq)
);
```

This table is the main path for `journal find -s ...`.
`subject_hash` is the algorithm-prefixed hash over the normalized subject type
and subject ID. It keeps the hot primary key bounded even when the subject is a
long path, URL, command string, or external object ID. `subject_id` is retained
for collision checks and output, subject to redaction policy.
There is intentionally no default plaintext prefix index on `subject_id`.
Exact lookup uses `(tenant_id, subject_type, subject_hash)` and checks
`subject_id` after the bounded hash lookup.

### 10.6 `journal_artifacts`

```sql
CREATE TABLE journal_artifacts (
    tenant_id         VARCHAR(64)  NOT NULL,
    artifact_hash     VARCHAR(128) NOT NULL,
    storage_type      VARCHAR(32)  NOT NULL,
    storage_ref       TEXT         NOT NULL,
    encryption_mode   VARCHAR(32)  NOT NULL DEFAULT 'none',
    encryption_key_id VARCHAR(256) NOT NULL DEFAULT '',
    size_bytes        BIGINT       NOT NULL,
    detected_content_type VARCHAR(255) NULL,
    redacted          BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at        DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (tenant_id, artifact_hash),
    KEY idx_created (tenant_id, created_at)
);
```

Artifacts are content-addressed byte objects. `artifact_hash` identifies the
canonical evidence bytes after irreversible redaction and before storage
encryption or transport compression. `size_bytes` is the byte length of that
canonical content. `detected_content_type` is only a storage hint; entry-level
declared content type belongs to `artifact_refs` and `journal_entry_artifacts`.
`detected_content_type` is not part of the entry hash and may be refreshed by
storage maintenance; declared attachment `content_type` is canonical evidence.
This keeps verification independent of encryption IVs or S3 multipart layout.
Multiple entries can reference the same artifact hash. Sensitive raw content
should be stored only behind an explicit artifact access policy; do not rely on
an unsalted content hash as the only protection for a guessable secret.

### 10.7 `journal_entry_artifacts`

```sql
CREATE TABLE journal_entry_artifacts (
    tenant_id      VARCHAR(64)  NOT NULL,
    entry_id       VARCHAR(64)  NOT NULL,
    name           VARCHAR(128) NOT NULL,
    artifact_hash  VARCHAR(128) NOT NULL,
    content_type   VARCHAR(255) NULL,
    size_bytes     BIGINT       NOT NULL,
    PRIMARY KEY (tenant_id, entry_id, name),
    KEY idx_artifact (tenant_id, artifact_hash)
);
```

This keeps several artifacts per entry out of `journal_entries` and keeps large
evidence out of the hot entry table.

Attachment names are evidence, not just display labels. The entry hash commits
to the canonical `(name, artifact_hash, content_type, size_bytes)` tuple, and
`journal_entry_artifacts` is the query projection of those committed
references. The same artifact bytes may be attached to different entries with
different names or declared content types without creating a new
`journal_artifacts` byte object. `size_bytes` in `artifact_refs` and
`journal_entry_artifacts` must equal `journal_artifacts.size_bytes`; a mismatch
is either an invalid append request or projection corruption.

### 10.8 `journal_seals`

```sql
CREATE TABLE journal_seals (
    tenant_id      VARCHAR(64)  NOT NULL,
    journal_id     VARCHAR(64)  NOT NULL,
    seal_id        VARCHAR(64)  NOT NULL,
    seq            BIGINT       NOT NULL,
    head_hash      VARCHAR(128) NOT NULL,
    seal_type      VARCHAR(32)  NOT NULL DEFAULT 'soft',
    signer         VARCHAR(128) NULL,
    signer_key_id  VARCHAR(256) NULL,
    signature_alg  VARCHAR(64)  NULL,
    signature      TEXT         NULL,
    sealed_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (tenant_id, journal_id, seal_id),
    KEY idx_journal_seq (tenant_id, journal_id, seq),
    KEY idx_sealed_at (tenant_id, sealed_at)
);
```

The signed seal payload must include `seal_type`, `signer_key_id`, and
`signature_alg`. Otherwise a valid checkpoint signature could be replayed with a
different seal policy or interpreted under the wrong key.
`seal_id` allows several signatures or anchors for the same journal checkpoint.
For a hard seal, inserting the seal and setting `journals.closed_at` must happen
in the same transaction.

---

## 11. Append Protocol

Append must be serialized per journal, but can scale across journals.

Transaction flow:

1. Normalize and validate the batch.
2. Compute the request hash from the canonical submitted entries and resolve the
   authenticated writer identity.
3. Require an append ID / `Idempotency-Key` for the audit-safe path. Keyless
   compatibility mode, if present, must branch explicitly before this protocol
   and mark the response non-idempotent.
4. Lock or conditionally update the `journals` row for
   `(tenant_id, journal_id)`.
5. Check `journal_append_requests` for the same `Idempotency-Key` inside the
   journal lock. If it exists with the same request hash, writer identity, and
   effective source, return the recorded sequence range. If it exists with
   different content, writer identity, or effective source, reject the append as
   a conflict.
6. Check `closed_at` and the current seal policy while holding the journal
   serialization point. Reject appends to a hard-sealed or closed journal unless
   an explicit segment policy permits the append.
7. Read `next_seq` and `head_hash`.
8. Assign sequence numbers.
9. Validate referenced artifact byte objects and attachment sizes.
10. Compute each entry hash in order.
11. Insert `journal_append_requests`. This insert is in the same transaction as
    the entries and uses the final sequence range, authenticated writer
    identity, and effective source.
12. Insert `journal_entries`.
13. Insert `journal_entry_subjects`.
14. Insert artifact references.
15. Update `journals.next_seq`, `journals.head_hash`, and
    `journals.updated_at`.
16. Commit.

The implementation can use row locking or optimistic conditional update.
If optimistic update is used, the idempotency re-check must happen after a
successful compare-and-swap of the journal head, or the implementation must
retry the whole append transaction.

Atomicity requirement:

```text
One append request commits all entries in the batch, or none.
```

Ordering requirement:

```text
Sequence order is the authoritative stream order.
occurred_at is client/event time and may be out of order.
observed_at is server time.
```

Immutability requirement:

```text
Committed journal entries are never updated in place.
Corrections, redactions, or reinterpretations are appended as new entries.
```

If an entry is wrong, write a correction entry, such as
`journal.correction` or a domain-specific correction type, with
`parent_entry_id` pointing to the corrected entry and a subject like
`entry:jre_01J...`. Do not mutate `summary`, `subjects`, `artifact_refs`,
`source`, `status`, `prev_hash`, or `entry_hash` after commit. If retention
removes data, record that as lifecycle state or tombstone metadata, not as an
edit to historical entries.

---

## 12. Query Semantics

### 12.1 Sequential Read

`journal cat` uses:

```sql
SELECT *
FROM journal_entries
WHERE tenant_id = ? AND journal_id = ? AND seq > ?
ORDER BY seq
LIMIT ?;
```

`journal cat` pagination is sequence based. Clients should resume with the last
seen `seq`, not with SQL offsets.

### 12.2 Subject Search

`journal find -s file:/path` uses `journal_entry_subjects`:

```sql
SELECT
    e.journal_id,
    e.seq,
    e.type,
    e.status,
    e.observed_at,
    e.source
FROM journal_entry_subjects s
JOIN journal_entries e
  ON e.tenant_id = s.tenant_id
 AND e.journal_id = s.journal_id
 AND e.seq = s.seq
WHERE s.tenant_id = ?
  AND s.subject_type = ?
  AND s.subject_hash = ?
  AND s.subject_id = ?
  AND s.observed_at >= ?
ORDER BY s.observed_at DESC, s.journal_id DESC, s.seq DESC
LIMIT ?;
```

`subject_id` remains in the predicate to guard against hash collisions.
Pagination uses the same stable tuple as the subject index:
`(observed_at, journal_id, seq)`.

### 12.3 Label Search

`journal find -m repo=github.com/mem9-ai/drive9` uses `journal_labels`:

```sql
SELECT
    j.journal_id,
    j.kind,
    j.title,
    j.created_at
FROM journal_labels l
JOIN journals j
  ON j.tenant_id = l.tenant_id
 AND j.journal_id = l.journal_id
WHERE l.tenant_id = ?
  AND l.label_key = ?
  AND l.label_hash = ?
  AND l.label_value = ?
ORDER BY l.created_at DESC, l.journal_id DESC
LIMIT ?;
```

Label pagination uses `(created_at, journal_id)` as the keyset tuple.

These examples show the default match projection. `include=entry` or
`journal find --entries` can select full entry envelopes only after the caller
passes `journal:read` authorization.

### 12.4 Type Search

`journal find -t tool.call.completed` uses `idx_type_observed`.

`journal find -t tool.call.completed --status error` uses
`idx_type_status_observed`. Status-only searches use `idx_status_observed`.
Actor searches use `idx_actor_observed`. Correlation lookups use
`idx_correlation_observed` and should return results in observed-time order.
Parent/child lookups use `idx_parent_observed`.

`occurred_at` filters are event-time filters. They may be post-filters unless a
future workload justifies dedicated occurred-time indexes.

All search paths should use keyset pagination. Offset pagination is unstable
under concurrent appends and becomes increasingly expensive on large journals.
The default ordering is:

```text
observed_at DESC, journal_id DESC, seq DESC
```

Label-only journal searches use:

```text
created_at DESC, journal_id DESC
```

### 12.5 JSON Summary Search

JSON summary fields should not be the first-line query mechanism. If a summary
field becomes operationally important, promote it to:

- a typed column,
- a subject,
- or a purpose-built projection table.

### 12.6 Multiple Filter Strategy

Repeated `--subject` and `--meta` filters are AND filters. The implementation
should choose one indexed predicate as the anchor scan, then apply the remaining
filters as existence joins against `journal_entry_subjects`, `journal_labels`,
or `journals`.

The query planner should prefer anchors in this rough order:

```text
exact entry_id / correlation_id
subject with exact hash
label with exact hash
type + status + time
type + time
actor + time
status + time
```

If a query has no selective indexed anchor, the server should require a time
bound and a small limit, or reject it with a clear error. The first
implementation should not fall back to scanning `summary` JSON to satisfy
compound filters.

The first implementation should not offer prefix, contains, fuzzy, or regex
search over subjects or labels. Those modes require explicit projections with
separate privacy and performance review. The core journal indexes are exact
match indexes.

### 12.7 Projection Rebuild

The canonical evidence is the journal genesis document plus the ordered
`journal_entries` stream and referenced artifact bytes. The following tables are
query projections:

```text
journal_labels
journal_entry_subjects
journal_entry_artifacts
```

They should be rebuildable from genesis metadata, entry subjects, entry
artifact references, and `journal.control.*` entries. Verification should check
the hash chain first, then optionally validate that projection rows match the
canonical evidence for the verified sequence range. A projection mismatch is an
index corruption or stale projection problem; it should not be treated as new
canonical evidence.

---

## 13. Artifact Storage

Artifact policy:

```text
small artifact     db9 small-file storage or inline artifact backend
large artifact     S3 or drive9 blob storage
sensitive artifact encrypted and redacted by default
```

The journal entry table stores only references and hashes. Even small artifacts
should stay out of `journal_entries`; "inline" here means inline in the artifact
storage backend, not inline in the hot entry row.

Artifact bytes should be uploaded before append in the first implementation.
If the append later fails, the uploaded artifact is an unreferenced byte object,
not evidence. A background GC may remove unreferenced artifacts after a grace
period, but must not remove artifacts referenced by retained entries unless an
artifact retention policy explicitly permits that loss of evidence.
The upload path is content-addressed and idempotent: uploading the same bytes
again returns the same artifact object, while bytes that do not match the
declared hash are rejected before storage is made visible.
Artifact fetch should be entry-scoped. A caller reads bytes through a committed
attachment reference and `journal:read`, not through possession of
`artifact_hash` alone.

Artifact byte-object metadata is immutable after first reference. If content
bytes, byte length, encryption metadata, or storage location must change, the
implementation should create a new artifact row or a storage-level redirect
that preserves the verified artifact hash. Entry-level attachment metadata such
as `name` and declared `content_type` lives in `artifact_refs` and
`journal_entry_artifacts`; it must not be changed in a way that makes an
existing entry's `artifact_refs` unverifiable.

Recommended artifact names:

```text
input
output
stdout
stderr
request
response
diff
screenshot
trace
```

Artifact fetching should require explicit CLI/API action. `journal cat` should
not accidentally dump secrets or megabytes of output.

Artifact verification has two levels:

```text
entry verification      recompute genesis + entry hash chain and committed refs
artifact verification   also fetch artifact bytes and compare artifact_hash
```

If artifact bytes have been archived or deleted by policy, entry verification
can still succeed, but artifact verification must report the artifact as
unavailable rather than silently treating the evidence as complete.

---

## 14. Redaction and Secret Handling

Default behavior:

1. Do not store bearer tokens, API keys, passwords, DSNs, or Authorization
   headers in plaintext summaries.
2. Redact known secret patterns before storing inline summaries.
3. Store sensitive artifacts encrypted and mark them `redacted=true` when the
   visible artifact is a sanitized version.
4. Preserve hashes for integrity even when values are redacted.

Example summary:

```json
{
  "url": "https://api.github.com/repos/mem9-ai/drive9",
  "authorization": "[REDACTED]",
  "authorization_fingerprint": "hmac-sha256:..."
}
```

Use keyed fingerprints, such as HMAC-SHA256 with a server-held key, for redacted
secret values. Raw SHA-256 fingerprints are acceptable for high-entropy content
addressing, but they are a poor default for passwords, DSNs, tokens with known
prefixes, or other values that may be guessed offline.

---

## 15. Integrity Model

### 15.1 Hash Chain

Each entry commits to the previous head:

```text
initial head = genesis_hash
entry_hash = hash(prev_hash, normalized_entry)
head_hash = entry_hash of latest entry
```

This detects deletion, insertion, reordering, or mutation inside a journal.
Because the initial head is the journal genesis hash, verification also covers
the immutable creation envelope.

### 15.2 Limits of Hash Chains

A hash chain alone does not stop a database administrator from rewriting the
entire chain. Stronger evidence requires seals whose signing key is outside the
tenant data plane.

### 15.3 Sealing

Seal options:

```text
soft seal     record checkpoint; appends still allowed after the checkpoint
hard seal     close journal; future appends rejected
segment seal  start a new segment after each seal
```

The first implementation should support soft seal. Hard seal can be added when
compliance requirements are clear. An unsigned soft seal is only a local
checkpoint that accelerates verification; it is not evidence against a database
administrator rewriting the journal. Audit-strength seals should include a
signature or external anchor whose key material is outside the tenant data
plane.

### 15.4 Verification Cost

Full verification is `O(number_of_entries)` because it recomputes the hash
chain. That is acceptable for small journals and incident investigation, but a
large long-lived journal needs checkpoints.

Soft seals can double as verification checkpoints:

```text
verify from genesis      recompute the whole chain
verify from last seal    verify seal signature, then recompute entries after it
verify head only         compare stored head metadata; fast but not proof
```

`journal verify` should default to full verification for correctness. A future
`--from-seal` or `--quick` mode can trade strength for latency explicitly.

---

## 16. Trust Boundaries

Not all entries have the same evidentiary strength.

```text
self_reported
  Written by the agent or client. Useful for debugging, weak for audit.

gateway_observed
  Written by a tool gateway or command runner that observed the operation.
  Stronger for tool-call audit.

server_observed
  Written by drive9 server middleware while serving filesystem, vault, journal,
  or API operations.

imported
  Imported from external logs. Integrity depends on import source.
```

The design should encourage gateway/server-side recording for audit-sensitive
operations. The CLI remains useful for self-reported or imported entries.

### 16.1 Authorization and Tenancy

Journal access is tenant-scoped. The minimum permission split should be:

```text
journal:create
journal:append
journal:read
journal:find
journal:verify
journal:seal
journal:artifact:write
journal:admin
```

Append permission alone should not imply read permission. This allows a tool
gateway to record evidence without being able to read unrelated journal data.
`journal:append` can include artifact upload for the append path, or a
deployment can require the narrower `journal:artifact:write` permission for
pre-uploaded artifacts. Neither permission grants artifact read access.

`journal:find` should be treated as metadata/index access, not full content
access. A strict deployment can have `find` return only journal IDs, sequence
numbers, entry types, status, observed time, and matched filter values. Full
subject lists, label values, summaries, and artifact metadata require
`journal:read` or an explicitly defined metadata-read policy. The CLI may print
full JSONL entries only for `journal cat` or `journal find --entries`, and only
when the caller has read permission.

Artifact byte reads require `journal:read` on a journal entry that references
the artifact. Knowing an `artifact_hash` must not be sufficient to read the
bytes.

Source elevation requires an additional trusted-writer permission:

```text
journal:source:gateway_observed
journal:source:server_observed
journal:source:imported
```

Ordinary user and agent credentials should only create `self_reported` entries.
Server middleware uses internal credentials for `server_observed` entries.
Tool gateways and command runners use scoped credentials for
`gateway_observed` entries.

API-key tokens may carry a `journal_permissions` claim containing the exact
journal permissions for that credential. Tokens without this claim are legacy
owner credentials and map to `journal:admin`; new least-privilege credentials
should include the claim explicitly. `journal:admin` is a wildcard for all
journal permissions, including source-elevation permissions.

### 16.2 Recursion Control

Journal writes must not recursively journal themselves by default. If server
middleware records drive9 API activity, it should suppress journal API
append/read/verify operations or route them to a separate system journal with an
explicit recursion guard.

Without this guard, `server_observed` logging of `POST /v1/journals/.../entries`
can create an infinite journal-write loop. The implementation should carry a
request-scoped "journal recording suppressed" marker for internal journal
writes.

---

## 17. Performance

### 17.1 Write Path

The only serialization bottleneck is per-journal sequence assignment and hash
chain update. This is acceptable because a single journal represents one
ordered stream.

Scaling properties:

- Many journals append concurrently.
- One journal appends serially.
- Batch append amortizes transaction overhead.
- Large payloads bypass the hot entry table.

Recommended batch size:

```text
10-500 entries per append request, depending on payload size
```

### 17.2 Producer-Inspired Append Path

Kafka producer design has useful ideas for journal performance, but the journal
service should not become Kafka. The reusable primitive is a long-lived
append writer with batching, retry, idempotence, and backpressure.

The recommended SDK/gateway writer should behave like this:

```text
JournalWriter
  input: entries
  group key: journal_id
  accumulator: one queue per journal_id
  flush triggers: batch_count, batch_bytes, linger
  ordering: max_in_flight_per_journal = 1 by default
  parallelism: many journals flush concurrently
  retry: exponential backoff until delivery timeout
  idempotence: one append_id / Idempotency-Key per batch
  ack: return success only after TiDB commit by default
```

Kafka concepts mapped to journal:

| Kafka producer concept | Journal equivalent | Recommendation |
|---|---|---|
| Topic partition | `journal_id` | Treat each journal as one ordered partition. |
| Record accumulator | Per-journal append buffer | Use in SDK, tool gateway, and long-running agent runtime. |
| `batch.size` | `batch_bytes` / `batch_count` | Flush on either size or count. |
| `linger.ms` | `linger` | Small delay, such as 5-25ms, to coalesce bursts. |
| `acks` | commit acknowledgment | Default must be "TiDB committed". Avoid lossy acks for audit. |
| Idempotent producer | `Idempotency-Key` + `journal_append_requests` | Make retries duplicate-safe. |
| `max.in.flight` | per-journal in-flight limit | Keep `1` per journal unless ordering tradeoffs are explicit. |
| Buffer memory | max queued bytes/entries | Apply backpressure rather than silently dropping audit entries. |
| Compression | HTTP/content artifact compression | Compress request bodies/artifacts, not indexed columns. |

The CLI should remain simple. `drive9 journal append` can expose optional
`--batch-count`, `--batch-bytes`, and `--linger` flags, but high-throughput
writers should use a library or gateway process instead of spawning the CLI for
every entry.

Default reliability policy:

```text
ack=commit
idempotence=on
max_in_flight_per_journal=1
drop_on_overflow=false
```

With `max_in_flight_per_journal=1`, the writer must not send batch `N+1` for a
journal until batch `N` has committed, returned an idempotent duplicate success,
or failed permanently. On transient failure, the writer retries the same batch
with the same append ID before releasing later batches. This is the key rule
that preserves sequence order while still allowing many journals to flush in
parallel.

An optional async mode can acknowledge after a local durable spool fsync and
flush to TiDB in the background. That is useful for low-latency observability,
but it should be explicitly marked weaker than server-committed audit evidence.
The spool format must persist the append ID, journal ID, batch order, and
canonical request bytes so restart recovery can retry without reordering or
duplicating committed batches.

What should not be copied from Kafka:

- Do not require a Kafka broker for the journal subsystem.
- Do not split one journal across multiple partitions by default; it weakens
  the simple total order and hash chain.
- Do not use lossy `acks=0` style behavior for audit-sensitive data.
- Do not store queryable summary fields only inside compressed opaque batches.

If one journal needs very high write throughput, add explicit journal channels
or segments later:

```text
journal_id + channel -> independent ordered chain
journal seal         -> Merkle/root checkpoint across channels
```

That is a deliberate tradeoff: it improves write parallelism but gives up a
single total order unless the reader merges channels.

### 17.3 Read Path

Sequential reads are efficient via:

```text
PRIMARY KEY (tenant_id, journal_id, seq)
```

Follow mode can start with polling or long polling. It does not require a new
storage primitive.

### 17.4 Search Path

Common searches use:

```text
journal_entries.idx_type_observed
journal_entries.idx_type_status_observed
journal_entries.idx_actor_observed
journal_entries.idx_status_observed
journal_entries.idx_parent_observed
journal_entries.idx_correlation_observed
journal_labels primary key
journal_entry_subjects primary key
```

This avoids scanning the JSON summary. Indexes that serve search results should
include the pagination tie-breaker columns needed by their ordering tuple; do
not rely on nondeterministic ordering for rows with identical timestamps.

### 17.5 Retention

Journals can grow quickly. Retention policy should be part of the metadata:

```text
keep_hot_for
keep_artifacts_for
archive_after
delete_after
```

Large artifacts should have independent retention from entry metadata.
Retention is a storage lifecycle policy, not a journal entry mutation. When a
journal is expired, verification beyond the retained range is no longer
possible unless seals or exported archives were preserved. Compliance-oriented
tenants should prefer archive-before-delete over direct deletion.

Artifact retention can make artifact bytes unavailable while keeping entry
metadata and the hash chain. In that case verification must distinguish
`hash_chain_ok` from `artifact_bytes_available`; a retained journal segment
with missing artifacts is not a complete evidence package.

Retention should not silently create a journal that appears fully verifiable
when only a suffix remains. If entries are pruned, the journal metadata or seal
metadata must record the retained sequence range and the verification mode must
report that it is verifying a retained segment, not the full original journal.
Hard audit deployments should delete or archive whole sealed journals rather
than pruning arbitrary entry prefixes.

Retention must not permit journal ID reuse. If a journal is physically deleted,
the system should retain a compact tombstone containing at least tenant ID,
journal ID, deletion time, and final head/seal metadata, or use an equivalent
ID allocation rule that makes reuse impossible.

### 17.6 Partitioning

If journal volume grows high enough, partition by time or tenant/time. The
logical schema should not depend on a specific partitioning strategy in the
first implementation.

---

## 18. Compatibility with Existing drive9

The journal subsystem is separate from the normal filesystem metadata tables.
The `/.journal/` projection is a virtual namespace backed by journal tables,
not ordinary user files.

This preserves special journal invariants:

- append-only writes
- sequence assignment
- hash chain
- seals
- subject indexing
- artifact redaction policy

---

## 19. Rollout Plan

### Phase 0: Spec and Validation

- Land this design spec.
- Review naming, CLI, and schema.
- Decide whether `journal seal` is in v1 or v1.1.

### Phase 1: Server and CLI MVP

- Add `journals`, `journal_labels`, `journal_append_requests`,
  `journal_entries`, and `journal_entry_subjects`.
- Implement `journal new`, `append`, `cat`, `find`, and `verify`.
- Support JSONL input/output.
- Enforce `Idempotency-Key` for append by default.
- Store only inline summaries, labels, and subject indexes.

### Phase 2: Artifacts and Redaction

- Add `journal_artifacts` and `journal_entry_artifacts`.
- Add artifact upload/reference support.
- Add redaction policy for summaries and artifacts.

### Phase 3: Filesystem Projection

- Expose `/.journal/clone`, `entries`, `ctl`, and `head`.
- Support append-only semantics through FUSE/WebDAV where practical.

### Phase 4: Seals and Stronger Audit

- Add soft seals.
- Add KMS-backed signatures or external anchoring.
- Add policies for hard seal or segment seal if needed.

### Phase 5: Tool Gateway Integration

- Record gateway-observed tool calls.
- Record command execution via `journal append` or a dedicated runner adapter.
- Ensure self-reported entries are visibly distinct from gateway-observed
  entries.

---

## 20. Open Questions

1. Should hard seal be included in the first implementation, or only soft seal?
2. Should `journal append` accept `--artifact @path` in v1, or should artifacts
   wait for phase 2?
3. Should `/.journal/` be visible in ordinary `drive9 fs ls /`, or hidden behind
   an explicit system namespace flag?
4. What is the default retry window / `journal_append_requests.expires_at`
   policy?
5. What are the default batch, summary, subject, and artifact size limits after
   load testing?
6. What is the default retention policy for journal entries and artifacts?
7. Which component should be the first gateway-observed writer: CLI runner,
   drive9 server middleware, or an external agent tool gateway?
8. Should keyless append compatibility mode exist at all, or should imports
   also be forced through generated idempotency keys?

---

## 21. Recommended First Cut

Build the smallest useful version:

```bash
drive9 journal new
drive9 journal append <journal>
drive9 journal cat <journal>
drive9 journal find
drive9 journal verify <journal>
```

Back it with:

```text
journals
journal_labels
journal_append_requests
journal_entries
journal_entry_subjects
```

Defer:

```text
artifacts
filesystem projection
KMS seals
tool gateway integration
```

This keeps the first implementation small while preserving the high-level
design: journal as a general append-only, queryable, verifiable record.

---

## 22. Validation Checklist

Implementation should include focused tests for:

1. Append ordering: concurrent appends to one journal produce a gap-free
   sequence and valid hash chain.
2. Create idempotency: retrying `journal new` with the same caller-provided
   journal ID and create envelope returns the same journal; reusing the ID with
   different metadata returns conflict.
3. Genesis storage: full verification recomputes `genesis_hash` from the
   immutable stored `genesis` document even after mutable title/meta projection
   fields change.
4. Idempotency: retrying the same `Idempotency-Key` returns the same sequence
   range; reusing the key with different content, a different writer identity,
   or a different effective source returns conflict.
5. CLI idempotency: `journal append` sends an idempotency key by default, while
   supervised retries can reuse an explicit key across processes.
6. Source assignment: ordinary clients cannot create `gateway_observed` or
   `server_observed` entries.
7. Seal/append race: a hard seal or close racing with append cannot commit both
   an unauthorized append and a closed journal state.
8. Query indexes: `find` by type/status/actor/subject/label returns results in
   deterministic observed-time order without JSON scans.
9. Genesis verification: mutating journal creation metadata or the first entry
   breaks full verification.
10. Redaction: known secret inputs are not stored in inline summaries or subject
   values; fingerprints use keyed HMAC where applicable.
11. Retention: deleting expired idempotency records does not affect journal entry
   verification.
12. Retention tombstone: physical journal deletion does not allow the same
    tenant/journal ID to be reused as a different journal.
13. Recursion guard: server-observed journal API logging cannot recursively write
   journal entries.
14. Entry immutability: corrections append new entries and never update
    committed `journal_entries` rows in place.
15. Projection rebuild: subject, label, and artifact projection rows can be
    rebuilt from genesis, verified entries, and control entries.
16. Canonical entry storage: `journal_entries.subjects` and
    `journal_entries.artifact_refs` are sufficient to rebuild subject and
    artifact projections for a verified range.
17. Artifact model: the same artifact hash can be reused with different
    attachment names or declared content types without mutating the byte-object
    row.
18. Artifact reference integrity: changing an attachment name, declared content
    type, or attachment size breaks entry verification.
19. Artifact size check: attachment `size_bytes` must match the referenced
    artifact byte-object size.
20. Artifact reference validation: append fails atomically if any referenced
    artifact is missing or has mismatched size.
21. Artifact upload idempotency: uploading the same artifact bytes is safe to
    retry, and bytes that do not match the declared hash are rejected.
22. Artifact retention reporting: verification distinguishes a valid entry hash
    chain from complete artifact byte availability.
23. Artifact access control: knowing `artifact_hash` is not enough to read
    bytes; reads are authorized through a journal entry attachment reference.
24. Artifact upload quota: unreferenced uploaded artifacts count against
    temporary quota and can be garbage-collected after a grace period.
25. Permission boundary: callers with `journal:find` but not `journal:read` do
    not receive inline summaries, full subject lists, label values, or artifact
    metadata unless an explicit metadata-read policy allows it.
26. Exact metadata search: core subject and label indexes support exact lookup
    only; prefix/fuzzy/regex search requires a separate opt-in projection.
27. Search shape: default `find` / search API responses are match references;
    full entries require an explicit option and `journal:read`.
28. Search pagination: repeated pages use keyset cursors and keep deterministic
    ordering when many entries share the same timestamp.
29. Cursor integrity: malformed, tenant-mismatched, expired, or query-mismatched
    cursors are rejected rather than interpreted as a different page, and cursor
    payloads do not expose sensitive query literals.
30. API errors: missing idempotency key, idempotency conflict, payload limit,
    permission denial, and retryable backend failure return stable error codes.
31. Seal envelope: seal verification binds seal ID, seal type, signer key,
    signature algorithm, journal ID, sequence, and head hash.
32. Producer ordering: the SDK/gateway writer does not release later batches for
    a journal while an earlier batch is retrying.
33. Artifact integrity: artifact verification uses the canonical redacted bytes
    before encryption or transport compression.
34. Verification scopes: verify results distinguish hash-chain, seal,
    projection, and artifact-byte availability status.
35. Timestamp canonicalization: wire timestamps, stored `DATETIME(3)` values,
    cursor ordering, and hash inputs all use the same UTC-normalized precision.
