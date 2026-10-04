# Drive9 Runtime P1

Drive9 Runtime is an optional, default-off API for running shell/argv commands
and file operations against a durable private workspace. The server controls
the Docker runner and publishes a verified workspace checkpoint before a
durable terminal result becomes visible.

Check capability support first:

```bash
drive9 runtime capabilities
```

The capability response includes the server-enforced queue/active-operation,
workspace, path, output, execution, capture, and checkpoint limits plus the
approved/default profile. Clients must treat those values as authoritative
rather than assuming local defaults.

Create a one-shot private workspace from a Drive9 root, or create a named
workspace and reuse the returned `workspace_ref`:

```bash
drive9 exec --workspace :/projects/demo --profile go-build -- go test ./...
drive9 exec --root :/projects/demo --scope-key example-run -- go test ./...
drive9 exec --workspace :/projects/demo --timeout 2m --shell-script 'go test ./... | tee test.log'
drive9 exec --workspace :/projects/demo --detach -- long-running-command
drive9 execution logs exe_... --follow
drive9 runtime fs read --workspace-ref wr_... /test-output.txt
drive9 execution get exe_...
```

The CLI writes a mode-`0600` idempotency receipt before an automatically keyed
submission and removes it once the server acknowledges the durable operation.
If the submit result is ambiguous, the error reports the retained path; retry
the identical command with `--idempotency-receipt PATH`. A new manual command
still generates a new key. Applications using the SDK must persist their own
stable key before the first request and reuse it after an ambiguous response.

## Go SDK

```go
capabilities, err := drive9Client.RuntimeCapabilities(ctx)
operation, err := drive9Client.SubmitRuntimeExecution(ctx, "tool-call-42", client.RuntimeExecutionRequest{
    Workspace: &client.RuntimeWorkspaceInput{
        ClientScopeKey: "agent-branch-42",
        Source: client.RuntimeWorkspaceSource{Root: "/projects/demo"},
    },
	Profile: "go-build",
    Execution: client.RuntimeExecutionSpec{Argv: []string{"go", "test", "./..."}},
})
```

`WatchRuntimeEvents` reconnects after bounded SSE windows and resumes from the
last callback-acknowledged cursor. Transport errors, HTTP 429, and HTTP 5xx are
retried without advancing that cursor; callback failures are returned to the
caller without acknowledging the event. `DownloadRuntimeArtifact` requires and
verifies the server's SHA-256 header before returning bytes.

## TypeScript SDK

```ts
import { Client } from "drive9";

const client = Client.defaultClient();
await client.runtimeCapabilities();
const operation = await client.submitRuntimeExecution("tool-call-42", {
  workspace: {
    client_scope_key: "agent-branch-42",
    source: { root: "/projects/demo" },
  },
	profile: "go-build",
  execution: { argv: ["go", "test", "./..."] },
});
```

The TypeScript event watcher also reconnects and resumes by cursor. Artifact
downloads reject missing or mismatched SHA-256 metadata.

## Semantics and boundaries

- Operations on one workspace are serialized.
- CLI execution waits for a terminal durable result by default; `--detach`
  returns after submission.
- `drive9 execution logs ID` replays the sealed log for a terminal execution.
  `--follow` resumes durable output events by cursor until terminal state, then
  verifies the replay is an exact prefix of the checksum-verified log artifact
  before printing the remainder. It never reruns the command.
- While waiting, Ctrl-C sends a durable cancellation request and then waits up
  to two minutes for the server's actual terminal state. It does not print a
  synthetic local cancellation result.
- Read/list/find/grep return the persisted result captured when that operation
  ran, not a later live view.
- Read supports files up to 4 MiB and fails explicitly above that limit. List,
  find, and grep default to 1,000 results and accept a limit up to 10,000; their
  result includes `truncated`, and list also includes `total_entries`. Grep
  rejects any input file larger than 16 MiB and returns matching paths only,
  never matching line bodies. Search result paths are capped at 4 MiB in
  aggregate, and stored result JSON is capped at 8 MiB.
- `edit` is a compare-and-replace operation over the complete file. It requires
  the caller's expected SHA-256 and does not implement a textual patch format.
- A non-zero command may still have durable file changes and is reported as a
  failed operation with its checkpoint.
- Operations finalized through durable publication include raw `usage`
  observations: execution wall time, bytes and entries scanned across the
  pre/post full scans, changed bytes staged, and retained log bytes. A failed
  or unknown operation that never reaches publication may report zero when its
  measurements were not durably staged. P1 does not turn these measurements
  into a monetary bill or budget.
- `outcome_unknown` requires explicit `recover`; recovery uses the last
  published checkpoint and never silently reruns a command.
- Recovery requires an explicit choice. `--discard-uncommitted` restores a
  fresh generation from the published checkpoint after old-resource cleanup;
  `--reconcile` never silently falls back to discard when retained evidence is
  insufficient.
- Arbitrary shell commands can change external systems. Drive9 does not provide
  exactly-once external effects or automatic command replay.
- Shell, argv, and explicit environment values are persisted with the
  operation and sent to the runner. P1 does not provide secret injection; do
  not pass credentials through command arguments, scripts, `--env`, or the SDK
  `environment` field.
- Runtime results remain in a private LayerFS view. P1 does not publish them
  into the source root and does not change existing filesystem or FUSE paths.
- P1 has no public Sandbox/session CRUD, provider selection, mount mode, PTY,
  background-process lifecycle, whole-project export, or completed Pi binding.
