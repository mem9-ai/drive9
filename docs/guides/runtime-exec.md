# One-shot Runtime exec

Drive9 Runtime is an optional, non-durable adapter for running one process
against an existing persistent Drive9 workspace. It does not own task state,
retry, replay, history, artifacts, or recovery. Agent frameworks such as Pi
remain authoritative for those decisions.

## CLI

```bash
drive9 exec \
  --workspace :/repo \
  --cwd . \
  --timeout 10m \
  --memory-mb 2048 \
  --env NODE_ENV=test \
  -- npm test
```

The command writes remote stdout and stderr to the matching local streams and
returns the process exit status. `--workspace` defaults to `:/`. Resource flags
are minimum requirements used by the server's hard candidate filter; they do
not let a caller name an operator profile.

Interrupting the client requests remote cancellation. The CLI reports canceled
only after the server confirms that the process stopped and bounded workspace
cleanup completed; otherwise it reports that the outcome is unknown. It never
retries.

## Go SDK

```go
result, err := c.Exec(ctx, client.ExecRequest{
    Argv: []string{"go", "test", "./..."},
    Workspace: client.RuntimeWorkspace{Root: "/repo"},
}, client.ExecOptions{
    Stdout: os.Stdout,
    Stderr: os.Stderr,
})
if client.IsRuntimeOutcomeUnknown(err) {
    // The process may have run. Reconcile instead of resubmitting blindly.
}
```

A nonzero process status is returned in `ExecResult.ExitCode`; it is not an SDK
error. Start, timeout, confirmed cancellation, and unknown outcome have distinct
typed errors.

## TypeScript SDK

```typescript
const result = await client.exec(
  { argv: ["npm", "test"], workspace: { root: "/repo" } },
  {
    onStdout: chunk => process.stdout.write(chunk),
    onStderr: chunk => process.stderr.write(chunk),
  },
);
```

Use `isRuntimeOutcomeUnknown(error)` for the same fail-closed rule. Both SDKs
also expose capabilities and active-execution cancellation. Successful cancel
means the server confirmed stop and workspace cleanup; an unconfirmed cancel
is outcome-unknown.

## Mount credentials

The server launches a fresh FUSE mount for each execution with
`drive9 mount --no-supervise --no-persist-credentials`. The credential is
available only to the live mount process and is omitted from mount process
state. Runtime gives that process isolated HOME/temp/runtime directories,
removes them after confirmed unmount, and does not pass the credential to
later drain or unmount commands. It uses profile `none`, write-sync durability,
a pre-exec drain, and a post-exec drain so no local-only overlay path can
disappear with the short-lived mount.
