# One-shot Runtime exec

Drive9 Runtime is an optional, non-durable adapter for running one process
against an existing persistent Drive9 workspace. It does not own task state,
retry, replay, history, artifacts, or recovery. Agent frameworks such as Pi
remain authoritative for those decisions.

Eligible `linux-full` providers compose the sandbox's literal Linux `/` from
an immutable provider image/snapshot lower plus a Drive9-backed writable
upper. Provider binaries remain available while writes under paths such as
`/root`, `/home`, `/etc`, and `/workspace` survive a fresh sandbox. `/tmp` and
`/run` are explicitly ephemeral. Runtime reports success only after confirmed
process exit, Drive9 drain, and provider teardown.

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
    Argv:           []string{"go", "test", "./..."},
    ExecutionClass: "linux-full",
    Workspace:      client.RuntimeWorkspace{Root: "/repo"},
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
  {
    argv: ["npm", "test"],
    execution_class: "linux-full",
    workspace: { root: "/repo" },
  },
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

Provider capabilities include each profile's execution class, rootfs identity,
and `production_eligible` / `bounded_selection_eligible` flags. Treat a
candidate with either flag false as preview/manual-only for the corresponding
use.

## Mount credentials

The server launches a fresh extent-profile FUSE mount for each execution with
`drive9 mount --no-supervise --no-persist-credentials`. The credential is
available only to the live mount process, omitted from persistent mount state,
and never passed to the application. Runtime gives the mount process isolated
state directories, performs pre/post-exec drains, and removes the provider
sandbox only after confirmed cleanup. The model process receives only the
request's explicit environment plus a fixed safe `PATH`; it does not inherit
server, provider, image, or mount-process environment variables.
