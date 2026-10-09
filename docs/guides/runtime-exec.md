# One-shot Runtime exec

Drive9 Runtime is an optional, non-durable adapter for running one process
against an existing persistent Drive9 workspace. It does not own task state,
retry, replay, history, artifacts, or recovery. Agent frameworks such as Pi
remain authoritative for those decisions.

Runtime has two explicit persistence scopes. `full_root` providers compose the
sandbox's literal Linux `/` from an immutable provider image lower plus a
Drive9-backed writable upper; writes under `/root`, `/home`, `/etc`, and
`/workspace` survive a fresh sandbox while `/tmp` and `/run` remain ephemeral.
`workspace` providers persist only the provider's official coding workspace.
Their root filesystem, `/tmp`, `/run`, `/etc`, other home content, and system
package installs are disposable. Runtime never silently downgrades a
`full_root` request to `workspace`.

Docker supplies `full_root`. Daytona profiles use the workspace path resolved
by Daytona's API and supply `workspace` only. Runtime reports success
only after confirmed process exit, Drive9 drain, and provider teardown.

## CLI

```bash
drive9 exec \
  --workspace :/repo \
  --layer pi-fork-42 \
  --persistence workspace \
  --cwd . \
  --timeout 10m \
  --env NODE_ENV=test \
  -- npm test
```

The command writes remote stdout and stderr to the matching local streams and
returns the process exit status. `--workspace` defaults to `:/`.
`--layer` selects one exact writable LayerFS view, such as a Pi conversation
fork. Omitting it mounts the base workspace view. Base and layer views use
independent execution mounts and may run concurrently. A layer view requires
`--persistence workspace`; the server rejects `full_root + layer_id`, including
the default omitted persistence value, because full-root compatibility is not
inherited across LayerFS forks.
`--persistence` defaults to `full_root` for compatibility and safety; pass
`workspace` only when persistence outside the coding workspace is unnecessary.
CPU, memory, PID, disk, network, and execution class are backend-owned provider
profile settings and are not client flags. `--timeout` is a caller cancellation
deadline; the selected backend profile may enforce a shorter maximum.

Interrupting the client requests remote cancellation. The CLI reports canceled
only after the server confirms that the process stopped and bounded workspace
cleanup completed; otherwise it reports that the outcome is unknown. It never
retries.

## Go SDK

```go
result, err := c.Exec(ctx, client.ExecRequest{
    Argv: []string{"go", "test", "./..."},
    Workspace: client.RuntimeWorkspace{
        Root:        "/repo",
        LayerID:     "pi-fork-42",
        Persistence: client.RuntimePersistenceWorkspace,
    },
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
    workspace: { root: "/repo", layer_id: "pi-fork-42", persistence: "workspace" },
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

Provider capabilities include each profile's execution class, machine-readable
`persistence`, rootfs/workspace identity, `workspace_persistence` and
`full_root_persistence` flags, plus `production_eligible` /
`bounded_selection_eligible`. Treat a candidate with either eligibility flag
false as preview/manual-only for the corresponding use.

## Mount credentials

The server launches a fresh extent-profile FUSE mount for each execution with
`drive9 mount --no-supervise --no-persist-credentials`. The credential is
available only to the live mount process, omitted from persistent mount state,
and never passed to the application. Runtime gives the mount process isolated
state directories, performs pre/post-exec drains, and removes the provider
sandbox only after confirmed cleanup. The model process receives only the
request's explicit environment plus a fixed safe `PATH`; it does not inherit
server, provider, image, or mount-process environment variables.
