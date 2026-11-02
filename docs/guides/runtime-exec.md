# Remote exec

`drive9 exec` runs a command in a sandbox with your Drive9 drive mounted at
`/workspace`. You do not create, mount, or clean up a sandbox yourself.

```bash
cd ~/drive9/repo/web && drive9 exec npm test   # inside a local drive9 mount
drive9 exec -w repo 'npm ci && npm test'         # anywhere
drive9 exec -e CI=1 -e GITHUB_TOKEN -t 20m -- make -j8 test
```

The server must have remote exec enabled. Otherwise the command reports
`remote exec is not enabled on this server`.

## What to expect

- **Where it runs.** Your whole drive is at `/workspace`. Inside a local
  drive9 mount, the command runs in the matching directory, after your pending
  local changes are uploaded so it sees exactly your files. Elsewhere, `-w`
  picks the directory (`repo`, `/repo`, and `:/repo` all work, giving
  `/workspace/repo`; default: the drive root). `-C` picks a subdirectory.
  Only FUSE mounts are followed: inside a WebDAV mount (the default on macOS
  and Windows), pass `-w`. `-w` does not upload local changes first, so the
  command sees what the server already has.
- **Who it runs as.** A normal user, `agent`, with `HOME=/home/agent`, so git,
  pip, npm, and go work as usual. It can edit any workspace file but cannot
  become root.
- **What persists.** Files under `/workspace` are in Drive9 when the command
  exits. Everything else in the sandbox is disposable.
- **Warm sandboxes.** Your consecutive commands (same API key) reuse one
  sandbox for a while, whichever directory they run in (10 minutes idle by
  default), so
  packages you installed and files in `/tmp` are still there for the next
  command. Processes are not: each command starts with nothing left running
  from the previous one. `--fresh` starts over in a new sandbox; `-v` shows
  whether a command got a `new` or `warm` one.
- **Feedback.** While a sandbox is starting, a terminal shows
  `starting sandbox…`; the line disappears as soon as the command runs.
  Colors are kept when your output is a terminal (`NO_COLOR` is respected).
- **Output.** stdout and stderr stream live to your terminal, separately.
- **Exit status.** `drive9 exec` exits with the command's status and adds no
  message of its own. Other outcomes use shell conventions:

  | Status | Meaning |
  | --- | --- |
  | 124 | `-t` timeout reached; the command was stopped |
  | 126 / 127 | command not executable / not found |
  | 130 | canceled with Ctrl-C |
  | 255 | the connection was lost and the outcome is unknown |

- **Ctrl-C.** The first Ctrl-C stops the remote command and waits for the
  workspace to drain. A second Ctrl-C detaches immediately; the command may keep
  running.
- **Never retried.** If the outcome is unknown, the command may already have
  run. Check the workspace before running it again.

## Flags

| Flag | Meaning |
| --- | --- |
| `-w, --workspace PATH` | Drive9 directory to run in (default: where you are in a local mount, else the drive root) |
| `-C, --cwd DIR` | subdirectory of the workspace to run in |
| `-e, --env KEY[=VALUE]` | set a variable; `KEY` alone copies it from your environment; repeatable |
| `-t, --timeout DURATION` | stop the command after this long (the server may cap it) |
| `--fresh` | start in a new sandbox instead of reusing the warm one |
| `--layer REF` | use a LayerFS layer view (layer ID, name, or `tag:key=value`), for example one agent fork |
| `--read-only` | mount the workspace read-only |
| `-v, --verbose` | show the sandbox, working directory, and duration |

A single argument that contains spaces or shell syntax runs through
`/bin/sh -c`: `drive9 exec 'cd web && npm test'`. Anything else runs as-is,
with or without `--`.

## Go SDK

```go
result, err := c.Exec(ctx, client.ExecRequest{
    Command:   "go test ./...",           // or Argv: []string{"go", "test", "./..."}
    Workspace: client.RuntimeWorkspace{Root: "/repo"},
}, client.ExecOptions{Stdout: os.Stdout, Stderr: os.Stderr})
switch {
case client.IsRuntimeBusy(err):
    // Too many commands are running; nothing started. Retry later.
case client.IsRuntimeOutcomeUnknown(err):
    // The command may have run. Reconcile instead of resubmitting.
case err == nil:
    fmt.Println("exit", result.ExitCode, "sandbox", result.Sandbox)
}
```

## TypeScript SDK

```typescript
const result = await client.exec(
  { command: "npm test", workspace: { root: "/repo" } },
  {
    onStdout: chunk => process.stdout.write(chunk),
    onStderr: chunk => process.stderr.write(chunk),
  },
);
```

Both SDKs expose `runtimeCapabilities` / `GetRuntimeCapabilities` (limits such
as the maximum timeout and concurrent commands) and cancellation of a running
command.

## Credentials

The server mounts the workspace inside the sandbox with your API key via
`drive9 mount --no-persist-credentials`. The key lives only in the mount
process's memory. It is not written to disk and is never visible to your
command or its environment.
