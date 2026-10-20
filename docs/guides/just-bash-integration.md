# Just Bash with Drive9

`Drive9Fs` adapts the Drive9 TypeScript SDK to Just Bash's `IFileSystem`.
Commands run in Just Bash; file operations under the mount go over HTTP to
Drive9. Writes are persisted before the adapter's promise resolves. Files outside
the mount, such as `/tmp`, can remain in Just Bash's in-memory filesystem.

The implementation is in `clients/drive9-js/src/just-bash.ts`, exported as
`drive9/just-bash`. This entry point is included when building the SDK from this
checkout. Just Bash is an optional peer dependency, so ordinary SDK consumers do
not need the interpreter. The integration is tested with Just Bash 3.6.0 and
requires Node.js >=20.19; the core SDK retains its existing Node.js support.

## Run the example

Start a local Drive9 server or use a provisioned tenant, then:

```bash
cd clients/drive9-js
npm install
export DRIVE9_SERVER=http://127.0.0.1:9009
export DRIVE9_API_KEY='<provisioned tenant token>'
npm run example:just-bash -- --cleanup
```

The example also accepts the SDK's usual `~/.drive9/config` configuration.
It creates a unique `/just-bash-example-<uuid>/` directory, demonstrates
redirects, append, pipes, grep, copy, rename and relative symlinks, and reconnects
with a fresh runtime to check persistence. A scratch file under `/tmp` does not
survive that fresh runtime. Omit `--cleanup` to inspect the persisted files.

Expected output includes:

```text
alpha
beta
gamma
Fresh runtime read: beta
Remote directory removed: /just-bash-example-...
```

## Mount an existing remote directory

```typescript
import { Bash, InMemoryFs, MountableFs } from "just-bash";
import { Client } from "drive9";
import { Drive9Fs } from "drive9/just-bash";

const client = new Client(serverURL, scopedToken);
const remote = new Drive9Fs(client, {
  root: "/agent-work", // existing directory; defaults to tenant root "/"
  readOnly: false,
});
const fs = new MountableFs({ base: new InMemoryFs() });
fs.mount("/workspace", remote);
const bash = new Bash({ fs, cwd: "/workspace" });

try {
  const result = await bash.exec("cat notes.txt | grep TODO");
  if (result.exitCode !== 0) console.error(result.stderr);
  else console.log(result.stdout);
} catch (error) {
  // Just Bash can reject exec(), including on some failed redirects.
  console.error(error);
}
```

Construct the adapter in trusted host code **before** calling `bash.exec()`.
It captures that host's async context for SDK network requests, so HTTP clients
and retry timers work with Just Bash's default runtime guards enabled. The
adapter does not turn off those guards or enable guest `curl`/network access.

Keep the client and its token in host code. Use a server-issued scoped token
with the necessary `read`, `list`, `write` and `delete` operations for the remote
directory. `readOnly: true` rejects every adapter mutation, but does not replace
server authorization. Drive9 restricts `chmod` to owner credentials; permission
changes, including copies that need to preserve a nondefault mode, can fail
with `EACCES` when using a scoped token. Just Bash also invokes `chmod` for
cross-mount copies, so those can require owner credentials even for default modes.

## Behavior and limits

| Capability | Behavior |
|---|---|
| Reads and writes | UTF-8 and binary data; explicit hex/base64/latin1 encodings supported. Writes use SDK upload handling. |
| Append | SDK native append or bounded, revision-checked rewrite; concurrent conflicts are surfaced. |
| Directories | `mkdir -p`, listing, recursive copy/delete; parent and file/directory checks before mutations. |
| Copy | Independent file contents, including recursive copies. The server's aliasing `?copy` API is deliberately not used. New copies retain permission bits when permitted. |
| Rename | Uses the server's rename operation, including directory trees. Replacing an existing directory is rejected. |
| Links | Hardlinks and relative symlinks, including parent-component resolution, dangling links and loop detection. Relative targets cannot escape the configured root. |
| Absolute symlinks | `ENOTSUP` on creation or dereference. Just Bash's mount layer forwards absolute targets unchanged, making mount paths and tenant paths ambiguous. `readlink` can inspect an existing absolute target. |
| Timestamps | `utimes` returns `ENOTSUP`; touching an existing file or preserving explicit timestamps is unsupported. No silent success or content rewrite is used to simulate timestamps. |
| Exclusive private creation | Not implemented because the HTTP API cannot atomically create a file with a requested mode. Use the in-memory `/tmp` for `mktemp`. |
| Errors | `Drive9FsError` exposes a Node-style `code` and message prefix. Missing paths, permission failures, conflicts, and transport failures remain distinct; `exists` and `rm -f` do not hide permission/transport errors. |
| Enumeration | Async `readdir`/`readdirWithFileTypes` support shell traversal and globbing. Synchronous `getAllPaths()` returns `[]`. |

This is a network filesystem adapter, not a transaction or snapshot. A shell
script, recursive copy, or copy followed by `chmod` can partially succeed.
Ordinary writes use the SDK's overwrite behavior; append uses revision checks.
The adapter performs no caching, so fresh reads observe server changes. Reading
and copying a file buffers its content in the host process. Recursive shell
searches can issue many HTTP requests; large search workloads should use
Drive9's server-side search APIs.

The current SDK does not propagate a shell cancellation signal to every HTTP
request. An in-flight mutation may finish after a shell execution is cancelled.
No automatic retry is added for writes or revision conflicts.

## Tests

```bash
cd clients/drive9-js
npm run lint
npm test
npm run build
```

`tests/just-bash.test.ts` exercises the real SDK against HTTP fixtures and runs
actual Just Bash commands over the adapter. Interpreter tests are skipped on
Node 18, while adapter tests still run; the existing SDK CI also runs Node 22.
`npm test` also builds the package and checks both ESM and CommonJS exports
against HTTP errors, including missing paths, permission denials and conflicts.

Live-server tests are included in the TypeScript portion of
`scripts/sdk-integration-tests.sh`. To run just this integration against a
provisioned test tenant:

```bash
DRIVE9_INTEGRATION=1 npx vitest run tests/just-bash.integration.test.ts
```

Set `DRIVE9_SERVER` and `DRIVE9_API_KEY` first. The suite creates unique remote
directories, checks scoped-token behavior, and cleans up its files and grants.
