# FUSE Smoke Test Investigation

## Background

The local FUSE E2E flow documented in `e2e/AGENTS.md` was run with:

```bash
export DRIVE9_BASE=http://127.0.0.1:9009
bash e2e/fuse-smoke-test.sh
```

The local server was healthy and reachable, and the smoke script progressed through
tenant provisioning, activation, CLI build, mount, directory creation, initial file
create, and overwrite steps before failing.

## Initial Script Issue

The first reproduction showed that `e2e/fuse-smoke-test.sh` enabled `set -x` around
the append validation step:

```bash
set -x
printf -- "-append" >> "$RW_TEXT_MOUNT"
remote_append=$(wait_remote_cat_eq "$RW_TEXT_REMOTE" "overwrite-${TS}-append")
check_eq "append visible via remote cat" "$remote_append" "overwrite-${TS}-append"
set +x
```

Because `wait_remote_cat_eq` captures command output with:

```bash
out=$(drive9 fs cat "$path" 2>&1)
```

the shell trace output was mixed into the assertion input. That made the first failure
partly a smoke-script bug, not just a product bug.

After commenting out the `set -x` / `set +x` lines, the script was re-run.

## Current Reproduction

After removing the tracing noise, the script still failed at the append validation step.
The relevant output was:

```text
PASS overwrite visible via remote cat (got=overwrite-1776996224)
e2e/fuse-smoke-test.sh: line 393: warning: command substitution: ignored null byte in input
...
-append
```

This shows the remaining problem is not shell tracing. The output captured from
`drive9 fs cat` appears to contain null bytes, and the visible suffix is only
`-append` instead of the expected full string:

```text
overwrite-1776996224-append
```

## Confirmed Observations

1. `e2e/AGENTS.md` local run instructions are valid for this reproduction.
2. The local server responds correctly for:
   - `POST /v1/provision`
   - `GET /v1/status`
   - pre-mount CLI checks
3. The FUSE mount succeeds.
4. Mounted file create and overwrite checks pass.
5. The append verification fails only when the file is read back remotely through the CLI.
6. Bash warns that the captured output contains null bytes.

## FUSE Mount Log Evidence

The mount log for the second run was:

```text
/home/ubuntu/bench/fuse_test/fuse-mount-live-1776996224.log
```

The log shows the append-side async commit reporting success:

```text
2026/04/24 02:03:48 commit queue: successfully uploaded /fuse-e2e-1776996224/alpha/text.txt (27 bytes)
2026/04/24 02:03:48 commit queue: async commit success for /fuse-e2e-1776996224/alpha/text.txt: base_rev=4 committed_rev=5 kind=1; refreshed FUSE revision state
```

Later, the FUSE trace also reports the file as size `27`:

```text
2026/04/24 02:03:53 fuse trace: fillEntryOut path=/fuse-e2e-1776996224/alpha/text.txt ino=5 size=27 is_dir=false
```

That size matches the expected appended content length, so the mounted side believes the
write and commit succeeded.

## Current Working Theory

There are two separate findings:

1. `e2e/fuse-smoke-test.sh` had a real script bug caused by `set -x` contaminating
   captured command output.
2. After removing that noise, there is still an apparent product issue in the FUSE
   append path or in the remote read path after append.

The remaining bug currently looks more like one of these:

1. FUSE append writes produce corrupted remote content, where the prefix becomes null bytes.
2. The remote read path returns corrupted bytes after append, even though FUSE metadata and
   commit logging report success.

## Related Code Path Notes

`cmd/drive9/cli/cat.go` is a thin passthrough:

```go
rc, err := c.ReadStream(context.Background(), path)
if err != nil {
    return err
}
defer func() { _ = rc.Close() }()
_, err = io.Copy(os.Stdout, rc)
return err
```

So the CLI itself is unlikely to be text-transforming the response. If null bytes are present
in the captured output, they likely originate in the returned stream.

## Secondary Cleanup Symptom

The script also leaves a cleanup warning after failure:

```text
rm: cannot remove '/tmp/drive9-fuse-smoke-1776996224': Device or resource busy
```

This appears to be a post-failure unmount/release timing issue and is not the primary cause
of the append failure.

## Recommended Next Steps

1. Reproduce the same append scenario and fetch the file bytes directly over HTTP to inspect
   the exact returned payload with a hex dump.
2. Inspect the FUSE write/flush/release/async-commit path for append handling, especially
   `O_APPEND`, offsets, and shadow-buffer assembly.
3. Check whether the server-side read path returns the same corrupted bytes seen through
   `drive9 fs cat`.
4. Keep the `set -x` lines disabled or removed from the smoke script so future runs reflect
   the actual product behavior instead of shell trace contamination.
