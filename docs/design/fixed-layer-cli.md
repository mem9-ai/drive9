# Fixed Layer controls

`drive9 fs layer` uses the fixed-view `/v1/views` API. The client and server
must be upgraded together. Commands return JSON; control failures return a
nonzero exit code. A terminal failure receipt is also printed for inspection.

```sh
# Capture the workspace, or derive from the current fixed result of a Layer.
drive9 fs layer create --operation create-attempt-1 attempt-1
drive9 fs layer create --operation create-attempt-2 --parent attempt-1 attempt-2

# One bounded page. Pass next_after to --after for another page.
drive9 fs layer list --limit 100
drive9 fs layer status attempt-1

# Claim one work generation. This command does not execute an agent.
drive9 fs layer work --operation work-attempt-1 --ttl 30m attempt-1

# Use work_id and fence from that claim, not guessed values.
drive9 fs layer renew --work WORK_ID --fence 1 --ttl 30m attempt-1
drive9 fs layer seal --operation seal-attempt-1 --work WORK_ID --fence 1 attempt-1

# After seal, claim a separate read-only validation generation.
drive9 fs layer work --operation validate-attempt-1 --ttl 10m --validation attempt-1

# Resolve a lost response using the original operation identity.
drive9 fs layer operation attempt-1 seal-attempt-1

drive9 fs layer cancel --operation cancel-attempt-1 attempt-1
drive9 fs layer delete --operation delete-attempt-1 attempt-1
```

Mutation commands require an explicit operation ID. Retain it before sending a
request. A timeout, malformed response or 5xx does not prove that the server
rolled back. The error retains the Layer and operation ID; query that operation
instead of issuing a new ID or rerunning the work. The work result's `launch`
permission is returned only by the unique first successful claim. Replays and
operation queries do not authorize a second execution.

Create captures the whole managed workspace. `--parent` derives from the
parent's fixed result. The previous base-root, tag selector, mutable checkpoint,
rollback and per-file commit commands are not sent to legacy Layer endpoints.
Unsupported commands fail locally. Atomic merge and mount selection are wired
separately; this control-command implementation does not claim they are ready.
The remaining legacy `--layer` data-transfer and mount paths must be migrated
before full end-to-end acceptance.
