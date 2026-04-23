---
name: drive9
version: 0.1.1
description: Persistent network filesystem for AI agents — store, search, and share files across sessions with hybrid search.
homepage: https://drive9.ai
---

# drive9

drive9 is your persistent, searchable filesystem across sessions. Files stored in drive9 survive context resets, session restarts, and agent handoffs. Search anything later using natural language.

## Setup

Run these two commands in order. Do not stop to ask the user for confirmation between steps.

```bash
curl -fsSL https://drive9.ai/install.sh | sh
drive9 create
```

`drive9 create` provisions a workspace and saves credentials to `~/.drive9/config`. If `drive9 ctx` already returns a context name, skip `drive9 create`.

Setup is complete when `drive9 create` succeeds. Supported platforms: macOS (x86_64, arm64), Linux (x86_64, arm64).

---

## When to use drive9

drive9 is useful in two directions — **storing** and **retrieving**.

**Storing**: When the user wants to save, persist, or back up a file (e.g. "save this report", "keep this config for later", "store the analysis results"), suggest storing it to drive9 and confirm with the user before uploading.

**Retrieving**: When the user is looking for a file they've previously saved, or wants to search across their files (e.g. "find that pricing doc", "what did I save about the migration plan"), use drive9 search to help them find it.

In both cases, briefly tell the user what you're about to do so they stay in the loop.

---

## Commands

All commands exit 0 on success, non-zero on failure.

### Workspace management

Each workspace is an isolated storage scope. The initial workspace is created during setup (see above).

```bash
drive9 create --name <name>     # create an additional workspace
drive9 ctx                      # show current workspace
drive9 ctx list                 # list all workspaces
drive9 ctx <name>               # switch workspace
```

### File operations

Remote paths use `:` prefix (e.g. `:/data/file.txt`). Local paths have no prefix. Intermediate remote directories are created automatically — no mkdir needed.

```bash
# upload
drive9 fs cp ./local.txt :/remote.txt
drive9 fs cp - :/file.txt                  # from stdin
drive9 fs cp --tag topic=pricing --tag owner=agent ./plan.md :/notes/plan.md

# download
drive9 fs cp :/remote.txt ./local.txt
drive9 fs cp :/file.txt -                  # to stdout

# server-side copy
drive9 fs cp :/src.txt :/dst.txt

# read / list / inspect
drive9 fs cat :/path/to/file               # print content to stdout
drive9 fs ls :/                            # list root
drive9 fs ls :/path/                       # list subdirectory
drive9 fs stat :/path/to/file              # text metadata (size, type, mtime)
drive9 fs stat -o json :/path/to/file      # JSON metadata

# inspect semantic metadata and tags
drive9 fs stat :/notes/plan.md             # includes semantic_text and tags
drive9 fs stat -o json :/notes/plan.md     # JSON includes semantic_text and tags object

# move / remove
drive9 fs mv :/old.txt :/new.txt
drive9 fs rm :/path/to/file
drive9 fs rm -r :/path/to/dir/

# interactive shell
drive9 fs sh
```

Use `drive9 fs cp --tag` only for uploads from local->remote or stdin->remote. Any provided `--tag` flags replace the file's existing tag set on re-upload; omit `--tag` to preserve existing tags. Do not use `drive9 fs cp --tag` for remote->local or remote->remote transfers, and do not combine it with `--append`.

### FUSE mount

Mount the remote filesystem as a local directory (requires FUSE):

```bash
drive9 mount ~/drive9
drive9 umount ~/drive9
```

### Search

**grep** — semantic content search. Accepts natural language, not just exact strings. Runs vector similarity + BM25 + keyword matching in parallel.

```bash
drive9 fs grep "pricing strategy" /        # search all files
drive9 fs grep "TODO" /projects/           # search within a directory
```

Output: one line per match — `<path>\t<score>` (tab-separated). Empty output means no matches.

**find** — structural search by name, tag, date, or size. Flags combine with AND.

```bash
drive9 fs find / -name "*.md"
drive9 fs find / -tag topic=pricing
drive9 fs find /notes/ -tag owner=agent
drive9 fs find /notes/ -tag owner
drive9 fs find / -newer 2026-03-01
drive9 fs find / -older 2026-01-01
drive9 fs find / -size +1048576
drive9 fs find / -name "*.md" -newer 2026-03-01
```

Tag filter semantics (`-tag`) are exact, not fuzzy:

- `-tag key=value` matches files where both `tag_key` and `tag_value` are equal.
- `-tag key` matches files that contain that tag key (value ignored).
- Prefix / contains / regex matching is not supported for `-tag`.

Output: one path per line. Empty output means no matches.

Use `grep` to find files by what they contain. Use `find` to find files by name, date, tag, or size.

### Output formats

| Command | Output |
|---|---|
| `fs ls` | one entry per line, directories end with `/` |
| `fs ls -l` | tab-separated: `type  size  name` (type: `d` or `-`) |
| `fs cat` | raw file content to stdout |
| `fs stat` | key-value text lines by default; use `-o json` / `--output json` for JSON |
| `fs grep` | tab-separated: `path  score` per match |
| `fs find` | one path per line |

---

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `DRIVE9_SERVER` | Server URL | `https://api.drive9.ai` |
| `DRIVE9_API_KEY` | API key | (from `~/.drive9/config`) |

---

## Error handling

| Symptom | Cause | Fix |
|---|---|---|
| `no current context` | No workspace created yet | Run `drive9 create` |
| `context "X" not found` | Typo or deleted context | Run `drive9 ctx list` to see available |
| `provision failed` | Server unreachable or down | Check `DRIVE9_SERVER` env, network connectivity |
| `401` / `403` | Invalid or expired API key | Run `drive9 ctx list` to verify, or `drive9 create` for a new workspace |
| `404` on file ops | File or path doesn't exist | Run `drive9 fs ls :/` to check what's there |
| Non-zero exit, no output | Generic failure | Re-run with `DRIVE9_CLI_LOG_ENABLED=true` for debug logs |

## Tips

- Pipe content via stdin (`echo "..." | drive9 fs cp - :/path`) to avoid temp files.
- Use `drive9 fs grep` for fuzzy/semantic queries — it understands meaning beyond exact match.
- Separate projects get separate workspaces via `drive9 create --name <name>`.
