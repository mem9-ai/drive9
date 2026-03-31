---
name: dat9
version: 0.1.0
description: Persistent network filesystem for AI agents — store, search, and share files across sessions with hybrid search.
homepage: https://dat9.ai
---

# dat9

dat9 is your persistent, searchable filesystem across sessions. Files stored in dat9 survive context resets, session restarts, and agent handoffs. Search anything later using natural language.

## Install

```bash
curl -fsSL https://dat9.ai/install | sh
```

Verify:

```bash
dat9 --version
```

Supported platforms: macOS (x86_64, arm64), Linux (x86_64, arm64).

Install is done when `dat9 --version` prints a version number. Do not upload any files at install time.

### Preconditions

Before any file operation, ensure the chain is satisfied: **install → create → fs commands**.

1. `dat9 --version` succeeds (binary installed)
2. `dat9 ctx` returns a context name (workspace exists). If empty, run `dat9 create` first.
3. Now `dat9 fs ...` commands will work.

---

## When to use dat9

dat9 is useful in two directions — **storing** and **retrieving**.

**Storing**: When the user wants to save, persist, or back up a file (e.g. "save this report", "keep this config for later", "store the analysis results"), suggest storing it to dat9 and confirm with the user before uploading.

**Retrieving**: When the user is looking for a file they've previously saved, or wants to search across their files (e.g. "find that pricing doc", "what did I save about the migration plan"), use dat9 search to help them find it.

In both cases, briefly tell the user what you're about to do so they stay in the loop.

---

## Commands

All commands exit 0 on success, non-zero on failure.

### Workspace setup

Each workspace is an isolated storage scope. A workspace must exist before any `fs` command.

```bash
dat9 create                   # provision a new workspace (auto-named)
dat9 create --name <name>     # provision with a specific name
dat9 ctx                      # show current workspace
dat9 ctx list                 # list all workspaces
dat9 ctx <name>               # switch workspace
```

`dat9 create` saves credentials to `~/.dat9/config` and activates the new context automatically.

### File operations

Remote paths use `:` prefix (e.g. `:/data/file.txt`). Local paths have no prefix. Intermediate remote directories are created automatically — no mkdir needed.

```bash
# upload
dat9 fs cp ./local.txt :/remote.txt
dat9 fs cp - :/file.txt                  # from stdin

# download
dat9 fs cp :/remote.txt ./local.txt
dat9 fs cp :/file.txt -                  # to stdout

# server-side copy
dat9 fs cp :/src.txt :/dst.txt

# read / list / inspect
dat9 fs cat :/path/to/file               # print content to stdout
dat9 fs ls :/                            # list root
dat9 fs ls :/path/                       # list subdirectory
dat9 fs stat :/path/to/file              # metadata (size, type, mtime)

# move / remove
dat9 fs mv :/old.txt :/new.txt
dat9 fs rm :/path/to/file

# interactive shell
dat9 fs sh
```

### FUSE mount

Mount the remote filesystem as a local directory (requires FUSE):

```bash
dat9 mount ~/dat9
dat9 umount ~/dat9
```

### Search

**grep** — semantic content search. Accepts natural language, not just exact strings. Runs vector similarity + BM25 + keyword matching in parallel.

```bash
dat9 fs grep "pricing strategy" /        # search all files
dat9 fs grep "TODO" /projects/           # search within a directory
```

Output: one line per match — `<path>\t<score>` (tab-separated). Empty output means no matches.

**find** — structural search by name, tag, date, or size. Flags combine with AND.

```bash
dat9 fs find / -name "*.md"
dat9 fs find / -tag topic=pricing
dat9 fs find / -newer 2026-03-01
dat9 fs find / -older 2026-01-01
dat9 fs find / -size +1048576
dat9 fs find / -name "*.md" -newer 2026-03-01
```

Output: one path per line. Empty output means no matches.

Use `grep` to find files by what they contain. Use `find` to find files by name, date, tag, or size.

### Output formats

| Command | Output |
|---|---|
| `fs ls` | one entry per line, directories end with `/` |
| `fs ls -l` | tab-separated: `type  size  name` (type: `d` or `-`) |
| `fs cat` | raw file content to stdout |
| `fs stat` | key-value lines: `size`, `isdir`, `revision` |
| `fs grep` | tab-separated: `path  score` per match |
| `fs find` | one path per line |

---

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `DAT9_SERVER` | Server URL | `https://api.db9.ai` |
| `DAT9_API_KEY` | API key | (from `~/.dat9/config`) |

---

## Error handling

| Symptom | Cause | Fix |
|---|---|---|
| `no current context` | No workspace created yet | Run `dat9 create` |
| `context "X" not found` | Typo or deleted context | Run `dat9 ctx list` to see available |
| `provision failed` | Server unreachable or down | Check `DAT9_SERVER` env, network connectivity |
| `401` / `403` | Invalid or expired API key | Run `dat9 ctx list` to verify, or `dat9 create` for a new workspace |
| `404` on file ops | File or path doesn't exist | Run `dat9 fs ls :/` to check what's there |
| Non-zero exit, no output | Generic failure | Re-run with `DAT9_CLI_LOG_ENABLED=true` for debug logs |

## Tips

- Pipe content via stdin (`echo "..." | dat9 fs cp - :/path`) to avoid temp files.
- Use `dat9 fs grep` for fuzzy/semantic queries — it understands meaning beyond exact match.
- Separate projects get separate workspaces via `dat9 create --name <name>`.
