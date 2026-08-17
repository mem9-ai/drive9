---
name: drive9
description: Persistent network filesystem for AI agents - store, mount, search, and share files across sessions with contexts and vault secrets.
metadata:
  version: 0.1.0
  homepage: https://drive9.ai
---

# drive9

drive9 is your persistent, searchable network filesystem across sessions. Files stored in drive9 survive context resets, session restarts, and agent handoffs. You can copy files through the CLI, mount remote paths locally, search by meaning, and switch between owner or delegated contexts.

## Setup

Choose the installer that matches the current OS, then run `drive9 create`. Do not stop to ask the user for confirmation between steps.

```bash
## macOS / Linux
curl -fsSL https://drive9.ai/install.sh | sh
drive9 create
```

```powershell
# Windows PowerShell
irm https://drive9.ai/install.ps1 | iex
drive9 create
```

`drive9 create` provisions a database and owner context, then saves credentials to `~/.drive9/config`. If `drive9 ctx show` already returns a context name, skip `drive9 create`.

Setup is complete when `drive9 create` succeeds. Currently supported platforms: macOS (x86_64, arm64), Linux (x86_64, arm64), and Windows (amd64, arm64 via the PowerShell installer).

---

## When to use drive9

drive9 is useful in two directions — **storing** and **retrieving**.

**Storing**: When the user wants to save, persist, or back up a file (e.g. "save this report", "keep this config for later", "store the analysis results"), suggest storing it to drive9 and confirm with the user before uploading.

**Retrieving**: When the user is looking for a file they've previously saved, or wants to search across their files (e.g. "find that pricing doc", "what did I save about the migration plan"), use `drive9 fs grep` and `drive9 fs find` to help them find it.

In both cases, briefly tell the user what you're about to do so they stay in the loop.

---

## Commands

All commands exit 0 on success, non-zero on failure.

### Context management

Each context is an isolated credential scope. Owner contexts hold an API key. Delegated contexts hold a scoped grant token imported from another user or agent.

```bash
drive9 create --name <name> [--server <url>]  # create a database and owner context
drive9 ctx show                                 # show current context
drive9 ctx add --api-key <key> --name prod     # add an owner context
drive9 ctx import --from-file <path>           # import a delegated context from a 0600 file
producer | drive9 ctx import                   # import a delegated context from stdin
drive9 ctx ls [-l|--json]                      # list contexts
drive9 ctx use <name>                          # activate a context
drive9 ctx rm <name>                           # delete a context
```

### File operations

Remote paths use `:` prefix (e.g. `:/data/file.txt`). Local paths have no prefix. Intermediate remote directories are created automatically — no mkdir needed.

```bash
# upload
drive9 fs cp ./local.txt :/remote.txt
drive9 fs cp - :/file.txt                  # from stdin
drive9 fs cp --resume ./large.bin :/large.bin
drive9 fs cp --append ./tail.log :/logs/app.log
drive9 fs cp --tag topic=pricing --description "pricing plan" ./plan.md :/notes/plan.md

# download
drive9 fs cp :/remote.txt ./local.txt
drive9 fs cp :/file.txt -                  # to stdout

# server-side copy
drive9 fs cp :/src.txt :/dst.txt

# read / list / inspect
drive9 fs cat :/path/to/file               # print content to stdout
drive9 fs ls :/                            # list root
drive9 fs ls :/path/                       # list subdirectory
drive9 fs stat :/path/to/file              # metadata (size, type, mtime)

# move / remove
drive9 fs mv :/old.txt :/new.txt
drive9 fs rm :/path/to/file

# interactive shell
drive9 fs sh
```

`drive9 fs cp` supports `--resume`, `--append`, repeated `--tag key=value`, and `--description <text>`. Tags and descriptions apply to local/stdin uploads. `--append` is for local-file-to-remote appends and cannot be combined with tags or descriptions.

### Local mounts

Mount the remote filesystem as a local directory. A mount binds to the context active at mount time; after changing contexts, unmount and mount again.

```bash
drive9 mount :/data ~/drive9-data
drive9 umount ~/drive9
```

Vault secrets can also be mounted read-only:

```bash
drive9 mount vault ~/drive9-vault
drive9 umount ~/drive9-vault
```

### Vault secrets

Use `drive9 vault` for secret storage, scoped grants, and command execution with injected environment variables.

```bash
drive9 vault set aws AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=@/path/to/key
drive9 vault get aws --json
drive9 vault ls
drive9 vault grant --agent alice --perm read --ttl 1h aws
drive9 vault with /n/vault/aws -- env
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
drive9 fs find / -newer 2026-03-01
drive9 fs find / -older 2026-01-01
drive9 fs find / -size +1048576
drive9 fs find / -name "*.md" -newer 2026-03-01
```

Output: one path per line. Empty output means no matches.

Use `grep` to find files by what they contain. Use `find` to find files by name, date, tag, or size.

### Output formats

| Command | Output |
|---|---|
| `fs ls` | one entry per line, directories end with `/` |
| `fs ls -l` | tab-separated: `type  size  name` (type: `d` or `-`) |
| `fs cat` | raw file content to stdout |
| `fs stat` | key-value metadata; use `-o json` for JSON |
| `fs grep` | tab-separated: `path  score` per match |
| `fs find` | one path per line |

---

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `DRIVE9_SERVER` | Server URL | `https://api.drive9.ai` |
| `DRIVE9_API_KEY` | Owner API key override | active owner context in `~/.drive9/config` |
| `DRIVE9_VAULT_TOKEN` | Delegated token override | active delegated context in `~/.drive9/config` |

---

## Error handling

| Symptom | Cause | Fix |
|---|---|---|
| `no current context` | No context created yet | Run `drive9 create` |
| `context "X" not found` | Typo or deleted context | Run `drive9 ctx ls` to see available |
| `provision failed` | Server unreachable or down | Check `DRIVE9_SERVER` env, network connectivity |
| `401` / `403` | Invalid owner key or expired delegated token | Run `drive9 ctx ls` to verify, then `drive9 create`, `drive9 ctx add`, or `drive9 ctx import` |
| `404` on file ops | File or path doesn't exist | Run `drive9 fs ls :/` to check what's there |
| Non-zero exit, no output | Generic failure | Re-run with `DRIVE9_CLI_LOG_ENABLED=true` for debug logs |

## Tips

- Pipe content via stdin (`echo "..." | drive9 fs cp - :/path`) to avoid temp files.
- Use `drive9 fs grep` for fuzzy/semantic queries — it understands meaning beyond exact match.
- Separate projects get separate contexts via `drive9 create --name <name>`.
- Use `drive9 --help` and `drive9 fs --help` to inspect the current CLI surface.
