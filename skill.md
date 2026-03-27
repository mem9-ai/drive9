---
name: dat9
version: 0.1.0
description: Network drive for AI agents — store files, search by content or metadata, with built-in vector and full-text search.
homepage: https://github.com/mem9-ai/dat9
---

# dat9

Network drive for AI agents. Store files, tag them, search by content or metadata. Built-in hybrid search: vector similarity + full-text + keyword, transparent to the caller.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/qiffang/dat9/main/install.sh | sh
```

Supports: macOS (x86_64, arm64), Linux (x86_64, arm64).

```bash
dat9 --version
```

---

## Getting Started

```bash
# Provision a database
dat9 create --name myapp

# Upload a file
dat9 fs cp ./notes.md :/notes.md

# Read it back
dat9 fs cat :/notes.md

# Search file contents (hybrid: vector + FTS + keyword)
dat9 fs grep "pricing strategy" /

# Find files by attributes
dat9 fs find / -name "*.md"
```

---

## Context Management

dat9 stores credentials in `~/.dat9/config`. Each context maps a name to an API key.

```bash
# Create a database (auto-generates name if omitted)
dat9 create
dat9 create --name staging

# Switch context
dat9 ctx staging

# Show current context
dat9 ctx

# List all contexts
dat9 ctx list
```

All `fs` commands use the current context. Switch with `dat9 ctx <name>`.

---

## Filesystem Operations

Remote paths use `:` prefix. Local paths have no prefix.

### Copy files

```bash
dat9 fs cp ./local.txt :/remote.txt      # upload
dat9 fs cp :/remote.txt ./local.txt      # download
dat9 fs cp :/src.txt :/dst.txt           # server-side copy
dat9 fs cp - :/file.txt                  # upload from stdin
dat9 fs cp :/file.txt -                  # download to stdout
```

### Read, list, inspect

```bash
dat9 fs cat :/path/to/file               # print to stdout
dat9 fs ls :/path/                       # list directory
dat9 fs stat :/path/to/file              # file metadata
```

### Move, remove

```bash
dat9 fs mv :/old.txt :/new.txt           # rename/move
dat9 fs rm :/path/to/file                # delete
```

### Interactive shell

```bash
dat9 fs sh
```

---

## Search

### grep — search file contents

Hybrid search: runs vector similarity (auto-embedding), full-text (BM25), and keyword (LIKE) in parallel. Results merged via RRF ranking. You write `grep`, the server picks the best strategy.

```bash
dat9 fs grep "pricing strategy" /
dat9 fs grep "TODO" /projects/
dat9 fs grep "机器学习" /research/
```

Output: matching file paths with relevance scores.

### find — search by file attributes

Standard Unix `find` flags.

```bash
dat9 fs find / -name "*.md"
dat9 fs find / -tag topic=pricing
dat9 fs find / -newer 2026-03-01
dat9 fs find / -older 2026-01-01
dat9 fs find / -size +1048576
```

Output: matching file paths.

---

## Complete CLI Reference

```
dat9
├── create [--name <name>] [--server <url>]    provision a new database
├── ctx [<name>]                               switch or show current context
├── ctx list                                   list all contexts
├── fs
│   ├── cp <src> <dst>                         copy files (local↔remote)
│   ├── cat <path>                             read file to stdout
│   ├── ls [path]                              list directory
│   ├── stat <path>                            file metadata (size, type, time)
│   ├── mv <old> <new>                         rename/move
│   ├── rm <path>                              delete
│   ├── sh                                     interactive shell
│   ├── grep <pattern> [dir]                   search file contents (hybrid)
│   └── find [dir] [-name] [-tag] [-newer] [-older] [-size]
│                                              find files by attributes
└── --version                                  show version
```

---

## How Search Works

`dat9 fs grep` is not a simple string match. The server runs three search strategies in parallel and merges the results:

| Strategy | Engine | When Used |
|----------|--------|-----------|
| Vector similarity | `VEC_EMBED_COSINE_DISTANCE` (TiDB auto-embedding) | Files have embeddings |
| Full-text search | `fts_match_word` (TiDB BM25) | Files have FTS index |
| Keyword fallback | `LIKE '%query%'` | Neither available |

Results are merged using Reciprocal Rank Fusion (RRF, k=60). You don't choose — the server uses everything available.

---

## Config File

`~/.dat9/config` (JSON, chmod 600):

```json
{
  "server": "http://localhost:9009",
  "current_context": "myapp",
  "contexts": {
    "myapp": { "api_key": "dat9_..." },
    "staging": { "api_key": "dat9_..." }
  }
}
```
