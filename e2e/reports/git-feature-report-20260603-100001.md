# Drive9 Git Feature Matrix Report

**Date:** 2026-06-03T10:00:52Z
**Suite:** `git`
**Base:** `http://k8s-dat9-dat9serv-d5e02e7d07-1645488597.ap-southeast-1.elb.amazonaws.com`
**CLI source:** `build`
**Host:** `Linux-6.17.0-1017-aws-x86_64-with-glibc2.39`
**Strict unchecked mode:** `0`

## Summary

| Status | Count |
|---|---:|
| PASS | 99 |
| FAIL | 11 |
| UNSUPPORTED | 0 |
| SKIP | 0 |
| TOTAL | 110 |

## Matrix

### Drive9 Git Workspace Behavior

- [x] coding-agent mount starts - PASS: mounted
- [x] tree manifest registered - PASS: workspace_id=52c82bb5-2544-4d33-a9e2-675fbc671e72
- [ ] prepare overlay whiteout - FAIL: rc=1 error: the following file has local modifications: docs/guide.md (use --cached to keep the file, or -f to force removal)
- [x] prepare overlay chmod - PASS: script.sh mode set to 644
- [x] prepare overlay symlink - PASS: restore-link -> README.md
- [x] stage oversized object before remount - PASS: ok
- [x] unmount drains git workspace state - PASS: rw coding-agent mount unmounted
- [x] fresh local-root remount starts - PASS: mounted
- [x] .git checkpoint restored - PASS: git status works
- [ ] overlay upsert/dir survives remount - FAIL: missing edited README or restore-dir
- [ ] overlay whiteout survives remount - FAIL: docs/guide.md still exists
- [x] overlay chmod survives remount - PASS: script.sh mode 644
- [x] overlay symlink survives remount - PASS: restore-link -> README.md
- [ ] oversized staged object downgrade - FAIL: status= M .gitignore M README.md M binary.bin D committed-local.txt M docs/guide.md D link-to-readme M script.sh A small-staged.txt M src/app.py ?? restore-dir/ ?? restore-link

### Git Clean Repo Readiness

- [x] .git directory is usable - PASS: ok
- [x] git log reads latest commit - PASS: ok
- [x] git show reads HEAD - PASS: ok
- [x] git ls-files lists manifest - PASS: ok
- [x] git cat-file reads clean blob - PASS: ok
- [ ] git status clean after fast clone - FAIL: status= M .gitignore M README.md M binary.bin M docs/guide.md M link-to-readme M script.sh M src/app.py
- [x] executable bit visible - PASS: script.sh executable
- [x] symlink visible - PASS: link-to-readme -> README.md
- [ ] binary file visible - FAIL: binary bytes mismatch
- [x] tag visibility - PASS: v0.1.0
- [x] clean status after commit - PASS: status clean
- [x] clean status after amend - PASS: status clean
- [x] clean status after stash push - PASS: status clean

### Git Clone Modes

- [x] drive9 git clone --fast - PASS: ok
- [x] drive9 git clone --fast --blobless --hydrate=off - PASS: ok
- [x] drive9 git clone --fast --blobless --hydrate=sync - PASS: ok
- [x] drive9 git clone --fast --blobless then explicit hydrate - PASS: ok
- [x] drive9 git hydrate explicit - PASS: ok
- [x] ops clone for full Git operation suite - PASS: ok
- [x] merge-flow clone for flow - PASS: ok
- [x] conflict-flow clone for flow - PASS: ok
- [x] rebase-flow clone for flow - PASS: ok
- [x] stash-flow clone for flow - PASS: ok
- [x] restore workspace clone - PASS: ok

### Git Commit History

- [x] git commit - PASS: ok
- [x] git commit --amend - PASS: ok
- [x] branch create/switch - PASS: ok
- [x] branch commit - PASS: ok

### Git Diff And Patch

- [x] git diff --cached - PASS: ok
- [x] generate text patch - PASS: ok
- [x] restore before text patch apply - PASS: ok
- [x] git apply text patch - PASS: ok
- [x] git diff nonempty - PASS: patch text visible
- [x] prepare binary patch edit - PASS: ok
- [x] generate binary patch - PASS: ok
- [x] restore before binary patch apply - PASS: ok
- [x] git apply binary patch - PASS: ok

### Git Fixture

- [x] local bare fixture repo generated - PASS: /tmp/drive9-feature-matrix.wQSJe6/git-fixture/remote.git

### Git Index Operations

- [x] git add individual path - PASS: ok
- [x] staged vs unstaged status accuracy - PASS: matched ^MM README\.md$
- [x] git restore --staged - PASS: ok
- [x] unstaged status after restore --staged - PASS: matched ^ M README\.md$
- [x] git add -A - PASS: ok
- [x] git reset path - PASS: ok
- [x] git reset leaves path unstaged - PASS: matched ^\?\? generated/reset\.txt$
- [x] git add -A after reset - PASS: ok
- [x] stage text patch result - PASS: ok
- [x] stage binary patch result - PASS: ok
- [x] stage branch work - PASS: ok

### Git Merge/Rebase/Stash

- [x] clean merge - PASS: ok
- [x] conflict fixture stage local edit - PASS: ok
- [x] conflict fixture local commit - PASS: ok
- [ ] conflict detection - FAIL: status= D .gitignore D binary.bin D docs/guide.md D link-to-readme D script.sh D src/app.py pattern=^UU README\.md$
- [x] rebase fixture branch create - PASS: ok
- [x] rebase fixture stage local file - PASS: ok
- [x] rebase fixture local commit - PASS: ok
- [ ] simple rebase - FAIL: rc=1 error: cannot rebase: You have unstaged changes. error: Please commit or stash them.
- [x] stash push -u - PASS: ok
- [x] stash apply - PASS: ok
- [x] dirty status after stash apply - PASS: matched README\.md
- [x] stash drop - PASS: ok

### Git Prerequisites

- [x] configure git identity for ops repo - PASS: ok
- [x] configure git identity for restore repo - PASS: ok

### Git Remote Operations

- [x] push branch to local bare remote - PASS: ok
- [x] fetch from local bare remote - PASS: ok
- [x] create local tag - PASS: ok
- [x] push tag to local bare remote - PASS: ok
- [x] remote ahead fixture commit - PASS: ok
- [x] pull from local bare remote - PASS: ok

### Git Working Tree Operations

- [x] modify tracked file - PASS: matched ^ M README\.md$
- [x] create files/directories - PASS: matched ^\?\? generated/
- [x] git mv tracked file - PASS: ok
- [ ] git rm tracked file - FAIL: rc=1 error: the following file has local modifications: docs/guide.md (use --cached to keep the file, or -f to force removal)
- [x] chmod executable bit change - PASS: matched ^ M script\.sh$
- [x] symlink changes - PASS: matched ^( D link-to-readme|\?\? link-to-app)
- [x] binary file modification - PASS: matched ^ M binary\.bin$
- [ ] ignored local-only generated files - FAIL: ignored-build/cache.tmp was not ignored

### Prerequisites

- [x] feature matrix suite selected - PASS: git
- [x] python3 available - PASS: ok
- [x] curl available - PASS: ok
- [x] jq available - PASS: ok
- [x] git available - PASS: ok
- [x] go available for CLI build - PASS: ok
- [x] drive9 CLI ready - PASS: /tmp/tmp.WlPjKB2aIr
- [x] FUSE host prerequisites - PASS: ok

### Provisioning

- [x] POST /v1/provision returns 202 - PASS: ok
- [x] provision returns api_key - PASS: ok
- [x] tenant becomes active - PASS: active

### Sandbox Restore

- [x] stage committed local state before remount - PASS: ok
- [x] commit local state before remount - PASS: ok
- [x] stage small local object before remount - PASS: ok
- [x] dirty status before remount - PASS: matched README\.md
- [ ] committed local state survives fresh local-root remount - FAIL: HEAD=74125ae3878aa838a16e27586e4f9edc82b2149f
- [x] unstaged edits survive fresh local-root remount - PASS: matched README\.md
- [x] small staged object preserved - PASS: matched ^A small-staged\.txt$
- [x] ignored generated files are non-durable by design - PASS: ignored-build/cache.tmp absent after fresh local root
