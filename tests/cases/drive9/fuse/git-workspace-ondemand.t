#!/bin/sh
# drive9/fuse/git-workspace-ondemand: Verifies DORMANT -> ARMED on-demand
# git-workspace discovery (`refresh=0` before `--fast`, bounded refresh after `--fast`
# and armed idle, and fresh `--local-root` index discovery).
# Run with `--durability interactive --perf`.
set -eu

export DRIVE9_SERVER="$SERVER"
export DRIVE9_API_KEY="$API_KEY"
export CLI="$CLI"
export GIT_ALLOW_PROTOCOL="${GIT_ALLOW_PROTOCOL:-file:https:http:ssh}"

CASE_DIR="$(cd "$(dirname "$0")" && pwd)"
FIXTURE_ROOT="$RUN_DIR/ondemand-fixture"
FIX_JSON=$(python3 "$CASE_DIR/tools/git_fixture.py" "$FIXTURE_ROOT" --force --tree-files 8)
FIXTURE_URL=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["file_url"])' "$FIX_JSON")
export FIXTURE_URL

# G0: Dormant mount (no git workspaces registered yet) performs ordinary FS ops
# and `.git`-segment lookups without polling `/v1/git-workspaces` (`refresh=0`).
check shell --req 'drive9-git-ondemand-dormant-ops' --claim 'dormant-fs-and-dotgit-activity' -- sh -c '
    mkdir -p "$MOUNT/dormant/plain/.git-like"
    for i in 1 2 3 4 5 6 7 8 9 10; do
        printf "iter-%s\n" "$i" > "$MOUNT/dormant/plain/f-$i.txt"
        cat "$MOUNT/dormant/plain/f-$i.txt" >/dev/null
    done
    ls "$MOUNT/dormant" >/dev/null
'

drain --mode require-ok --timeout 120s
cd "$RUN_DIR"
remount --cache same
cd "$MOUNT"

check shell --req 'drive9-git-ondemand-dormant-zero-refresh' --claim 'dormant-mount-emits-zero-git-workspace-refresh' -- sh -c '
    python3 - "$RUN_DIR/mount-1.log" <<'\''PY'\''
import re, sys
refresh = None
for line in open(sys.argv[1], errors="replace"):
    m = re.search(r"drive9: perf git_workspace refresh=(\d+) forced_refresh=(\d+)", line)
    if m:
        refresh = int(m.group(1))
assert refresh == 0, f"expected dormant refresh=0, got {refresh} in {sys.argv[1]}"
PY
'

# G1 + G3: Two `--fast` clones arm the mount (`forced_refresh >= 2`), and 5s of
# armed non-git activity stays bounded (`refresh <= 16`, catching 1s poll loops).
check shell --req 'drive9-git-ondemand-arm-and-idle' --claim 'fast-clone-arms-without-polling-storm' -- sh -c '
    mkdir -p "$MOUNT/armed"
    "$CLI" git clone --fast "$FIXTURE_URL" "$MOUNT/armed/ws1"
    "$CLI" git clone --fast "$FIXTURE_URL" "$MOUNT/armed/ws2"
    test -z "$(git -C "$MOUNT/armed/ws1" status --porcelain=v1)"
    for i in 1 2 3 4 5; do
        printf "tick-%s\n" "$i" > "$MOUNT/armed/idle-$i.txt"
        sleep 1
    done
'

drain --mode require-ok --timeout 180s
cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

check shell --req 'drive9-git-ondemand-bounded-refresh-and-restore' --claim 'armed-refresh-bounded-and-fresh-local-root-restores' -- sh -c '
    python3 - "$RUN_DIR/mount-2.log" <<'\''PY'\''
import re, sys
refresh = forced = None
for line in open(sys.argv[1], errors="replace"):
    m = re.search(r"drive9: perf git_workspace refresh=(\d+) forced_refresh=(\d+)", line)
    if m:
        refresh, forced = int(m.group(1)), int(m.group(2))
assert refresh is not None and 1 <= refresh <= 16, f"refresh={refresh} out of [1,16]"
assert forced >= 2, f"expected forced_refresh>=2, got {forced}"
PY
    grep -q "Drive9 fixture" "$MOUNT/armed/ws1/README.md"
    test -z "$(git -C "$MOUNT/armed/ws1" status --porcelain=v1)"
'
