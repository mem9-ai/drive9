#!/bin/sh
# drive9/git-ops-fast-clone: `drive9 git clone --fast` from a local bare
# fixture into the mount, then edit+commit (ex-e2e git-ops-smoke core; mount
# case — runs on Linux FUSE).
set -eu

export DRIVE9_SERVER="$D9_SERVER"
export DRIVE9_API_KEY="$D9_API_KEY"
export CLI="$D9_CLI"

FIXTURE=$(mktemp -d)
git init -q "$FIXTURE/work"
git -C "$FIXTURE/work" -c user.name=t -c user.email=t@t commit -q --allow-empty -m init
printf 'fixture readme\n' > "$FIXTURE/work/README.md"
git -C "$FIXTURE/work" add README.md
git -C "$FIXTURE/work" -c user.name=t -c user.email=t@t commit -q -m readme
git clone -q --bare "$FIXTURE/work" "$FIXTURE/repo.git"

check shell --req 'drive9-git-fast-clone' -- sh -c '
    "$CLI" git clone --fast "file://'"$FIXTURE"'/repo.git" cloned
    test -f cloned/README.md
    test "$(cat cloned/README.md)" = "fixture readme"
    test -z "$(git -C cloned status --porcelain)"'

printf 'edited on mount\n' >> cloned/README.md
git -C cloned add README.md
git -C cloned -c user.name=t -c user.email=t@t commit -q -m edit

check shell --req 'drive9-git-edit-visible' -- sh -c '
    cat cloned/README.md | grep -q "edited on mount"
    git -C cloned log --oneline | head -2 | grep -q edit'

drain --mode require-ok --timeout 180s
check sync-ok --req 'drive9-git-sync'
