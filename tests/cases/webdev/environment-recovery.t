#!/bin/sh
# Project recovery across three lifecycle legs on a dirty git workspace:
#   1. In-place SIGSTOP/SIGCONT pause & resume of this mount's daemon
#   2. Client restart reusing the local cache (`remount --cache same`)
#   3. Clean-environment replacement (`remount --cache fresh`)
set -eu

require --claim 'git-available' -- command -v git

PAYLOAD_DIR=${PAYLOAD_DIR:-}

mkdir -p proj
if [ -n "$PAYLOAD_DIR" ]; then
  cp -a "$PAYLOAD_DIR/." proj/
  [ -n "$(find proj -type f | head -1)" ] || {
    echo "PAYLOAD-EMPTY: $PAYLOAD_DIR has no files"; exit 1
  }
fi
mkdir -p proj/case-seed/src proj/case-seed/assets
printf 'app committed\n'              > proj/case-seed/src/app.js
printf 'util committed\n'             > proj/case-seed/src/util.js
head -c 65536 /dev/zero | tr '\0' 'H' > proj/case-seed/assets/hero.bin
(
  cd proj
  git init -q
  git config user.email t@t
  git config user.name t
  git add -A
  git commit -qm base
)

printf 'uncommitted new feature\n' > proj/case-seed/src/new-feature.js
printf 'app dirty edit\n'          > proj/case-seed/src/app.js
printf 'doomed legacy\n'           > proj/case-seed/src/legacy.js
mv proj/case-seed/src/util.js proj/case-seed/src/helpers.js
rm proj/case-seed/src/legacy.js
drain --mode require-ok --timeout 120s
sample pre-exit

verify_bare() {
  printf 'app dirty edit\n'          | cmp - proj/case-seed/src/app.js
  printf 'util committed\n'          | cmp - proj/case-seed/src/helpers.js
  printf 'uncommitted new feature\n' | cmp - proj/case-seed/src/new-feature.js
  [ ! -e proj/case-seed/src/legacy.js ] && [ ! -e proj/case-seed/src/util.js ]
  [ -e proj/case-seed/assets/hero.bin ]
}

# Leg 1: pause and resume ONLY the mount daemon serving $MOUNT.
pids=$(pgrep -f "mount.*$MOUNT" || true)
[ -n "$pids" ] || { echo "MOUNT-DAEMON-NOT-FOUND for $MOUNT"; exit 1; }
echo "$pids" | xargs kill -STOP
hold --for 2s
echo "$pids" | xargs kill -CONT
sleep 1
printf 'probe after resume\n' > proj/.resume-probe
cat proj/.resume-probe > /dev/null
rm proj/.resume-probe
verify_bare

# Leg 2: client restart with existing cache.
halt client
remount --cache same
cd "$MOUNT"
verify_bare

# Leg 3: clean-environment replacement with fresh cache.
printf 'written after recovery\n' > proj/case-seed/src/after-recovery.js
drain --mode require-ok --timeout 120s
sample pre-exit-2
halt client
remount --cache fresh
cd "$MOUNT"
sample recovered-fresh

printf 'next round after recovery\n' > proj/case-seed/src/next-round.js
printf 'next round after recovery\n' | cmp - proj/case-seed/src/next-round.js
printf 'app dirty edit\n'            | cmp - proj/case-seed/src/app.js
sample final
drain --mode require-ok --timeout 120s
sample remote-final --as remote

check manifest-equals --this @recovered-fresh --against @pre-exit-2 \
    --ignore 'proj/.git/**,.git/**,.sim/**' --req 'scenario12-check1'
check manifest-absent --this @recovered-fresh \
    --paths 'proj/case-seed/src/legacy.js,proj/case-seed/src/util.js' --req 'scenario12-check2'
check manifest-contains --this @remote-final --against @final \
    --paths 'proj/case-seed/src/after-recovery.js,proj/case-seed/src/next-round.js' --req 'scenario12-check4'
