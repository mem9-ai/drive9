#!/bin/sh
# Per-attribute persistence across same-cache and fresh-cache remounts:
# executable mode (0755), pinned mtime, symlink target, and user.* xattr.
# Content sync completes before attributes are mutated so metadata persistence
# is isolated from content upload.
set -eu

mkdir -p attrs
printf '#!/bin/sh\necho attr-exec\n' > attrs/exec.sh
printf 'mtime pinned\n'             > attrs/timed.txt
drain --mode require-ok --timeout 120s

chmod 755 attrs/exec.sh
[ -x attrs/exec.sh ]
d9-stat set-mtime attrs/timed.txt 1577836800
ln -sfn ../attrs/exec.sh attrs/exec-link
[ "$(readlink attrs/exec-link)" = "../attrs/exec.sh" ]

if d9-stat xattr-set attrs/timed.txt user.drive9sim.test v1 2>"$RUN_DIR/xattr-err"; then
  [ "$(d9-stat xattr-get attrs/timed.txt user.drive9sim.test)" = "v1" ]
  echo "xattr: set-ok readback-ok"
else
  echo "xattr: UNSUPPORTED $(cat "$RUN_DIR/xattr-err")"
fi

m=$(d9-stat mtime attrs/timed.txt)
if [ "$m" -ge 1577836800 ] && [ "$m" -le 1577836860 ]; then
  check shell --accept pass,unconfirmed --req 'E-check1' -- test "$m" -le 1577836860
else
  echo "MTIME-SET-NOT-EFFECTIVE set=1577836800..1577836860 got=$m"
  check shell --accept pass,unconfirmed --req 'E-check1' -- sh -c 'exit 3'
fi
sample attrs-set
drain --mode require-ok --timeout 120s

# Leg 1: client restart with existing cache.
halt client
remount --cache same
cd "$MOUNT"

rc=0
[ -x attrs/exec.sh ] || { echo "MODE-LOST"; rc=1; }
[ "$(readlink attrs/exec-link)" = "../attrs/exec.sh" ] || { echo "LINK-LOST"; rc=1; }
m=$(d9-stat mtime attrs/timed.txt)
if [ "$m" -ge 1577836800 ] && [ "$m" -le 1577836860 ]; then
  check shell --accept pass,unconfirmed --req 'E-check2' -- test "$m" -le 1577836860
else
  echo "MTIME-NOT-PRESERVED-SAME set=1577836800..1577836860 got=$m"
  check shell --accept pass,unconfirmed --req 'E-check2' -- sh -c 'exit 3'
fi
if got_x=$(d9-stat xattr-get attrs/timed.txt user.drive9sim.test 2>/dev/null); then
  [ "$got_x" = "v1" ] || { echo "XATTR-VALUE-DIFF"; rc=1; }
else
  echo "xattr: UNSUPPORTED after same-cache remount"
fi
[ "$rc" -eq 0 ]

# Leg 2: remount with fresh cache (remote truth only).
halt client
remount --cache fresh
cd "$MOUNT"
sample fresh

rc=0
[ -x attrs/exec.sh ] || { echo "MODE-LOST"; rc=1; }
[ "$(readlink attrs/exec-link)" = "../attrs/exec.sh" ] || { echo "LINK-LOST"; rc=1; }
m=$(d9-stat mtime attrs/timed.txt)
if [ "$m" -ge 1577836800 ] && [ "$m" -le 1577836860 ]; then
  check shell --accept pass,unconfirmed --req 'E-check2' -- test "$m" -le 1577836860
else
  echo "MTIME-NOT-PRESERVED-FRESH set=1577836800..1577836860 got=$m"
  check shell --accept pass,unconfirmed --req 'E-check2' -- sh -c 'exit 3'
fi
if got_x=$(d9-stat xattr-get attrs/timed.txt user.drive9sim.test 2>/dev/null); then
  [ "$got_x" = "v1" ] || { echo "XATTR-VALUE-DIFF"; rc=1; }
else
  echo "xattr: UNSUPPORTED after fresh remount"
fi
[ "$rc" -eq 0 ]

printf 'mtime pinned\n'              | cmp - attrs/timed.txt
printf '#!/bin/sh\necho attr-exec\n' | cmp - attrs/exec.sh

check manifest-equals --this @fresh --against @attrs-set \
    --ignore '.sim/**' --req 'E-check2'
check artifact-contains --artifact script.out \
    --pattern 'xattr:' --req 'E-evidence'
