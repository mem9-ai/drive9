#!/bin/sh
# community/fsstress: xfstests fsstress behavior-level port — random
# metadata+data mix (creat/mkdir/symlink/rename/truncate/unlink/…) over a
# growing tree, upstream default op frequencies, mmap ops off (upstream
# forks children for SIGBUS recovery; the thread model cannot replicate).
# Functional stress: the verdict is "no fatal errors", the envelope keeps
# op counts. Knobs: OPS (per thread), P (threads); -c cleans the tree.
set -eu

OPS=${OPS:-2000}
P=${P:-4}

check fstest --suite fsstress --args "-n $OPS -p $P -c" --req 'community-fsstress'

drain --mode require-ok --timeout 300s
check sync-ok --req 'community-fsstress-sync'
