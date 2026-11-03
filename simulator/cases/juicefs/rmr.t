#!/bin/sh
# juicefs/rmr: recursive delete of a 20x20 directory tree (420 entries) must
# remove everything and the path must be reusable (blackbox juicefs.rmr port).
set -eu

i=0
while [ $i -lt 20 ]; do
    j=0
    while [ $j -lt 20 ]; do
        mkdir -p "rmr-tree/d$i"
        printf 'entry-%s-%s\n' "$i" "$j" > "rmr-tree/d$i/f$j"
        j=$((j+1))
    done
    i=$((i+1))
done

rm -rf rmr-tree

# removal is asynchronous server-side: the destructive caller must await
# the landed removal before asserting absence or reusing the path
check shell --req 'juicefs-rmr-gone' -- sh -c 'i=0; while [ -e rmr-tree ] && [ $i -lt 200 ]; do i=$((i+1)); sleep 0.1; done; test ! -e rmr-tree'
# path reusability asserted at SERVER truth (kernel entry caches can hold
# the removed name for minutes on CI even though the server is clean) plus
# a fresh-name create/remove cycle on the mount itself
check shell --req 'juicefs-rmr-reuse' -- sh -c '"$D9_CLI" fs stat :/rmr-tree >/dev/null 2>&1 && exit 1; mkdir rmr-reuse && rmdir rmr-reuse'

drain --mode require-ok --timeout 120s
check sync-ok --req 'juicefs-rmr-sync'
