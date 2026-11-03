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
check shell --req 'juicefs-rmr-reuse' -- sh -c 'mkdir rmr-tree && rmdir rmr-tree'

drain --mode require-ok --timeout 120s
check sync-ok --req 'juicefs-rmr-sync'
