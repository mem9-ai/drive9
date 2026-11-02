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

check shell --req 'juicefs-rmr-gone'  -- test ! -e rmr-tree
check shell --req 'juicefs-rmr-reuse' -- sh -c 'mkdir rmr-tree && rmdir rmr-tree'

drain --mode require-ok --timeout 120s
check sync-ok --req 'juicefs-rmr-sync'
