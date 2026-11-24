#!/bin/sh
# community/fio-read-cache: large-file read throughput across cache states
# for 1m and 4m block sizes. Seeds and drains real data, remounts `--cache fresh`
# so client write caches start cold, then probes:
#   1. cold-client (requires `DROP_CACHES_CMD`, otherwise recorded unsupported)
#   2. kernel-warm (immediate repeat)
#   3. client-warm-kernel-dropped (when `DROP_CACHES_CMD` is supplied)
set -eu

DATA_MIB=${DATA_MIB:-256}
RUNTIME=${RUNTIME:-10}
DIRECT=${DIRECT:-0}

dd if=/dev/urandom of=data.1m.bin bs=1048576 count="$DATA_MIB"
dd if=/dev/urandom of=data.4m.bin bs=1048576 count="$DATA_MIB"
drain --mode require-ok --timeout 300s

# Remount with a fresh client cache so the initial read is genuinely cold on the client.
cd "$RUN_DIR"
remount --cache fresh
cd "$MOUNT"

# ---- BS 1m ----
if [ -n "${DROP_CACHES_CMD:-}" ]; then
    sh -c "$DROP_CACHES_CMD"
    check fstest --suite fio --args "--rw=read --bs=1m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.1m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-1m-cold'
    check fstest --suite fio --args "--rw=read --bs=1m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.1m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-1m-kernel-warm'
    sh -c "$DROP_CACHES_CMD"
    check fstest --suite fio --args "--rw=read --bs=1m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.1m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-1m-client-warm-kernel-dropped'
else
    check shell --accept pass,unsupported --req 'community-fio-read-cache-1m-cold' -- \
        sh -c 'echo "DROP_CACHES_CMD not set: kernel page-cache drop unavailable" >&2; exit 2'
    check fstest --suite fio --args "--rw=read --bs=1m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.1m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-1m-kernel-warm'
fi

# ---- BS 4m ----
if [ -n "${DROP_CACHES_CMD:-}" ]; then
    sh -c "$DROP_CACHES_CMD"
    check fstest --suite fio --args "--rw=read --bs=4m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.4m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-4m-cold'
    check fstest --suite fio --args "--rw=read --bs=4m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.4m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-4m-kernel-warm'
    sh -c "$DROP_CACHES_CMD"
    check fstest --suite fio --args "--rw=read --bs=4m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.4m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-4m-client-warm-kernel-dropped'
else
    check shell --accept pass,unsupported --req 'community-fio-read-cache-4m-cold' -- \
        sh -c 'echo "DROP_CACHES_CMD not set: kernel page-cache drop unavailable" >&2; exit 2'
    check fstest --suite fio --args "--rw=read --bs=4m --size=${DATA_MIB}m --numjobs=1 --iodepth=1 --direct=$DIRECT --runtime=$RUNTIME --time_based --unlink=0 --filename=data.4m.bin --allow_file_create=0 --readonly --invalidate=0" --req 'community-fio-read-cache-4m-kernel-warm'
fi

check shell --req 'community-fio-read-cache-integrity' -- sh -c '
    export DRIVE9_SERVER="$SERVER" DRIVE9_API_KEY="$API_KEY"
    "$CLI" fs cat :/data.1m.bin > "$RUN_DIR/rem.1m.bin"
    "$CLI" fs cat :/data.4m.bin > "$RUN_DIR/rem.4m.bin"
    [ "$(d9-stat sha256 data.1m.bin)" = "$(d9-stat sha256 "$RUN_DIR/rem.1m.bin")" ] &&
    [ "$(d9-stat sha256 data.4m.bin)" = "$(d9-stat sha256 "$RUN_DIR/rem.4m.bin")" ] &&
    rm -f "$RUN_DIR/rem.1m.bin" "$RUN_DIR/rem.4m.bin"
'

rm -f data.1m.bin data.4m.bin
drain --mode require-ok --timeout 300s
