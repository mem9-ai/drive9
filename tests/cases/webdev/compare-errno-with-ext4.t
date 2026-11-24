#!/bin/sh
# Differential POSIX errno contract probe via built-in `fstest errno-diff`.
# Compares application-visible errnos across two passes:
#   pass 1 (remote):     $MOUNT root; parity + documented remote divergences asserted
#   pass 2 (local-only): $MOUNT/.errno-local (--overlay '**/.errno-local/**') +
#                        cross-layer EXDEV against $MOUNT/.errno-alt (family X)
set -eu

REQ_EXT4=""
if [ "${REQUIRE_EXT4:-0}" = "1" ]; then
    REQ_EXT4="--require-ext4"
fi

REF_DIR="${EXT4_BASE:-$RUN_DIR/errno-ref}"
mkdir -p "$REF_DIR"
REF_ARG="--ref-dir '$REF_DIR'"

WSARGS=""
if [ -n "${GIT_WS_SRC:-}" ]; then
    drive9 git clone --fast "$GIT_WS_SRC" "$MOUNT/.errno-gitsrc"
    printf 'marker\n' > "$MOUNT/.errno-gitsrc/errno-link-marker"
    WSARGS="--git-ws-root '$MOUNT/.errno-gitsrc'"
fi

TABLE_REMOTE="$RUN_DIR/errno-comparison-table.txt"
TABLE_LOCAL="$RUN_DIR/errno-comparison-table-local.txt"

check fstest --suite errno-diff \
    --args "$REQ_EXT4 $REF_ARG $WSARGS --table-out '$TABLE_REMOTE'" \
    --req 'errno-cmp-remote' --accept pass,unsupported

mkdir -p "$MOUNT/.errno-local"
MOUNT="$MOUNT/.errno-local" check fstest --suite errno-diff \
    --args "$REQ_EXT4 $REF_ARG --diverge-as-record --suffix=-local --alt-root '$MOUNT/.errno-alt' --table-out '$TABLE_LOCAL'" \
    --req 'errno-cmp-local' --accept pass,unsupported

rmdir "$MOUNT/.errno-local" "$MOUNT/.errno-alt" 2>/dev/null || true

check shell --req 'errno-cmp-complete' --claim 'tables-produced' --accept pass,unsupported -- \
    sh -c "
      if [ ! -f '$TABLE_REMOTE' ] && [ '${REQUIRE_EXT4:-0}' = '1' ]; then
        exit 2
      fi
      test -f '$TABLE_REMOTE' && test -f '$TABLE_LOCAL' && grep -q 'total=' '$TABLE_LOCAL'
    "
