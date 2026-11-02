#!/usr/bin/env bash
# Deprecated entry point. Configuration aliases are handled by the new helper.
echo 'warning: local-minio.sh is deprecated; use local-object-store.sh' >&2
export OBJECT_STORE_LEGACY_SETTINGS=1
exec bash "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/local-object-store.sh" "$@"
