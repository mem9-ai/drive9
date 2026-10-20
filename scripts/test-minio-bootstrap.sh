#!/usr/bin/env bash
# Deprecated entry point; the canonical suite uses generic object-store settings.
exec bash "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/test-object-store-bootstrap.sh" "$@"
