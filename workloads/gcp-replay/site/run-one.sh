#!/usr/bin/env bash
set -uo pipefail
cd -- "${BASH_SOURCE[0]%/*}" || exit 1
if (($# != 1)); then
	printf 'Usage: %s s02_save_read.py\n' "$0" >&2
	exit 2
fi
exec bash run-all.sh "$1"
