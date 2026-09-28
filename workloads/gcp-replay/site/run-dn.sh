#!/usr/bin/env bash
set -uo pipefail
cd -- "${BASH_SOURCE[0]%/*}" || exit 1
if [[ "${1:-}" == "--list" ]]; then
	exec python3 run_dn.py --list
fi
set -a
# shellcheck source=config.example.env
source config.env || exit 1
set +a
export PATH="${D9_NODE_BIN:?}:/usr/sbin:/sbin:$PATH"
if (($#)); then
	exec python3 run_dn.py "$@"
fi
read -r -a dn_cases <<< "${DN_ONLY:-}"
exec python3 run_dn.py "${dn_cases[@]}"
