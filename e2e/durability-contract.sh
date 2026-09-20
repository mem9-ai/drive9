#!/usr/bin/env bash
# Optional durability override shared by E2E suites invoked by Autopilot.
#
# The first argument is the suite's historical mode.  Use an empty string for
# suites that historically omitted --durability and inherited the CLI default.
# Without DRIVE9_E2E_DURABILITY this helper preserves that exact behavior.

drive9_e2e_init_durability() {
  if [ "$#" -ne 1 ]; then
    echo "drive9_e2e_init_durability requires exactly one legacy mode argument" >&2
    return 64
  fi
  local legacy_mode="$1"
  case "$legacy_mode" in
    ""|interactive|fsync|close-sync|write-sync) ;;
    *)
      echo "invalid legacy E2E durability: $legacy_mode" >&2
      return 64
      ;;
  esac

  if [ -n "${DRIVE9_E2E_DURABILITY:-}" ]; then
    case "$DRIVE9_E2E_DURABILITY" in
      interactive|fsync|close-sync|write-sync) ;;
      *)
        echo "invalid DRIVE9_E2E_DURABILITY=$DRIVE9_E2E_DURABILITY" >&2
        return 64
        ;;
    esac
    DRIVE9_E2E_EFFECTIVE_DURABILITY="$DRIVE9_E2E_DURABILITY"
    DRIVE9_E2E_DURABILITY_OVERRIDDEN=1
  else
    DRIVE9_E2E_EFFECTIVE_DURABILITY="$legacy_mode"
    DRIVE9_E2E_DURABILITY_OVERRIDDEN=0
  fi
  export DRIVE9_E2E_EFFECTIVE_DURABILITY DRIVE9_E2E_DURABILITY_OVERRIDDEN
}

drive9_e2e_print_mount_argv() {
  printf 'mount_argv='
  printf ' %q' "$@"
  printf '\n'
}
