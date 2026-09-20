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

# Reuse an explicitly supplied, already-built Drive9 CLI when Autopilot owns
# the build.  Standalone invocations leave DRIVE9_CLI_BIN unset/empty and keep
# each suite's historical mktemp/build path unchanged.  Return 1 only for the
# no-override case; malformed non-empty overrides fail closed with EX_USAGE.
drive9_e2e_use_cli_override() {
  # Reset both the selected path and its ownership before inspecting a new
  # request.  In particular, a valid external override followed by an invalid
  # override must never leave the old external path eligible for cleanup.
  CLI_BIN=""
  DRIVE9_E2E_CLI_OVERRIDE_ACTIVE=0
  local candidate="${DRIVE9_CLI_BIN:-}"
  [ -n "$candidate" ] || return 1
  case "$candidate" in
    /*) ;;
    *)
      echo "DRIVE9_CLI_BIN must be an absolute canonical path" >&2
      return 64
      ;;
  esac
  if [ -L "$candidate" ] || [ ! -f "$candidate" ] || [ ! -x "$candidate" ]; then
    echo "DRIVE9_CLI_BIN must be a regular executable file (not a symlink)" >&2
    return 64
  fi
  local parent canonical
  parent="$(cd "$(dirname "$candidate")" 2>/dev/null && pwd -P)" || return 64
  canonical="$parent/$(basename "$candidate")"
  if [ "$canonical" != "$candidate" ]; then
    echo "DRIVE9_CLI_BIN must be an absolute canonical path" >&2
    return 64
  fi
  CLI_BIN="$candidate"
  DRIVE9_E2E_CLI_OVERRIDE_ACTIVE=1
  return 0
}

drive9_e2e_cleanup_cli_bin() {
  if [ "${DRIVE9_E2E_CLI_OVERRIDE_ACTIVE:-0}" != "1" ] && [ -n "${CLI_BIN:-}" ]; then
    rm -f "$CLI_BIN"
  fi
}

drive9_e2e_print_mount_argv() {
  local rendered
  rendered="$(drive9_e2e_render_mount_argv "$@")" || return
  printf 'mount_argv=%s\n' "$rendered"
}

drive9_e2e_validate_mount_argv() {
  if [ "$#" -lt 2 ]; then
    echo "drive9 E2E mount argv requires a CLI and mount subcommand" >&2
    return 64
  fi
  if [ "$2" != "mount" ]; then
    echo "drive9 E2E mount argv must use the mount subcommand" >&2
    return 64
  fi
  if [ -z "$1" ]; then
    echo "drive9 E2E mount argv requires a non-empty CLI" >&2
    return 64
  fi
  local token
  local LC_ALL=C
  for token in "$@"; do
    case "$token" in
      *[[:cntrl:]]*)
        echo "drive9 E2E mount argv contains a control character" >&2
        return 64
        ;;
    esac
  done
}

# Render with Bash's portable %q form, then prove that the Python/shlex
# consumer used by Autopilot reconstructs the exact original token vector.
# Unsupported representations fail before a mount can run.
drive9_e2e_render_mount_argv() {
  drive9_e2e_validate_mount_argv "$@" || return
  local rendered
  printf -v rendered ' %q' "$@"
  if ! python3 - "$rendered" "$@" <<'PY'
import shlex
import sys

if shlex.split(sys.argv[1]) != sys.argv[2:]:
    raise SystemExit(64)
PY
  then
    echo "drive9 E2E mount argv failed shell-word roundtrip" >&2
    return 64
  fi
  printf '%s' "$rendered"
}

drive9_e2e_validate_mount_role() {
  if [ "$#" -ne 1 ]; then
    echo "drive9_e2e_validate_mount_role requires exactly one role" >&2
    return 64
  fi
  case "$1" in
    ""|*[!a-z0-9-]*|-*|*-)
      echo "invalid E2E mount role: $1" >&2
      return 64
      ;;
  esac
}

# Emit a role-bound, machine-readable argv record immediately before a real
# mount attempt.  Keep the legacy mount_argv line for existing consumers.  The
# two lines are rendered from the same argv array so role evidence cannot drift
# from the command that the suite executes.
drive9_e2e_print_mount_evidence() {
  if [ "$#" -lt 3 ]; then
    echo "drive9_e2e_print_mount_evidence requires role, CLI, and mount argv" >&2
    return 64
  fi
  local role="$1"
  shift
  drive9_e2e_validate_mount_role "$role" || return
  local rendered_argv rendered_role
  rendered_argv="$(drive9_e2e_render_mount_argv "$@")" || return
  printf -v rendered_role ' %q' "$role"
  printf 'mount_evidence=%s%s\n' "$rendered_role" "$rendered_argv"
  printf 'mount_argv=%s\n' "$rendered_argv"
}

drive9_e2e_print_mount_role_plan() {
  if [ "$#" -eq 0 ]; then
    echo "drive9_e2e_print_mount_role_plan requires at least one role" >&2
    return 64
  fi
  local role
  local seen=$'\n'
  for role in "$@"; do
    drive9_e2e_validate_mount_role "$role" || return
    case "$seen" in
      *$'\n'"$role"$'\n'*)
        echo "duplicate E2E mount role in plan: $role" >&2
        return 64
        ;;
    esac
    seen="${seen}${role}"$'\n'
  done
  printf 'mount_role_plan='
  printf ' %q' "$@"
  printf '\n'
}

# Arguments are newline-delimited canonical role sequences.  Keeping this
# primitive string based makes it compatible with macOS Bash 3 (no namerefs).
drive9_e2e_assert_mount_role_plan() {
  if [ "$#" -ne 2 ]; then
    echo "drive9_e2e_assert_mount_role_plan requires observed and expected sequences" >&2
    return 64
  fi
  drive9_e2e_validate_mount_role_sequence "$1" || return
  drive9_e2e_validate_mount_role_sequence "$2" || return
  if [ "$1" != "$2" ]; then
    printf 'mount role sequence mismatch: observed=%q expected=%q\n' "$1" "$2" >&2
    return 1
  fi
}

drive9_e2e_validate_mount_role_sequence() {
  if [ "$#" -ne 1 ]; then
    echo "drive9_e2e_validate_mount_role_sequence requires one sequence" >&2
    return 64
  fi
  local role
  local seen=$'\n'
  while IFS= read -r role; do
    drive9_e2e_validate_mount_role "$role" || return
    case "$seen" in
      *$'\n'"$role"$'\n'*)
        echo "duplicate E2E mount role in sequence: $role" >&2
        return 64
        ;;
    esac
    seen="${seen}${role}"$'\n'
  done <<< "$1"
}
