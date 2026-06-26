#!/usr/bin/env python3
"""Verify E2E scripts chmod CLI_BIN after building into a mktemp path."""

from __future__ import annotations

from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    failures: list[str] = []

    for script in sorted((repo_root / "e2e").glob("*.sh")):
        lines = script.read_text().splitlines()
        for index, line in enumerate(lines):
            if "make " not in line or "build-cli" not in line or 'CLI_BIN="$CLI_BIN"' not in line:
                continue
            followup = "\n".join(lines[index + 1:index + 4])
            if 'chmod +x "$CLI_BIN"' not in followup:
                failures.append(f"{script.relative_to(repo_root)}:{index + 1}")

    if failures:
        print("missing chmod +x after build-cli CLI_BIN output:")
        for failure in failures:
            print(f"  {failure}")
        return 1

    print("all e2e build-cli CLI_BIN outputs restore execute permission")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
