#!/usr/bin/env python3
"""Generate a local Git fixture remote for drive9 Git matrix E2E tests."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run(cwd: Path | None, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(cwd) if cwd else None,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed rc={proc.returncode}: {proc.stdout.strip()}")
    return proc.stdout.strip()


def write(path: Path, data: str | bytes, mode: str = "w") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if "b" in mode:
        path.write_bytes(data if isinstance(data, bytes) else data.encode())
    else:
        path.write_text(data if isinstance(data, str) else data.decode(), encoding="utf-8")


def commit(repo: Path, message: str) -> None:
    run(repo, "add", "-A")
    run(repo, "commit", "-m", message)


def init_repo(repo: Path) -> None:
    try:
        run(None, "init", "-b", "main", str(repo))
    except RuntimeError:
        run(None, "init", str(repo))
        run(repo, "checkout", "-B", "main")
    run(repo, "config", "user.email", "drive9-fixture@example.test")
    run(repo, "config", "user.name", "Drive9 Fixture")
    run(repo, "config", "core.filemode", "true")


def build_fixture(root: Path) -> dict[str, str]:
    source = root / "source"
    bare = root / "remote.git"
    if source.exists() or bare.exists():
        raise RuntimeError(f"fixture paths already exist under {root}")
    root.mkdir(parents=True, exist_ok=True)

    init_repo(source)
    write(source / "README.md", "# Drive9 fixture\n\nInitial body.\n")
    write(source / "src" / "app.py", "def main():\n    return 'drive9'\n")
    write(source / "docs" / "guide.md", "# Guide\n\nFixture guide.\n")
    write(source / "script.sh", "#!/usr/bin/env sh\nprintf 'drive9-fixture\\n'\n")
    os.chmod(source / "script.sh", 0o755)
    write(source / "binary.bin", bytes(range(32)), "wb")
    write(source / ".gitignore", "ignored-build/\n*.tmp\nagent-bench/local-only/\n")
    os.symlink("README.md", source / "link-to-readme")
    commit(source, "initial fixture tree")
    run(source, "tag", "v0.1.0")

    run(source, "checkout", "-B", "feature/clean-merge")
    write(source / "feature-clean.txt", "clean merge branch\n")
    commit(source, "feature clean merge")

    run(source, "checkout", "main")
    run(source, "checkout", "-B", "feature/conflict")
    write(source / "README.md", "# Drive9 fixture\n\nConflict branch body.\n")
    commit(source, "feature conflict change")

    run(source, "checkout", "main")
    run(source, "checkout", "-B", "feature/rebase")
    write(source / "docs" / "rebase-upstream.md", "# Rebase upstream\n")
    commit(source, "feature rebase upstream")

    run(source, "checkout", "main")
    run(None, "clone", "--bare", str(source), str(bare))
    run(bare, "config", "uploadpack.allowFilter", "true")
    run(bare, "config", "uploadpack.allowAnySHA1InWant", "true")

    return {
        "source_repo": str(source),
        "bare_repo": str(bare),
        "file_url": bare.resolve().as_uri(),
        "main_branch": "main",
        "tag": "v0.1.0",
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", help="empty directory where fixture repositories will be created")
    parser.add_argument("--force", action="store_true", help="remove the root before creating the fixture")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    if args.force and root.exists():
        shutil.rmtree(root)
    info = build_fixture(root)
    print(json.dumps(info, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
