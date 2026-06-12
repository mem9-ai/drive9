#!/usr/bin/env python3
"""Generate a local Git fixture remote for drive9 Git matrix E2E tests."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
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


def build_fixture(root: Path, tree_files: int = 0) -> dict[str, str]:
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
    for idx in range(tree_files):
        write(source / "tree" / f"dir{idx // 16:02d}" / f"file{idx:04d}.txt", f"fixture tree file {idx}\n")
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
        "tree_files": str(tree_files),
    }


def is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def safe_delete_bases() -> list[Path]:
    bases = [Path(tempfile.gettempdir()), Path("/tmp"), Path("/private/tmp")]
    env_base = os.environ.get("DRIVE9_GIT_FIXTURE_SAFE_DELETE_BASE")
    if env_base:
        bases.append(Path(env_base))
    resolved = []
    for base in bases:
        try:
            resolved.append(base.resolve())
        except OSError:
            continue
    return resolved


def ensure_safe_to_delete(root: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    resolved = root.resolve()
    dangerous = {Path("/").resolve(), Path.home().resolve(), repo_root}
    if (
        resolved in dangerous
        or is_relative_to(repo_root, resolved)
        or is_relative_to(resolved, repo_root)
    ):
        raise ValueError(f"{resolved} is a protected path")

    for base in safe_delete_bases():
        if resolved != base and is_relative_to(resolved, base):
            return
    raise ValueError(
        f"{resolved} is outside the allowed temp roots; set "
        "DRIVE9_GIT_FIXTURE_SAFE_DELETE_BASE to permit a dedicated fixture directory"
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", help="empty directory where fixture repositories will be created")
    parser.add_argument("--force", action="store_true", help="remove the root before creating the fixture")
    parser.add_argument(
        "--tree-files",
        type=int,
        default=0,
        help="number of extra committed files under tree/ (scales checkout-heavy workloads)",
    )
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    if args.force and root.exists():
        try:
            ensure_safe_to_delete(root)
        except ValueError as exc:
            print(f"refusing to remove fixture root: {exc}", file=sys.stderr)
            return 2
        shutil.rmtree(root)
    info = build_fixture(root, tree_files=max(0, args.tree_files))
    print(json.dumps(info, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
