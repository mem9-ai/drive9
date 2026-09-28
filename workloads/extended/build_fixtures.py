#!/usr/bin/env python3
"""Build fixtures for the extended cases (on the root disk, not a mount)."""

import json
import subprocess
import zipfile

from cases import FIXTURES, file_content, file_name


ZIP_SMOKE = 300
ZIP_FULL = 15000
REPO_SMOKE = 52
REPO_SMOKE_NAME = "clone-smoke.git"
REPO_FULL_NAME = "drive9.git"
REPO_FULL_URL = "https://github.com/mem9-ai/drive9.git"


def build_zip(count):
    path = FIXTURES / f"unzip-{count}.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for i in range(count):
            archive.writestr(file_name(i), file_content(i))
    print("zip", path, path.stat().st_size, flush=True)


def build_smoke_repo(count, name):
    work = FIXTURES / f"{name}-work"
    bare = FIXTURES / name
    subprocess.run(["rm", "-rf", str(work), str(bare)], check=True)
    work.mkdir(parents=True)
    for i in range(count):
        (work / file_name(i)).write_bytes(file_content(i))
    subprocess.run(["git", "init", "-q", "-b", "main", str(work)], check=True)
    subprocess.run(["git", "-C", str(work), "config", "user.email", "bench@example.com"], check=True)
    subprocess.run(["git", "-C", str(work), "config", "user.name", "bench"], check=True)
    subprocess.run(["git", "-C", str(work), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(work), "commit", "-q", "-m", "fixture"], check=True)
    subprocess.run(["git", "clone", "-q", "--bare", str(work), str(bare)], check=True)
    subprocess.run(["rm", "-rf", str(work)], check=True)
    return bare


def fetch_real_repo(name):
    bare = FIXTURES / name
    subprocess.run(["rm", "-rf", str(bare)], check=True)
    subprocess.run(["git", "clone", "-q", "--bare", "--depth=1", REPO_FULL_URL, str(bare)], check=True)
    return bare


def tracked_count(bare):
    out = subprocess.check_output(["git", "--git-dir", str(bare), "ls-tree", "-r", "--name-only", "HEAD"])
    return len([line for line in out.split(b"\n") if line])


def head_sha(bare):
    return subprocess.check_output(
        ["git", "--git-dir", str(bare), "rev-parse", "HEAD"], text=True).strip()


def main():
    FIXTURES.mkdir(parents=True, exist_ok=True)
    for count in (ZIP_SMOKE, ZIP_FULL):
        build_zip(count)
    smoke_bare = build_smoke_repo(REPO_SMOKE, REPO_SMOKE_NAME)
    full_bare = fetch_real_repo(REPO_FULL_NAME)
    manifest = {
        "zip_smoke": ZIP_SMOKE,
        "zip_full": ZIP_FULL,
        "repo_smoke": tracked_count(smoke_bare),
        "repo_full": tracked_count(full_bare),
        "repo_smoke_name": REPO_SMOKE_NAME,
        "repo_full_name": REPO_FULL_NAME,
        "repo_full_head": head_sha(full_bare),
        "repo_full_url": REPO_FULL_URL,
    }
    (FIXTURES / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print("manifest", manifest, flush=True)


if __name__ == "__main__":
    main()
