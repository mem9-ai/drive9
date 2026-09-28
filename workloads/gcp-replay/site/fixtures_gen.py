"""Deterministic fixtures for the site acceptance suite.

All fixtures are generated once and cached under fixtures/ (marked with
.fixture-ready) so scenarios can share identical inputs.
"""

from __future__ import annotations

import os
import pathlib
import random
import shutil
import subprocess
import tarfile

from common import FIXTURES, manifest_diff, tree_manifest, write_file


# ----------------------------------------------------------- project fixture


def project_plan(file_count):
    """Deterministic list of (relpath, size, mode) for a website project."""
    plan = []
    rng = random.Random(20260916)

    # Fixed skeleton with tricky names: empty files, hidden files, unicode,
    # spaces, deep nesting, permissions.
    skeleton = [
        ("index.html", 2400, 0o644),
        ("about.html", 1800, 0o644),
        ("404.html", 700, 0o644),
        (".gitignore", 120, 0o644),
        (".env.example", 90, 0o600),
        (".well-known/security.txt", 80, 0o644),
        ("README.md", 900, 0o644),
        ("LICENSE", 1100, 0o644),
        ("deploy.sh", 260, 0o755),
        ("empty.txt", 0, 0o644),
        ("empty-dir/.keep", 0, 0o644),
        ("中文说明.md", 512, 0o644),
        ("带 空格 的 文件.txt", 300, 0o644),
        ("styles/main.css", 5200, 0o644),
        ("styles/theme-dark.css", 3100, 0o644),
        ("styles/print.css", 900, 0o644),
        ("styles/vendor/reset.css", 1400, 0o644),
        ("scripts/app.js", 6400, 0o644),
        ("scripts/router.js", 3300, 0o644),
        ("scripts/vendor/lodash.min.js", 24000, 0o644),
        ("assets/logo.svg", 2200, 0o644),
        ("assets/favicon.ico", 4096, 0o644),
        ("assets/images/hero.jpg", 65536, 0o644),
        ("assets/images/背景 图.png", 32768, 0o644),
        ("assets/fonts/inter.woff2", 16384, 0o644),
        ("assets/empty.svg", 0, 0o644),
        ("content/posts/hello-world.md", 1200, 0o644),
        ("content/posts/世界你好.md", 1100, 0o644),
        ("content/pages/pricing.md", 800, 0o644),
        ("data/config.json", 420, 0o644),
        ("data/i18n/zh-CN.json", 2600, 0o644),
        ("data/i18n/en-US.json", 2500, 0o644),
        ("src/main.jsx", 1400, 0o644),
        ("src/App.jsx", 2200, 0o644),
        ("src/components/Header.jsx", 1600, 0o644),
        ("src/components/Footer.jsx", 1400, 0o644),
        ("src/features/auth/login.js", 900, 0o644),
        ("src/features/auth/logout.js", 400, 0o644),
        ("tests/smoke.test.js", 700, 0o644),
        ("tests/fixtures/sample.json", 300, 0o644),
    ]
    plan.extend(skeleton)

    # Fill up to file_count with deterministic component/page/asset files.
    index = 0
    while len(plan) < file_count:
        bucket = index % 5
        if bucket == 0:
            rel = "src/components/generated/Comp%04d.jsx" % index
            size = 400 + (index * 37) % 2600
        elif bucket == 1:
            rel = "src/pages/page-%04d/index.js" % index
            size = 500 + (index * 53) % 3000
        elif bucket == 2:
            rel = "content/posts/post-%04d.md" % index
            size = 200 + (index * 29) % 1500
        elif bucket == 3:
            rel = "assets/media/img-%04d.bin" % index
            size = 4096 + (index * 131) % 60000
        else:
            rel = "src/features/feature-%04d/impl.js" % index
            size = 300 + (index * 17) % 1200
        mode = 0o755 if index % 97 == 0 else 0o644
        plan.append((rel, size, mode))
        index += 1
    return plan[:file_count]


def ensure_project(file_count):
    """Generate (once) a project fixture directory; returns its path."""
    name = "project-%d" % file_count
    dest = FIXTURES / name
    if (dest / ".fixture-ready").exists():
        return dest
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    plan = project_plan(file_count)
    for i, (rel, size, mode) in enumerate(plan):
        path = dest / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        data = b"" if size == 0 else random.Random(9000 + i).randbytes(size)
        with open(path, "wb") as fh:
            fh.write(data)
        os.chmod(path, mode)
    (dest / ".fixture-ready").write_text("ok\n")
    return dest


def ensure_project_tarball(file_count):
    name = "project-%d" % file_count
    tar_path = FIXTURES / (name + ".tar.gz")
    if tar_path.exists():
        return tar_path
    src = ensure_project(file_count)
    make_tar(src, tar_path)
    return tar_path


def make_tar(src, tar_path, *, exclude_ready=True):
    src = pathlib.Path(src)
    with tarfile.open(tar_path, "w:gz") as tf:
        for path in sorted(src.rglob("*")):
            rel = path.relative_to(src)
            if exclude_ready and rel.name == ".fixture-ready":
                continue
            info = tf.gettarinfo(str(path), arcname=str(rel))
            info.mtime = 1700000000
            info.uid = info.gid = 1000
            info.uname = info.gname = "ubuntu"
            if info.isfile():
                with open(path, "rb") as fh:
                    tf.addfile(info, fh)
            else:
                tf.addfile(info)
    return tar_path


# ------------------------------------------------------------ asset fixture


def asset_plan(count):
    """Deterministic [(kind, size)] mix covering text/image/font/archive."""
    if count >= 200:
        return (
            [("text", 4096)] * 150
            + [("image", 1 << 20)] * 40
            + [("archive", 20 << 20)] * 10
        )
    return (
        [("text", 4096)] * 12 + [("image", 1 << 20)] * 6 + [("archive", 20 << 20)] * 2
    )


def ensure_assets(count):
    """Generate (once) count assets; returns directory path."""
    name = "assets-%d" % count
    dest = FIXTURES / name
    if (dest / ".fixture-ready").exists():
        return dest
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    for i, (kind, size) in enumerate(asset_plan(count)):
        ext = {"text": "txt", "image": "jpg", "font": "woff2", "archive": "zip"}[kind]
        data = random.Random(70000 + i).randbytes(size)
        write_file(dest / ("%s-%04d.%s" % (kind, i, ext)), data)
    (dest / ".fixture-ready").write_text("ok\n")
    return dest


# -------------------------------------------------------------- git fixture


def _git(repo, *args):
    return subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.com",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _apply_version(repo, version):
    """Mutate the working tree for version A or B, deterministically."""
    src = pathlib.Path(repo)
    if version == "A":
        write_file(src / "index.html", b"<html>A</html>\n" + b"A" * 2000)
        write_file(src / "styles/main.css", b"/* A */\n" + b"A" * 4000)
        write_file(src / "assets/logo.svg", b"<svg>A</svg>\n")
        (src / "pages").mkdir(exist_ok=True)
        for i in range(20):
            write_file(
                src / "pages" / ("page-%02d.html" % i),
                b"<html>A page %d</html>\n" % i + b"A" * 300,
            )
        (src / "legacy").mkdir(exist_ok=True)
        for i in range(10):
            write_file(src / "legacy" / ("old-%02d.txt" % i), b"A-old-%d\n" % i)
    else:
        write_file(src / "index.html", b"<html>B</html>\n" + b"B" * 3500)
        write_file(src / "styles/main.css", b"/* B */\n" + b"B" * 2500)
        write_file(src / "assets/logo.svg", b"<svg>B</svg>\n")
        # modify existing pages
        for i in range(20):
            write_file(
                src / "pages" / ("page-%02d.html" % i),
                b"<html>B page %d</html>\n" % i + b"B" * 500,
            )
        # delete half of legacy
        for i in range(0, 10, 2):
            path = src / "legacy" / ("old-%02d.txt" % i)
            if path.exists():
                path.unlink()
        # add new dir
        (src / "new").mkdir(exist_ok=True)
        for i in range(6):
            write_file(
                src / "new" / ("fresh-%02d.md" % i), b"B-fresh-%d\n" % i + b"B" * 200
            )
        # replace binary image
        write_file(src / "assets/hero.jpg", b"B" * 9000)


def ensure_git_repo():
    """Repo with tags vA / vB (add/delete/modify/move). Returns path."""
    dest = FIXTURES / "git-repo"
    if (dest / ".fixture-ready").exists():
        return dest
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(dest)], check=True, timeout=60
    )
    # base commit
    _apply_version(dest, "A")
    _git(dest, "add", "-A")
    _git(dest, "commit", "-q", "-m", "vA")
    _git(dest, "tag", "vA")
    # second commit
    _apply_version(dest, "B")
    _git(dest, "add", "-A")
    _git(dest, "commit", "-q", "-m", "vB")
    _git(dest, "tag", "vB")
    (dest / ".fixture-ready").write_text("ok\n")
    return dest


# ------------------------------------------------------------------ driver


def ensure_all():
    ensure_project_tarball(100)
    ensure_project_tarball(1000)
    ensure_assets(20)
    ensure_assets(200)
    ensure_git_repo()


if __name__ == "__main__":
    import sys

    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which == "all":
        ensure_all()
        print("fixtures ready at", FIXTURES)
    elif which == "project":
        print(ensure_project_tarball(int(sys.argv[2])))
    elif which == "assets":
        print(ensure_assets(int(sys.argv[2])))
    elif which == "git":
        print(ensure_git_repo())
    else:
        raise SystemExit("unknown fixture: " + which)
