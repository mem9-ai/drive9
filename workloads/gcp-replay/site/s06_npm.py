"""Scenario 6: install dependencies and run the package manager (npm).

The fixture project depends only on local `file:` tarballs, so npm runs fully
offline while still exercising: mass small-file creation, .bin symlinks, lock
file updates, temp-dir rename and removal of old packages.

Acceptance checks:
- successful installs produce readable package files with correct content
- declared symlinks resolve
- deletes/replacements show up in directory enumeration (no ghost packages)
- add / upgrade / remove dependency flows end in a consistent tree
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import tarfile

from common import (
    FIXTURES,
    LOCAL,
    manifest_diff,
    read_file,
    run_checked,
    sh,
    tree_manifest,
    workdir,
)
from fixtures_gen import make_tar

from config import NODE_BIN


def npm_env():
    env = dict(os.environ)
    env["PATH"] = NODE_BIN + ":" + env.get("PATH", "")
    env["npm_config_update_notifier"] = "false"
    env["npm_config_audit"] = "false"
    env["npm_config_fund"] = "false"
    return env


def npm(args, cwd, cache=None, timeout=900, check=True):
    cmd = [os.path.join(NODE_BIN, "npm"), "--no-audit", "--no-fund"]
    if cache:
        cmd += ["--cache", str(cache)]
    cmd += list(args)
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=npm_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            "npm %s failed (%s)\nstdout=%s\nstderr=%s"
            % (
                " ".join(args),
                proc.returncode,
                proc.stdout[-1500:],
                proc.stderr[-1500:],
            )
        )
    return proc


def make_pkg(path, name, version, *, deps=None, with_bin=False, size=800):
    """Create a minimal npm package tarball (package/...)."""
    root = path.parent / (name + "-" + version)
    if root.exists():
        shutil.rmtree(root)
    (root / "package").mkdir(parents=True)
    pkg = {"name": name, "version": version, "main": "index.js"}
    if deps:
        pkg["dependencies"] = deps
    if with_bin:
        pkg["bin"] = {name: "cli.js"}
    (root / "package" / "package.json").write_text(json.dumps(pkg, indent=2) + "\n")
    (root / "package" / "index.js").write_text(
        "module.exports = function %s(){ return %r; };\n"
        % (name.replace("-", "_"), name + "@" + version)
    )
    if with_bin:
        (root / "package" / "cli.js").write_text(
            "#!/usr/bin/env node\nconsole.log(%r);\n" % (name + "@" + version)
        )
    # pad the package with a few deterministic files
    for i in range(4):
        (root / "package" / ("lib-%d.js" % i)).write_text(
            "// %s file %d\n" % (name, i) + "x" * size
        )
    make_tar(root, path, exclude_ready=False)
    shutil.rmtree(root)
    return path


def ensure_npm_project():
    dest = FIXTURES / "npm-project"
    if (dest / ".fixture-ready").exists():
        return dest
    if dest.exists():
        shutil.rmtree(dest)
    (dest / "vendor").mkdir(parents=True)
    make_pkg(dest / "vendor" / "pkg-a-1.0.0.tgz", "pkg-a", "1.0.0", with_bin=True)
    make_pkg(
        dest / "vendor" / "pkg-b-1.0.0.tgz",
        "pkg-b",
        "1.0.0",
        deps={"pkg-c": "file:./pkg-c-1.0.0.tgz"},
    )
    make_pkg(dest / "vendor" / "pkg-c-1.0.0.tgz", "pkg-c", "1.0.0")
    make_pkg(dest / "vendor" / "pkg-d-1.0.0.tgz", "pkg-d", "1.0.0", with_bin=True)
    make_pkg(dest / "vendor" / "pkg-e-1.0.0.tgz", "pkg-e", "1.0.0")
    # upgrade variant used later
    make_pkg(dest / "vendor" / "pkg-e-1.1.0.tgz", "pkg-e", "1.1.0")

    manifest = {
        "name": "site-fixture",
        "version": "1.0.0",
        "private": True,
        "dependencies": {
            "pkg-a": "file:./vendor/pkg-a-1.0.0.tgz",
            "pkg-b": "file:./vendor/pkg-b-1.0.0.tgz",
            "pkg-c": "file:./vendor/pkg-c-1.0.0.tgz",
            "pkg-d": "file:./vendor/pkg-d-1.0.0.tgz",
        },
        "scripts": {"check": "node -e \"require('pkg-a'); console.log('ok')\""},
    }
    (dest / "package.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (dest / "index.js").write_text("require('pkg-a'); require('pkg-b');\n")
    (dest / ".fixture-ready").write_text("ok\n")
    return dest


def check_node_modules(report, app, label):
    modules = app / "node_modules"
    problems = []
    for name in ("pkg-a", "pkg-b", "pkg-c", "pkg-d"):
        pkg_json = modules / name / "package.json"
        if not pkg_json.exists():
            problems.append({"pkg": name, "why": "missing"})
            continue
        try:
            meta = json.loads(pkg_json.read_text())
        except Exception as err:
            problems.append({"pkg": name, "why": "unreadable: %s" % err})
            continue
        if meta.get("name") != name:
            problems.append(
                {"pkg": name, "why": "name mismatch: %s" % meta.get("name")}
            )
    report.check(
        not problems, "%s: installed packages readable" % label, problems=problems
    )

    # .bin symlinks must resolve
    bin_dir = modules / ".bin"
    links = []
    if bin_dir.exists():
        for entry in sorted(bin_dir.iterdir()):
            if entry.is_symlink():
                target = os.readlink(entry)
                resolved = (entry.parent / target).resolve()
                links.append(
                    {
                        "name": entry.name,
                        "target": target,
                        "resolves": resolved.exists(),
                    }
                )
    broken = [row for row in links if not row["resolves"]]
    report.check(bool(links), "%s: .bin symlinks created" % label, links=links[:6])
    report.check(not broken, "%s: .bin symlinks resolve" % label, broken=broken)


def run(report):
    if not NODE_BIN:
        # no toolchain on this host: record as not-covered instead of failing
        report.check(
            True,
            "S6 not covered: D9_NODE_BIN is not configured",
            note="set D9_NODE_BIN to a directory containing node and npm",
        )
        return
    root = workdir("s06-npm")
    fixture = ensure_npm_project()
    app = root / "app"
    shutil.copytree(fixture, app, ignore=shutil.ignore_patterns(".fixture-ready"))
    cache = root / "npm-cache"

    report.data["fixture"] = str(fixture)
    report.data["npm_version"] = sh(
        [os.path.join(NODE_BIN, "npm"), "--version"]
    ).stdout.strip()
    report.data["node_version"] = sh(
        [os.path.join(NODE_BIN, "node"), "--version"]
    ).stdout.strip()

    # ---- 1) cold install (no cache) -------------------------------------
    with report.step("npm install (cold cache)"):
        proc = npm(["install", "--prefer-offline"], app, cache=cache)
    report.data["cold_install"] = {
        "stdout_tail": proc.stdout[-800:],
        "stderr_tail": proc.stderr[-400:],
    }
    check_node_modules(report, app, "cold install")

    # ---- 2) warm install (cache present) --------------------------------
    with report.step("npm install (warm cache, existing node_modules)"):
        proc = npm(["install"], app, cache=cache)
    check_node_modules(report, app, "warm install")
    report.check((app / "package-lock.json").exists(), "package-lock.json written")

    # ---- 3) lockfile reinstall (npm ci) ---------------------------------
    with report.step("npm ci (lockfile reinstall)"):
        shutil.rmtree(app / "node_modules")
        proc = npm(["ci"], app, cache=cache)
    check_node_modules(report, app, "npm ci")
    report.check(
        not [p for p in (app / "node_modules").iterdir() if p.name.startswith(".pkg-")],
        "npm ci leaves no stale temp packages",
        entries=[p.name for p in (app / "node_modules").iterdir()][:8],
    )

    # ---- 4) add a dependency --------------------------------------------
    with report.step("add dependency pkg-e"):
        npm(["install", "file:./vendor/pkg-e-1.0.0.tgz"], app, cache=cache)
    pkg_e = app / "node_modules" / "pkg-e" / "package.json"
    report.check(
        pkg_e.exists() and json.loads(pkg_e.read_text()).get("version") == "1.0.0",
        "added dependency present with expected version",
    )

    # ---- 5) upgrade the dependency --------------------------------------
    with report.step("upgrade dependency pkg-e -> 1.1.0"):
        npm(["install", "file:./vendor/pkg-e-1.1.0.tgz"], app, cache=cache)
    report.check(
        json.loads(pkg_e.read_text()).get("version") == "1.1.0",
        "upgraded dependency version visible",
    )

    # ---- 6) remove a dependency -----------------------------------------
    with report.step("remove dependency pkg-d"):
        npm(["uninstall", "pkg-d"], app, cache=cache)
    report.check(
        not (app / "node_modules" / "pkg-d").exists(),
        "removed package no longer in node_modules",
    )
    listing = sorted(p.name for p in (app / "node_modules").iterdir())
    report.check(
        "pkg-d" not in listing, "removed package not listed", listing=listing[:12]
    )

    # ---- 7) load the package at runtime ---------------------------------
    with report.step("node resolves pkg-a via require"):
        proc = sh(
            [
                os.path.join(NODE_BIN, "node"),
                "-e",
                "const a=require('pkg-a'); console.log(typeof a)",
            ],
            cwd=str(app),
        )
    report.check(
        proc.stdout.strip() == "function",
        "pkg-a loads through node require",
        stdout=proc.stdout.strip(),
        stderr=proc.stderr[-300:],
    )

    # ---- 8) npm cache lives on the mount --------------------------------
    cache_entries = sum(1 for _ in cache.rglob("*")) if cache.exists() else 0
    report.check(
        cache.exists() and cache_entries > 0,
        "npm cache directory on drive9 populated",
        entries=cache_entries,
    )

    sync = report.sync("after s06")
    report.check(sync["ok"], "drain succeeded", drain=sync.get("result"))


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "s06-npm")
