"""n10 npx 冷/热缓存 + corepack（本地 home 功能校验 + 工作区完整性探测）."""

from __future__ import annotations

import os
import pathlib
import shutil
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402


def count_files(root):
    n = 0
    for _dp, _dn, filenames in os.walk(root):
        n += len(filenames)
    return n


def run(report):
    dn.node_versions(report)
    base = dn.case_dir("n10-npx-corepack")
    cache = dn.STATE / "n10-npx-cache-outside-mount"
    cache.mkdir(parents=True, exist_ok=True)
    env = dn.node_env(npm_config_cache=str(cache))

    t0 = time.time()
    cold = dn.nsh(["npx", "--yes", "cowsay@1.6.0", "dn"], env=env, timeout=900)
    cold_s = time.time() - t0
    t0 = time.time()
    hot = dn.nsh(["npx", "--yes", "cowsay@1.6.0", "dn"], env=env, timeout=900)
    hot_s = time.time() - t0
    report.check(
        cold.returncode == 0 and "dn" in (cold.stdout or ""),
        "npx 冷启动执行成功",
        rc=cold.returncode,
        err=(cold.stderr or "")[-300:],
    )
    report.check(
        hot.returncode == 0 and "dn" in (hot.stdout or ""), "npx 热缓存执行成功"
    )
    report.data.setdefault("metrics", {})["npx_cold_s"] = round(cold_s, 2)
    report.data.setdefault("metrics", {})["npx_hot_s"] = round(hot_s, 2)

    ck_home = dn.STATE / "corepack-home"
    ck_env = dn.node_env(COREPACK_HOME=str(ck_home))
    prep = dn.nsh(["corepack", "prepare", "pnpm@9.15.1"], env=ck_env, timeout=900)
    report.check(
        prep.returncode == 0,
        "corepack prepare pnpm 成功（本地 home）",
        out=(prep.stdout or "")[-200:],
        err=(prep.stderr or "")[-300:],
    )
    ver = dn.nsh(
        ["node", str(ck_home / "v1" / "pnpm" / "9.15.1" / "bin" / "pnpm.cjs"), "-v"],
        env=ck_env,
        timeout=600,
    )
    report.check(
        ver.returncode == 0 and ver.stdout.strip().startswith("9."),
        "本地 corepack 安装的 pnpm 可运行",
        out=(ver.stdout or "").strip()[:40],
        err=(ver.stderr or "")[-200:],
    )
    cli = dn.nsh(["corepack", "pnpm", "-v"], env=ck_env, timeout=600)
    blob = (cli.stdout or "") + (cli.stderr or "")
    keyid = "Cannot find matching keyid" in blob
    dn.record_compat(report, "corepack_cli_keyid_error", keyid)
    report.check(
        True, "corepack CLI 版本解析行为已记录", rc=cli.returncode, keyid_error=keyid
    )

    shim_dir = base / "shims"
    shim_dir.mkdir(parents=True, exist_ok=True)
    en = dn.nsh(
        ["corepack", "enable", "--install-directory", str(shim_dir)],
        env=ck_env,
        timeout=300,
    )
    report.check(
        en.returncode == 0 and (shim_dir / "pnpm").exists(),
        "corepack shim 文件写入",
        rc=en.returncode,
        files=sorted(p.name for p in shim_dir.iterdir())[:8],
    )

    fuse_home = base / "corepack-fuse"
    if fuse_home.exists():
        shutil.rmtree(fuse_home, ignore_errors=True)
    probe = dn.nsh(
        ["corepack", "prepare", "pnpm@9.15.1"],
        env=dn.node_env(COREPACK_HOME=str(fuse_home)),
        timeout=900,
    )
    fuse_files = count_files(fuse_home)
    local_files = count_files(ck_home)
    dn.record_compat(report, "corepack_fuse_files", fuse_files)
    dn.record_compat(report, "corepack_local_files", local_files)
    report.check(
        local_files > 500, "本地 corepack 安装文件数正常", local_files=local_files
    )
    report.check(
        fuse_files >= local_files * 0.95,
        "corepack 安装到工作区（drive9）文件完整",
        fuse_files=fuse_files,
        local_files=local_files,
        rc=probe.returncode,
    )
    report.sync("n10 final")


if __name__ == "__main__":
    main_guard(run, "n10-npx-corepack")
