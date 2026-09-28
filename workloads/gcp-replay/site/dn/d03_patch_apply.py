"""d03 补丁应用：git apply / patch -p0，含失败补丁原子性与 index.lock 清理."""

from __future__ import annotations

import os
import pathlib
import stat
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402


def run(report):
    base = dn.case_dir("d03-patch-apply")
    repo = base / "repo"
    repo.mkdir(parents=True, exist_ok=True)
    env = dn.node_env(
        GIT_CONFIG_GLOBAL=str(base / "gitconfig"), GIT_CONFIG_SYSTEM="/dev/null"
    )
    (base / "gitconfig").write_text("")

    dn.nrun(["git", "init", "-q"], cwd=repo, env=env)
    dn.nrun(["git", "config", "user.email", "dn@test"], cwd=repo, env=env)
    dn.nrun(["git", "config", "user.name", "dn"], cwd=repo, env=env)
    src = repo / "src"
    src.mkdir(exist_ok=True)
    for i in range(20):
        (src / ("f%02d.txt" % i)).write_text("base-%02d\n" % i + "line\n" * 20)
    (src / "run.sh").write_text("#!/bin/sh\necho base\n")
    dn.nrun(["git", "add", "-A"], cwd=repo, env=env)
    dn.nrun(["git", "commit", "-q", "-m", "base"], cwd=repo, env=env)

    for i in range(5):
        (src / ("f%02d.txt" % i)).write_text("patched-%02d\n" % i + "line\n" * 30)
    (src / "new.txt").write_text("new-file\n")
    os.chmod(src / "run.sh", 0o755)
    dn.nrun(["git", "add", "-A"], cwd=repo, env=env)
    patch_text = dn.nrun(["git", "diff", "--cached"], cwd=repo, env=env).stdout
    patch_path = base / "change.patch"
    patch_path.write_text(patch_text)
    dn.nrun(["git", "reset", "-q", "--hard", "HEAD"], cwd=repo, env=env)

    dn.nrun(["git", "apply", str(patch_path)], cwd=repo, env=env)
    report.check(
        all(
            (src / ("f%02d.txt" % i)).read_text().startswith("patched-%02d" % i)
            for i in range(5)
        ),
        "git apply 后修改文件内容正确",
    )
    report.check(
        (src / "new.txt").read_text() == "new-file\n", "git apply 后新增文件正确"
    )
    report.check(
        bool(os.stat(src / "run.sh").st_mode & stat.S_IXUSR), "git apply 后执行位保留"
    )
    report.check(
        not (repo / ".git" / "index.lock").exists(), "正常应用后无 index.lock 残留"
    )

    dn.nrun(["git", "add", "-A"], cwd=repo, env=env)
    dn.nrun(["git", "commit", "-q", "-m", "patched"], cwd=repo, env=env)
    st = dn.nrun(["git", "status", "--porcelain"], cwd=repo, env=env)
    report.check(st.stdout.strip() == "", "提交后工作树干净", got=st.stdout[:200])

    before = dn.tree_sha(repo)
    bad_path = base / "broken.patch"
    bad_path.write_text(patch_text + "this is not a patch\n")
    proc = dn.nsh(["git", "apply", str(bad_path)], cwd=repo, env=env)
    after = dn.tree_sha(repo)
    report.check(proc.returncode != 0, "坏补丁返回非零", rc=proc.returncode)
    report.check(before == after, "坏补丁未改动工作树")
    report.check(
        not (repo / ".git" / "index.lock").exists(), "坏补丁后无 index.lock 残留"
    )

    d2 = base / "patchcase"
    d2.mkdir(parents=True, exist_ok=True)
    (d2 / "a.txt").write_text("one\n")
    (d2 / "b.txt").write_text("one\n")
    dn.nsh(["diff", "-u", "a.txt", "b.txt"], cwd=d2, timeout=60)
    (d2 / "a.txt").write_text("two\n")
    diff = dn.nsh(["diff", "-u", "a.txt", "b.txt"], cwd=d2, timeout=60).stdout
    (d2 / "p.diff").write_text(diff)
    proc = dn.nsh(["bash", "-c", "patch a.txt < p.diff"], cwd=d2, timeout=60)
    report.check(
        proc.returncode == 0 and (d2 / "a.txt").read_text() == "one\n",
        "patch 应用正常",
        rc=proc.returncode,
        out=(proc.stdout or "")[-200:],
    )

    report.sync("d03 final")


if __name__ == "__main__":
    main_guard(run, "d03-patch-apply")
