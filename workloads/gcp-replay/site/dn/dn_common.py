"""Shared helpers for the dn suite (agent file-usage + node ecosystem corners).

Reuses the site-acceptance framework (Report/drain/api in common.py); test
workspaces live under <mount>/site-acceptance/dn/<case>/.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import time

_KIT = pathlib.Path(__file__).resolve().parent.parent
if str(_KIT) not in sys.path:
    sys.path.insert(0, str(_KIT))

from common import (
    NODE_BIN,
    STATE_ROOT,
    drain,
    main_guard,  # noqa: E402,F401
    sha256_file,
    workdir,
)

DN = pathlib.Path(__file__).resolve().parent
WORKER = DN / "dn_worker.py"
STATE = STATE_ROOT / "dn"
TOOLS = STATE_ROOT / "tools"
NPM_CACHE = STATE / "npm-cache"
LOGDIR = STATE / "logs"
for _p in (STATE, TOOLS, NPM_CACHE, LOGDIR):
    _p.mkdir(parents=True, exist_ok=True)

TOOL_BIN = TOOLS / "node_modules" / ".bin"


def node_env(**extra):
    env = dict(os.environ)
    parts = [str(TOOL_BIN)]
    if NODE_BIN:
        parts.append(NODE_BIN)
    env["PATH"] = os.pathsep.join(parts + [env.get("PATH", "")])
    env.setdefault("npm_config_cache", str(NPM_CACHE))
    env.setdefault("npm_config_update_notifier", "false")
    env.setdefault("npm_config_fund", "false")
    env.setdefault("npm_config_audit", "false")
    env.update(extra)
    return env


def nsh(args, timeout=900, cwd=None, env=None, **kw):
    kw.setdefault("capture_output", True)
    kw.setdefault("text", True)
    return subprocess.run(
        [str(a) for a in args],
        timeout=timeout,
        cwd=str(cwd) if cwd else None,
        env=env or node_env(),
        **kw,
    )


def nrun(args, timeout=900, cwd=None, env=None):
    proc = nsh(args, timeout=timeout, cwd=cwd, env=env)
    if proc.returncode != 0:
        raise RuntimeError(
            "rc=%s %s\nstdout=%s\nstderr=%s"
            % (
                proc.returncode,
                args,
                (proc.stdout or "")[-1500:],
                (proc.stderr or "")[-1500:],
            )
        )
    return proc


def spawn_worker(subcmd, args, log_path):
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [sys.executable, str(WORKER), subcmd, *[str(a) for a in args]],
        env=node_env(),
        stdout=log,
        stderr=log,
        stdin=subprocess.DEVNULL,
    )
    proc.dn_log = log
    return proc


def spawn_bg(args, log_path, env=None, cwd=None):
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [str(a) for a in args],
        cwd=str(cwd) if cwd else None,
        env=env or node_env(),
        stdout=log,
        stderr=log,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    proc.dn_log = log
    return proc


def kill_group(proc):
    import signal

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        pass
    close_log(proc)


def close_log(proc):
    log = getattr(proc, "dn_log", None)
    if log is not None:
        try:
            log.close()
        except Exception:
            pass


def wait_proc(proc, timeout=600):
    try:
        return proc.wait(timeout=timeout)
    finally:
        close_log(proc)


def kill9(proc):
    import signal

    try:
        os.kill(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    close_log(proc)


def wait_for(pred, timeout=60, interval=0.2, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    raise TimeoutError("timeout waiting for %s" % what)


def case_dir(name):
    return workdir("dn", sub=name)


def read_json(path):
    return json.loads(pathlib.Path(path).read_text())


def node_versions(report):
    node = nsh(["node", "-v"], timeout=60)
    npm = nsh(["npm", "-v"], timeout=60)
    nv = (node.stdout or "").strip()
    nvpm = (npm.stdout or "").strip()
    report.data["node_version"] = nv
    report.data["npm_version"] = nvpm
    report.check(nv.startswith("v22."), "node 22 可用", got=nv)
    report.check(nvpm.startswith("10."), "npm 10 可用", got=nvpm)
    return nv, nvpm


def record_compat(report, key, value):
    report.data.setdefault("compat", {})[key] = value


def tree_sha(root):
    """{relpath: sha256|symlink:<target>} for all entries under root."""
    from common import sha256_file as _sha

    root = pathlib.Path(root)
    out = {}
    for p in sorted(root.rglob("*")):
        rel = str(p.relative_to(root))
        if p.is_symlink():
            out[rel] = "symlink:" + os.readlink(p)
        elif p.is_file():
            out[rel] = _sha(p)
    return out
