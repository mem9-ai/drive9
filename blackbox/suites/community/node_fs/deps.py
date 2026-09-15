from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from harness.core import Context, DependencyUnavailable, write_json


def module_cfg() -> dict:
    config_path = Path(__file__).resolve().parent / "config.json"
    with open(config_path, encoding="utf-8") as handle:
        return json.load(handle)


def ensure_dependencies(ctx: Context) -> None:
    ensure_node_fs_deps(ctx)


def _validate_pinned_binary(node_bin: str, version: str) -> None:
    """Reject a NODE_BIN override whose version differs from the pinned one.

    An arbitrary node changes fs semantics between releases, which would
    silently invalidate the triaged exclusions. NODE_FS_SKIP_NODE_BIN_CHECK=1
    is the explicit escape hatch for local experiments.
    """
    try:
        probe = subprocess.run(
            [node_bin, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DependencyUnavailable(f"cannot query {node_bin} --version: {exc}") from exc
    actual = (probe.stdout or "").strip()
    if probe.returncode != 0 or actual != f"v{version}":
        if os.environ.get("NODE_FS_SKIP_NODE_BIN_CHECK") == "1":
            return
        raise DependencyUnavailable(
            f"NODE_BIN {node_bin} reports {actual or 'no version'} but community.node_fs is pinned to "
            f"v{version}; use the matching binary or set NODE_FS_SKIP_NODE_BIN_CHECK=1 to override"
        )


def ensure_node_fs_deps(ctx: Context) -> tuple[Path, str]:
    """Resolve the pinned Node source checkout and the matching node binary.

    Returns (node_source_root, node_bin). The source tag and binary version are
    pinned together in config.json so results stay comparable across runs;
    bumping the pin is a deliberate change that requires re-triaging the
    exclusions list.
    """
    cfg = module_cfg()
    version = str(cfg.get("node_version", "")).strip()
    ref = str(cfg.get("node_source_ref", "")).strip()
    url = str(cfg.get("node_source_url", "https://github.com/nodejs/node.git"))
    if not version or not ref:
        raise DependencyUnavailable("community.node_fs config.json is missing node_version/node_source_ref")

    if os.environ.get("NODE_FS_SOURCE"):
        source_root = Path(os.environ["NODE_FS_SOURCE"]).expanduser().resolve()
        if (source_root / "tools" / "test.py").is_file():
            node_src = source_root
        else:
            raise DependencyUnavailable(f"NODE_FS_SOURCE={source_root} has no tools/test.py")
    else:
        node_src = ctx.deps.ensure_git_clone("node", url, ref)
        write_json(
            node_src / ".drive9-blackbox-dependency.json",
            {
                "name": "node",
                "source": url,
                "ref": ref,
                "license": "MIT",
                "pinned_binary_version": version,
            },
        )
    if not (node_src / "tools" / "test.py").is_file():
        raise DependencyUnavailable(f"node source at {node_src} is missing tools/test.py")

    node_bin = os.environ.get("NODE_BIN", "")
    if node_bin:
        resolved = Path(node_bin).expanduser().resolve()
        if not resolved.exists():
            raise DependencyUnavailable(f"NODE_BIN={node_bin} does not exist")
        _validate_pinned_binary(str(resolved), version)
        return node_src, str(resolved)
    # Exact pin: downloads the official tarball unless the system node already
    # is the pinned version. ensure_node_version resolves "=X.Y.Z" exactly.
    return node_src, ctx.deps.ensure_node_version(f"={version}")
