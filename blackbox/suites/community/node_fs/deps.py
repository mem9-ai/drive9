from __future__ import annotations

import json
import os
from pathlib import Path

from harness.core import Context, DependencyUnavailable


def module_cfg() -> dict:
    config_path = Path(__file__).resolve().parent / "config.json"
    with open(config_path, encoding="utf-8") as handle:
        return json.load(handle)


def ensure_dependencies(ctx: Context) -> None:
    ensure_node_fs_deps(ctx)


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
    if not (node_src / "tools" / "test.py").is_file():
        raise DependencyUnavailable(f"node source at {node_src} is missing tools/test.py")

    node_bin = os.environ.get("NODE_BIN", "")
    if node_bin:
        if Path(node_bin).exists():
            return node_src, str(Path(node_bin).resolve())
        raise DependencyUnavailable(f"NODE_BIN={node_bin} does not exist")
    # Exact pin: downloads the official tarball unless the system node already
    # is the pinned version. ensure_node_version resolves "=X.Y.Z" exactly.
    return node_src, ctx.deps.ensure_node_version(f"={version}")
