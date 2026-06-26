from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context, ModuleSkip, ModuleXFail
from harness.module_base import BaseModule


class CommunityPyxattr(BaseModule):
    id = "community.pyxattr"
    category = "community.xattr"
    description = "Run pyxattr-backed extended attribute checks on Drive9 FUSE."
    labels = ("compatibility", "xattr", "community")
    timeout = 120

    def ensure_dependencies(self, ctx: Context) -> None:
        if not ctx.capabilities.get("features", {}).get("xattr"):
            raise ModuleSkip("host Python lacks os xattr helpers", "platform skip")
        ctx.deps.ensure_pyxattr()

    def run(self, ctx: Context) -> dict[str, Any]:
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("community_pyxattr", remote)
        try:
            p = handle.mountpoint / "xattr.txt"
            p.write_text("xattr\n", encoding="utf-8")
            script = (
                "import os, sys\n"
                "p = sys.argv[1]\n"
                "name = 'user.drive9.blackbox'\n"
                "bname = name.encode()\n"
                "os.setxattr(p, bname, b'value')\n"
                "assert os.getxattr(p, bname) == b'value'\n"
                "assert name in os.listxattr(p)\n"
                "os.removexattr(p, bname)\n"
            )
            result = ctx.target.run_cmd("community-pyxattr", ["python3", "-c", script, str(p)], timeout=self.timeout)
            if not result.ok:
                if ctx.capabilities.get("os") == "Darwin":
                    raise ModuleXFail(f"xattr check failed on macFUSE; see {result.stderr}", "known platform incompatibility")
                raise BlackboxError(f"xattr check failed; see {result.stderr}")
            return {"file": str(p)}
        finally:
            ctx.target.unmount(handle)
