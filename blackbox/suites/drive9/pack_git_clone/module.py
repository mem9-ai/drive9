from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context
from suites.drive9._base import Drive9WorkflowBase


class Drive9PackGitClone(Drive9WorkflowBase):
    description = "Fast-cloned Git state can be packed and unpacked with Drive9 pack/unpack."

    def run(self, ctx: Context) -> dict[str, Any]:
        bare = ctx.target.create_git_fixture("pack-git-clone-fixture")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        archive = f":{remote}/git-pack.tar.gz"
        h1 = ctx.target.mount("drive9_pack_git_clone", remote, profile="coding-agent", cache_key="pack")
        try:
            repo = h1.mountpoint / "repo"
            result = ctx.target.drive9("drive9-pack-git-clone", ["git", "clone", "--fast", str(bare), str(repo)], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 git clone --fast failed; see {result.stderr}")
            result = ctx.target.drive9("drive9-pack-git", ["pack", "--mount", str(repo), archive, ".git"], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 pack git state failed; see {result.stderr}")
        finally:
            ctx.target.unmount(h1, no_auto_pack=True)
        h2 = ctx.target.mount("drive9_pack_git_clone", remote, profile="coding-agent", cache_key="unpack", extra=["--unpack", archive])
        try:
            repo = h2.mountpoint / "repo"
            if not (repo / ".git").exists():
                raise BlackboxError("unpacked .git missing")
            return {"archive": archive}
        finally:
            ctx.target.unmount(h2, no_auto_pack=True)
