from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context
from .drive9_workflow_base import Drive9WorkflowBase


class Drive9GitBlobless(Drive9WorkflowBase):
    id = "drive9.workflow.git_blobless"
    description = "drive9 git clone --fast --blobless into a FUSE mount with synchronous hydrate."

    def run(self, ctx: Context) -> dict[str, Any]:
        bare = ctx.target.create_git_fixture("git-blobless-fixture")
        remote = ctx.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("drive9_git_blobless", remote, profile="coding-agent")
        try:
            repo = handle.mountpoint / "repo"
            result = ctx.target.drive9("drive9-git-blobless", ["git", "clone", "--fast", "--blobless", "--hydrate=sync", str(bare), str(repo)], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 git clone --fast --blobless failed; see {result.stderr}")
            self.configure_git(ctx, repo)
            self.assert_clean(ctx, repo)
            if (repo / "src" / "app.py").read_text(encoding="utf-8") != "print('fixture')\n":
                raise BlackboxError("blobless hydrated file content mismatch")
            return {"repo": str(repo)}
        finally:
            ctx.target.unmount(handle)
