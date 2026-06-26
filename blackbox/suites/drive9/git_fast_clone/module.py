from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context
from suites.drive9._base import Drive9WorkflowBase


class Drive9GitFastClone(Drive9WorkflowBase):
    id = "drive9.workflow.git_fast_clone"
    description = "drive9 git clone --fast into a FUSE mount, followed by status/edit/commit."

    def run(self, ctx: Context) -> dict[str, Any]:
        bare = ctx.target.create_git_fixture("git-fast-clone-fixture")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("drive9_git_fast_clone", remote, profile="coding-agent")
        try:
            repo = handle.mountpoint / "repo"
            result = ctx.target.drive9("drive9-git-fast-clone", ["git", "clone", "--fast", str(bare), str(repo)], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 git clone --fast failed; see {result.stderr}")
            self.configure_git(ctx, repo)
            self.assert_clean(ctx, repo)
            (repo / "README.md").write_text("# fixture\n\nedited\n", encoding="utf-8")
            ctx.target.capture(["git", "add", "-A"], cwd=repo)
            ctx.target.capture(["git", "commit", "-m", "blackbox edit"], cwd=repo)
            return {"repo": str(repo)}
        finally:
            ctx.target.unmount(handle)
