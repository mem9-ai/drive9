from __future__ import annotations

from typing import Any

from harness.core import BlackboxError, Context
from suites.drive9._base import Drive9WorkflowBase


class Drive9GitWorktree(Drive9WorkflowBase):
    description = "drive9 git worktree add/remove --fast on top of a fast-cloned FUSE repository."

    def run(self, ctx: Context) -> dict[str, Any]:
        bare = ctx.target.create_git_fixture("git-worktree-fixture")
        remote = ctx.target.remote_root(self.id)
        ctx.target.mkdir_remote(remote)
        handle = ctx.target.mount("drive9_git_worktree", remote, profile="coding-agent")
        try:
            repo = handle.mountpoint / "repo"
            result = ctx.target.drive9("drive9-git-worktree-clone", ["git", "clone", "--fast", str(bare), str(repo)], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 git clone --fast failed; see {result.stderr}")
            wt = handle.mountpoint / "repo-wt"
            result = ctx.target.drive9("drive9-git-worktree-add", ["git", "worktree", "add", "--fast", str(repo), str(wt)], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 git worktree add --fast failed; see {result.stderr}")
            if not (wt / "README.md").exists():
                raise BlackboxError("worktree README missing")
            result = ctx.target.drive9("drive9-git-worktree-remove", ["git", "worktree", "remove", "--fast", "--force", str(wt)], timeout=self.timeout)
            if not result.ok:
                raise BlackboxError(f"drive9 git worktree remove --fast failed; see {result.stderr}")
            return {"repo": str(repo), "worktree": str(wt)}
        finally:
            ctx.target.unmount(handle)
