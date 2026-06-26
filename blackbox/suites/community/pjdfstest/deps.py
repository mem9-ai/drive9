from __future__ import annotations
import os
from harness.core import Context, ModuleSkip
from harness.core import DependencyUnavailable

def ensure_dependencies(ctx: Context) -> None:
    if not ctx.capabilities.get("is_root") and os.environ.get("PJDFSTEST_ALLOW_NONROOT", "0") != "1":
        raise ModuleSkip("pjdfstest normally requires root; set PJDFSTEST_ALLOW_NONROOT=1 to run anyway", "platform skip")
    ctx.deps.ensure_prove()
    tests_dir, bin_path = ctx.deps.ensure_pjdfstest()
    # Store for the module to pick up
    ctx.deps._pjdfstest_tests = tests_dir
    ctx.deps._pjdfstest_bin = bin_path
