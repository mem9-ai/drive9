"""Supplement E: do file attributes survive a restart and a cache-free remount?

Checks mode / mtime / xattr per attribute, immediately, after a client restart,
and after a cache-free remount. Unsupported attributes are reported as such.
"""

from __future__ import annotations

from s12_recovery import attribute_check


def run(report):
    with report.step("attribute checks (mode / mtime / xattr)"):
        attribute_check(report)


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "e-attributes")
