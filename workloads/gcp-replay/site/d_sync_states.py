"""Supplement D: three sync states, exit, then recover in a cache-free client.

Reuses the recovery group logic from S12 (D1 unsynced / D2 sync-not-confirmed /
D3 sync-confirmed), each in its own mount so the groups cannot interfere.
"""

from __future__ import annotations

from s12_recovery import group_round


def run(report):
    for label, mode in (
        ("d1-nosync", "nosync"),
        ("d2-partial", "partial"),
        ("d3-synced", "synced"),
    ):
        with report.step("group %s" % label):
            row = group_round(report, label, mode)
        report.data.setdefault("groups", []).append(row)


if __name__ == "__main__":
    from common import main_guard

    main_guard(run, "d-sync-states")
