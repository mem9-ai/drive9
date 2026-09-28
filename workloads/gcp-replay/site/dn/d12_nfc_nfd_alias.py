# dn/d12_nfc_nfd_alias.py
"""d12 NFC/NFD 别名收敛：同一规范路径的两种拼写分叉写入.

两种拼写映射到同一远端路径，drain 必须无残留冲突。该场景尚未修复
（名称身份/FileID 绑定，归 drive9 #903 / tidbcloud fs#176），当前预期 FAIL。
"""

from __future__ import annotations

import pathlib
import sys
import unicodedata

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from common import main_guard  # noqa: E402
import dn_common as dn  # noqa: E402


def run(report):
    base = dn.case_dir("d12-nfc-nfd-alias")
    d = base / "work"
    d.mkdir(parents=True, exist_ok=True)

    nfc = unicodedata.normalize("NFC", "café")
    nfd = unicodedata.normalize("NFD", "café")
    a = d / (nfc + "-probe")
    b = d / (nfd + "-probe")
    a.write_text("nfc")
    b.write_text("nfd")
    both = a.exists() and b.exists()
    dn.record_compat(report, "nfc_nfd_both_exist", bool(both))
    report.check(both, "NFC/NFD 两种拼写同时可见", same_string=nfc == nfd)

    ev = report.sync("d12 final")
    report.check(
        ev["ok"],
        "drain 无残留冲突（别名分叉可安全收敛）",
        exit_code=ev.get("exit_code"),
    )


if __name__ == "__main__":
    main_guard(run, "d12-nfc-nfd-alias")
