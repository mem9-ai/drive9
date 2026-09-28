"""Summarize the dn (agent/node corner-case) suite.

usage: python3 dn/summarize_dn.py
writes results/DN-SUMMARY.json and results/DN-SUMMARY.md
"""

from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from common import RESULT_DIR, TARGET  # noqa: E402

ORDER = [
    ("d01-append-tail", "D01 追加日志+跟随读"),
    ("d02-kill-resume", "D02 写入中被杀后续写"),
    ("d03-patch-apply", "D03 补丁应用"),
    ("d04-multi-agent", "D04 多 agent 并发"),
    ("d05-lock-semantics", "D05 锁语义"),
    ("d06-watcher-loop", "D06 watcher 闭环"),
    ("d07-exec-after-write", "D07 写完立即执行"),
    ("d08-temp-storm", "D08 临时文件风暴"),
    ("d09-fd-replaced", "D09 持句柄被删/替换"),
    ("d10-weird-names", "D10 名称边界"),
    ("d11-deep-wide-scan", "D11 深宽目录扫描"),
    ("n01-pnpm-layout", "N01 pnpm 布局"),
    ("n02-workspaces", "N02 npm workspaces"),
    ("n03-tsc-incremental", "N03 TS 增量构建"),
    ("n04-vitest", "N04 vitest 快照"),
    ("n05-pack-tarball", "N05 pack+tarball"),
    ("n06-ci-interrupt", "N06 ci 中断重跑"),
    ("n07-cache-concurrency", "N07 并发安装+cache"),
    ("n08-rm-node-modules", "N08 删除 node_modules"),
    ("n09-node-watch", "N09 node watch 事件"),
    ("n10-npx-corepack", "N10 npx+corepack"),
    ("d12-nfc-nfd-alias", "D12 NFC/NFD 别名收敛"),
    ("n11-cache-inside-mount", "N11 挂载内共享 npm cache"),
]


def collect():
    rows = []
    for name, title in ORDER:
        path = RESULT_DIR / (name + ".json")
        if not path.exists():
            rows.append(
                {
                    "scenario": name,
                    "title": title,
                    "verified": None,
                    "status": "not-run",
                }
            )
            continue
        data = json.loads(path.read_text())
        rows.append(
            {
                "scenario": name,
                "title": title,
                "verified": data.get("verified"),
                "duration_s": data.get("duration_s"),
                "failures": data.get("failures", []),
                "failed_checks": [
                    c["name"] for c in data.get("checks", []) if not c.get("ok")
                ],
                "sync_ok": all(s.get("ok") for s in data.get("sync_evidence", []))
                if data.get("sync_evidence")
                else None,
                "compat": data.get("compat"),
                "metrics": data.get("metrics"),
                "unsupported": {
                    k: v
                    for k, v in (data.get("compat") or {}).items()
                    if v is False or (isinstance(v, str) and v in ("mixed",))
                },
            }
        )
    return rows


def main():
    rows = collect()
    RESULT_DIR.joinpath("DN-SUMMARY.json").write_text(
        json.dumps({"target": TARGET, "rows": rows}, indent=2, ensure_ascii=False)
    )

    lines = [
        "---",
        "title: dn suite summary",
        "---",
        "",
        "target: %s" % TARGET,
        "",
        "| case | verified | duration | sync(drain) | failed checks |",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        verified = row["verified"]
        mark = "pass" if verified else ("FAIL" if verified is False else "not-run")
        duration = "%.1fs" % row["duration_s"] if row.get("duration_s") else "-"
        sync = {True: "ok", False: "FAIL", None: "-"}[row.get("sync_ok")]
        failed = "; ".join(row.get("failed_checks", [])[:3]) or "-"
        lines.append(
            "| %s | %s | %s | %s | %s |" % (row["title"], mark, duration, sync, failed)
        )
    lines += ["", "## 兼容性记录（compat）", ""]
    for row in rows:
        if row.get("compat"):
            lines.append(
                "- **%s**: %s"
                % (row["title"], json.dumps(row["compat"], ensure_ascii=False))
            )
    RESULT_DIR.joinpath("DN-SUMMARY.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
