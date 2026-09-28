"""Summarize all scenario/supplement results into a single report.

usage: python3 summarize.py
writes results/SUMMARY.json and results/SUMMARY.md
"""

from __future__ import annotations

import json
import pathlib

from common import RESULT_DIR, TARGET

ORDER = [
    ("s01-import", "S1 导入完整项目"),
    ("s02-save-read", "S2 保存代码后立即读取"),
    ("s03-watch", "S3 持续编辑时预览发现变化"),
    ("s04-assets", "S4 批量安装素材"),
    ("s05-conflict", "S5 争用同名素材"),
    ("s06-npm", "S6 安装依赖/包管理器"),
    ("s07-refactor", "S7 重构移动删除"),
    ("s08-git", "S8 Git 切换版本"),
    ("s09-build", "S9 构建"),
    ("s10-export", "S10 打包导出"),
    ("s11-external", "S11 独立客户端查看"),
    ("s12-recovery", "S12 恢复项目文件"),
    ("a-fsync-interrupt", "补充A fsync 期间断连"),
    ("b-mount-retry", "补充B 首次挂载失败重连"),
    ("c-response-lost", "补充C 远端完成响应未达"),
    ("d-sync-states", "补充D 三种同步状态恢复"),
    ("e-attributes", "补充E 属性重挂载保留"),
    ("f-cache-cap", "补充F 缓存保护阈值"),
]


def collect():
    rows = []
    for name, title in ORDER:
        path = RESULT_DIR / (name + ".json")
        if not path.exists():
            rows.append({"scenario": name, "title": title, "verified": None,
                         "status": "not-run"})
            continue
        data = json.loads(path.read_text())
        rows.append({
            "scenario": name,
            "title": title,
            "verified": data.get("verified"),
            "duration_s": data.get("duration_s"),
            "failures": data.get("failures", []),
            "checks": len(data.get("checks", [])),
            "failed_checks": [c["name"] for c in data.get("checks", []) if not c.get("ok")],
            "sync_ok": all(s.get("ok") for s in data.get("sync_evidence", [])) if data.get("sync_evidence") else None,
            "metrics": data.get("metrics"),
            "notes": {k: v for k, v in data.items()
                      if k in ("inotify_supported", "inotify_event_count",
                               "open_handle_semantics", "attributes",
                               "executed_remotely", "not_triggered",
                               "unsynced_groups", "blocked_ips",
                               "blocked_response_ips", "remote_proofs",
                               "stage", "npm_version", "node_version")},
        })
    return rows


def main():
    rows = collect()
    RESULT_DIR.joinpath("SUMMARY.json").write_text(json.dumps(
        {"target": TARGET, "rows": rows}, indent=2, ensure_ascii=False))

    lines = ["# Site acceptance summary", "", "target: fe581b9f (FUSE)", "",
             "| scenario | verified | duration | failed checks |", "|---|---|---|---|"]
    for row in rows:
        verified = row["verified"]
        mark = "pass" if verified else ("FAIL" if verified is False else "not-run")
        duration = "%.1fs" % row["duration_s"] if row.get("duration_s") else "-"
        failed = "; ".join(row["failed_checks"][:3]) or "-"
        lines.append("| %s | %s | %s | %s |" % (row["title"], mark, duration, failed))
    RESULT_DIR.joinpath("SUMMARY.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
