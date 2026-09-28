"""Validate the completed serial matrix and emit reproducible summaries."""

import csv
import hashlib
import json
import pathlib
import re
import statistics
import sys
import zipfile

BASE = pathlib.Path(__file__).resolve().parent
GROUPS = ("none-a", "none-b", "coding-a", "coding-b", "efs", "ebs")
CASES = ("rename", "atomic-replace", "ls-stat-consistency", "copy-file", "copy-tree",
         "overwrite-delete-recreate", "many-small-files", "medium-file", "concurrent-files", "permissions")


def stats(values):
    return {"count": len(values), "median": statistics.median(values), "mean": statistics.mean(values),
            "min": min(values), "max": max(values)} if values else None


def main():
    status = json.loads((BASE / "status.json").read_text())
    assert status["mode"] == "full" and status["state"] in ("completed", "completed_with_failures"), status
    rows = [json.loads(p.read_text()) for p in (BASE / "results").glob("*.json") if not p.name.endswith(".workload.json")]
    measured = [r for r in rows if r["stage"] == "measured"]
    warmup = [r for r in rows if r["stage"] == "warmup"]
    smoke = [r for r in rows if r["stage"] == "smoke"]
    assert len(measured) == 60 and len(warmup) == 60 and len(smoke) == 60, (len(measured), len(warmup), len(smoke))
    assert len({r["label"] for r in rows}) == len(rows)
    windows = sorted((r["start_epoch"], r["end_epoch"], r["label"]) for r in rows if "start_epoch" in r and "end_epoch" in r)
    overlaps = [(a[2], b[2]) for a, b in zip(windows, windows[1:]) if a[1] > b[0]]
    assert not overlaps, overlaps
    assert all(r["drain_after"]["result"]["ok"] for r in rows)
    for row in measured + warmup:
        assert row["scale"] == 1 if "scale" in row else not row["verified"]
        if row["verified"]:
            assert row["exit_code"] == 0
            assert abs(row["duration_s"] - sum(row["phases_s"].values())) < 1e-6
            if row["group"] in GROUPS[:4] and row.get("sample"):
                assert row["remote_sample_verified"]
            if row["group"] in GROUPS[:4] and row.get("absent"):
                assert row["remote_absence_verified"]
    matrix = {}
    for case in CASES:
        matrix[case] = {}
        for group in GROUPS:
            samples = sorted((r for r in measured if r["case"] == case and r["group"] == group), key=lambda r: r["round"])
            assert [r["round"] for r in samples] == list(range(1, 2)), (case, group)
            valid = [r for r in samples if r["verified"]]
            phases = sorted({key for r in valid for key in r["phases_s"]})
            matrix[case][group] = {
                "attempted": 1, "valid": len(valid), "failed": 1-len(valid),
                "duration_s": stats([r["duration_s"] for r in valid]),
                "drain_after_s": stats([r["drain_after"]["wall_s"] for r in valid]),
                "phases_s": {key: stats([r["phases_s"][key] for r in valid]) for key in phases},
                "rounds": [{"round": r["round"], "duration_s": r.get("duration_s"), "valid": r["verified"],
                            "drain_after_s": r["drain_after"]["wall_s"], "error": r.get("error") or r.get("postcheck_error")} for r in samples],
            }
    ratios = {}
    for case in CASES:
        ratios[case] = {}
        for group in GROUPS[:4]:
            numerator = matrix[case][group]["duration_s"]
            ratios[case][group] = {}
            for baseline in ("efs", "ebs"):
                denominator = matrix[case][baseline]["duration_s"]
                ratios[case][group][baseline] = numerator["median"] / denominator["median"] if numerator and denominator else None
    report = {"version": "dev (main fea9cdcf)", "sha256": "95dc072b10f6605f1ffdc95d2bf2de230fadd79a3813d2dca9e1107915144b22",
              "timing_definition": "sum of timed operation phases; setup, validation and post-case drain are separate",
              "measured_attempted": len(measured), "measured_valid": sum(r["verified"] for r in measured),
              "warmup_attempted": len(warmup), "warmup_valid": sum(r["verified"] for r in warmup),
              "smoke_valid": sum(r["verified"] for r in smoke), "cross_case_overlaps": overlaps,
              "wall_start_epoch": min(r["start_epoch"] for r in measured),
              "wall_end_epoch": max(r["end_epoch"] for r in measured),
              "matrix": matrix, "median_elapsed_ratios": ratios,
              "failures": [r for r in rows if not r["verified"]],
              "atomic_qualification": json.loads((BASE / "atomic-qualification.json").read_text())}
    (BASE / "summary.json").write_text(json.dumps(report, indent=2))
    with (BASE / "measured-rounds.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["case", "group", "round", "valid", "duration_s", "drain_after_s", "start_epoch", "end_epoch"])
        for row in sorted(measured, key=lambda r: (r["case"], r["group"], r["round"])):
            writer.writerow([row["case"], row["group"], row["round"], row["verified"], row.get("duration_s"),
                             row["drain_after"]["wall_s"], row.get("start_epoch"), row.get("end_epoch")])
    print(json.dumps({k: report[k] for k in ("measured_attempted", "measured_valid", "warmup_attempted", "warmup_valid", "smoke_valid", "cross_case_overlaps")}), flush=True)
    for case in CASES:
        print(case + " " + " ".join(f"{g}={matrix[case][g]['duration_s']['median']:.6f}" if matrix[case][g]["duration_s"] else f"{g}=FAILED" for g in GROUPS), flush=True)
    archive_files = list((BASE / "results").glob("*.json"))
    for pattern in ("*.py", "mounts.json", "atomic*.json", "status.json", "summary.json", "measured-rounds.csv", "full-launch.json", "final-environment.json"):
        archive_files.extend(BASE.glob(pattern))
    archive_files.extend((BASE / "efs-atomic-retry-20260911-01").glob("*.json"))
    manifest = {}
    with zipfile.ZipFile(BASE / "performance-evidence.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(set(archive_files)):
            body = path.read_bytes()
            assert not re.search(rb"(?i)x-amz-signature=[0-9a-f]{32,}", body), "signed URL detected: " + path.name
            assert not re.search(rb"Bearer\s+[A-Za-z0-9_-]{20,}", body), "credential detected: " + path.name
            assert not re.search(rb"gh[pousr]_[A-Za-z0-9]{20,}", body), "GitHub token detected: " + path.name
            assert "credentials" not in path.relative_to(BASE).parts
            name = str(path.relative_to(BASE))
            manifest[name] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            archive.writestr(name, body)
        archive.writestr("MANIFEST.json", json.dumps(manifest, indent=2))
    print("ARCHIVE " + str((BASE / "performance-evidence.zip").stat().st_size) + " bytes", flush=True)


if __name__ == "__main__":
    main()
