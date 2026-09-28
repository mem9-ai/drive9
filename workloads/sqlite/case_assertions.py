#!/usr/bin/env python3
"""Assertions for the Drive9 SQLite 50 MiB WAL checkpoint case.

This helper only reads sanitized test artifacts. It never reads a Drive9 API
key and uses only Python's standard library.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


KNOWN_BAD_SHA = "e11bfde7be732663af47ade790df7808bf302a03"


def load_json(path: str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise AssertionError(f"{path}: expected a JSON object")
    return value


def load_jsonl(path: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with Path(path).open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise AssertionError(
                    f"{path}:{line_number}: expected a JSON object"
                )
            records.append(value)
    return records


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def require_checkpoint(checkpoint: dict[str, Any], label: str) -> None:
    require(checkpoint.get("ok") is True, f"{label}: ok is not true")
    require(checkpoint.get("result") == [0, 0, 0], f"{label}: not [0,0,0]")
    require(not checkpoint.get("timed_out", False), f"{label}: timed out")
    require(
        checkpoint.get("worker_close_hung") is False,
        f"{label}: worker close hung",
    )


def require_verification(value: dict[str, Any], expected_acks: int) -> None:
    require(value.get("ok") is True, "verification: ok is not true")
    require(
        value.get("expected_ack_count") == expected_acks,
        "verification: unexpected expected_ack_count",
    )
    require(
        value.get("actual_applied_count") == expected_acks,
        "verification: unexpected actual_applied_count",
    )
    require(value.get("sequence_exact") is True, "verification: sequence mismatch")
    require(value.get("changed_mismatches") == [], "verification: changed rows differ")
    require(value.get("stable_mismatches") == [], "verification: stable rows differ")
    require(value.get("state_exact") is True, "verification: state row differs")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def authority_metadata(path: Path) -> tuple[int, int, bool]:
    value = load_json(str(path))
    require(value.get("state") == "present", f"{path}: object is not present")
    revision = int(value.get("revision", 0))
    size = int(value.get("size", -1))
    is_dir = value.get("is_dir")
    require(revision > 0, f"{path}: revision is not positive")
    require(size >= 0, f"{path}: size is negative")
    require(is_dir is False, f"{path}: object is not a regular file")
    return revision, size, is_dir


def require_stable_authority(
    directory: Path,
    label: str,
    suffix: str,
) -> tuple[int, int, str]:
    metadata = [
        authority_metadata(directory / f"{label}-stat-{index}.json")
        for index in range(3)
    ]
    require(len(set(metadata)) == 1, f"{label}: authority metadata changed")
    revision, size, _ = metadata[0]
    downloads = [directory / f"{label}-{index}.{suffix}" for index in (1, 2)]
    digests = []
    for path in downloads:
        require(path.stat().st_size == size, f"{path}: download size mismatch")
        digests.append(file_sha256(path))
    require(len(set(digests)) == 1, f"{label}: authority SHA-256 changed")
    return revision, size, digests[0]


def check_pass_result(args: argparse.Namespace) -> None:
    result = load_json(args.result)
    require(result.get("status") == "ok", "result status is not ok")
    require(
        result.get("settings")
        == {
            "journal_mode": "wal",
            "mmap_size": 0,
            "page_size": 4096,
            "synchronous": 2,
            "wal_autocheckpoint": 0,
        },
        "unexpected SQLite settings",
    )

    pre = result["pre_checkpoint"]
    require(pre.get("mixed_commits") == 64, "pre-checkpoint commits are not 64")
    require(pre.get("update_commits") == 32, "pre-checkpoint updates are not 32")
    require(pre.get("insert_commits") == 32, "pre-checkpoint inserts are not 32")
    require(pre.get("wal_frames") == 1000, "checkpoint WAL is not 1000 frames")
    require(pre.get("wal_bytes") == 4_120_032, "checkpoint WAL size is wrong")

    require_checkpoint(result["primary_checkpoint"], "primary checkpoint")
    require_checkpoint(result["final_checkpoint"], "final checkpoint")

    overlap = result["overlap_writer"]
    require(overlap.get("ok") is True, "overlap writer failed")
    require(
        overlap.get("worker_close_hung") is False,
        "overlap writer close hung",
    )

    reader = result["reader_status"]
    require(reader.get("ok") is True, "reader failed")
    require(reader.get("worker_close_hung") is False, "reader close hung")
    require(result.get("reader_unfinished") is None, "reader operation unfinished")
    windows = result["reader_window_counts"]
    require(int(windows.get("before", 0)) > 0, "no pre-checkpoint reads")
    require(int(windows.get("after", 0)) > 0, "no post-checkpoint reads")

    live = result["live_verification"]
    require_verification(live, 105)
    require(
        live.get("expected_filler_seq") == pre.get("filler_commits"),
        "filler sequence mismatch",
    )
    require(
        result["post_checkpoint"].get("mixed_commits") == 40,
        "post-checkpoint commits are not 40",
    )


def load_last_perf(path: str) -> dict[str, Any]:
    samples = load_jsonl(path)
    require(bool(samples), "missing perf samples")
    sample = samples[-1]
    require(sample.get("reason") == "stop", "final perf sample is not stop")
    return sample


def require_perf_context(sample: dict[str, Any], candidate_sha: str) -> None:
    context = sample.get("context", {})
    require(context.get("git_hash") == candidate_sha, "perf git_hash mismatch")
    require(context.get("sync_mode") == "strict", "perf sync_mode is not strict")
    require(
        context.get("write_policy") == "writeback",
        "perf write_policy is not writeback",
    )
    require(
        context.get("profile") == "coding-agent",
        "perf profile is not coding-agent",
    )


def check_pass_perf(args: argparse.Namespace) -> None:
    sample = load_last_perf(args.perf)
    require_perf_context(sample, args.candidate_sha)
    remote = sample.get("remote_ops", {})
    append = remote.get("append_log", {})
    write = remote.get("write", {})
    fsync = sample.get("fuse_ops", {}).get("fsync", {})
    counters = sample.get("counters", {})
    queues = sample.get("queues", {})

    require(int(append.get("count", 0)) > 0, "append-log was not used")
    require(int(append.get("errors", 0)) == 0, "append-log remote error")
    require(int(write.get("count", 0)) > 0, "no remote main write observed")
    require(int(write.get("errors", 0)) == 0, "remote write error")
    require(int(fsync.get("errors", 0)) == 0, "FUSE fsync error")
    require(
        int(counters.get("append_log_fsync_append_count", 0)) > 0,
        "no append-log fsync append observed",
    )
    require(
        int(counters.get("append_log_outcome_conflict", 0)) == 0,
        "append-log outcome conflict",
    )
    require(
        int(counters.get("append_log_outcome_error", 0)) == 0,
        "append-log outcome error",
    )
    for field in ("commit_pending", "uploader_queued", "uploader_in_flight"):
        require(int(queues.get(field, 0)) == 0, f"nonzero final queue: {field}")


def check_xfail_result(args: argparse.Namespace) -> None:
    require(args.candidate_sha == KNOWN_BAD_SHA, "not the pinned known-bad SHA")
    require(args.return_code == 2, "known failure return code is not 2")
    result = load_json(args.result)
    require(result.get("status") == "checkpoint_failed", "wrong failure status")
    pre = result["pre_checkpoint"]
    require(pre.get("wal_frames") == 1000, "known failure WAL frame mismatch")
    require(pre.get("wal_bytes") == 4_120_032, "known failure WAL size mismatch")
    checkpoint = result["primary_checkpoint"]
    require(checkpoint.get("ok") is False, "known checkpoint unexpectedly passed")
    require(
        not checkpoint.get("timed_out", False),
        "known checkpoint timed out",
    )
    require(
        checkpoint.get("worker_close_hung") is False,
        "known checkpoint worker close hung",
    )
    error = checkpoint["error"]
    require(error.get("sqlite_errorcode") == 1034, "not SQLite error 1034")
    require(
        error.get("sqlite_errorname") == "SQLITE_IOERR_FSYNC",
        "not SQLITE_IOERR_FSYNC",
    )
    require(error.get("message") == "disk I/O error", "wrong SQLite message")
    overlap = result["overlap_writer"]
    require(overlap.get("ok") is True, "overlap writer failed")
    require(
        overlap.get("worker_close_hung") is False,
        "known overlap writer close hung",
    )
    reader = result["reader_status"]
    require(reader.get("ok") is True, "reader failed")
    require(
        reader.get("worker_close_hung") is False,
        "known reader close hung",
    )
    require(
        result.get("reader_unfinished") is None,
        "known reader operation unfinished",
    )
    require(
        result.get("keeper_close_skipped_after_failure") is True,
        "failure evidence did not preserve the keeper-close boundary",
    )

    acknowledgements = load_jsonl(args.acks)
    require(len(acknowledgements) == 65, "known failure ack count is not 65")
    sequences = [int(item["sequence"]) for item in acknowledgements]
    require(sequences == list(range(1, 66)), "known failure ack sequence differs")

    mount_log = Path(args.mount_log).read_text(encoding="utf-8", errors="replace")
    needle = "flush upload failed for /tmp/case50/main.db: revision conflict"
    require(needle in mount_log, "main.db revision-conflict log is absent")


def check_xfail_perf(args: argparse.Namespace) -> None:
    require(args.candidate_sha == KNOWN_BAD_SHA, "not the pinned known-bad SHA")
    sample = load_last_perf(args.perf)
    require_perf_context(sample, args.candidate_sha)
    remote = sample.get("remote_ops", {})
    append = remote.get("append_log", {})
    write = remote.get("write", {})
    fsync = sample.get("fuse_ops", {}).get("fsync", {})
    counters = sample.get("counters", {})

    require(int(append.get("count", 0)) > 0, "append-log was not used")
    require(int(append.get("errors", 0)) == 0, "WAL append had remote errors")
    require(
        int(counters.get("append_log_fsync_append_count", 0)) > 0,
        "no append-log fsync append observed",
    )
    require(
        int(counters.get("append_log_outcome_conflict", 0)) == 0,
        "WAL append conflict differs from known failure",
    )
    require(
        int(counters.get("append_log_outcome_error", 0)) == 0,
        "WAL append error differs from known failure",
    )
    require(int(write.get("errors", 0)) > 0, "main remote write did not fail")
    require(int(fsync.get("errors", 0)) > 0, "FUSE fsync did not fail")


def check_verification(args: argparse.Namespace) -> None:
    verification = load_json(args.verification)
    require_verification(verification, args.expected_acks)
    require(
        verification.get("quick_check") == "ok",
        "authority quick_check is not ok",
    )


def check_pass_authority(args: argparse.Namespace) -> None:
    directory = Path(args.directory)
    prepared = load_json(args.prepare)
    initial_size = int(prepared["actual_bytes"])
    _revision, final_size, _digest = require_stable_authority(
        directory,
        "main",
        "db",
    )
    require(final_size > initial_size, "authority main did not grow")
    wal = load_json(str(directory / "wal-state.json"))
    require(
        wal.get("state") in {"absent", "zero"},
        "authority WAL is not absent or zero",
    )


def check_xfail_authority(args: argparse.Namespace) -> None:
    directory = Path(args.directory)
    template = Path(args.template)
    _main_revision, main_size, main_digest = require_stable_authority(
        directory,
        "main",
        "bin",
    )
    _wal_revision, wal_size, _wal_digest = require_stable_authority(
        directory,
        "wal",
        "bin",
    )
    require(main_size == template.stat().st_size, "main size differs from template")
    require(main_digest == file_sha256(template), "main differs from template")
    require(wal_size > 0, "known-failure authority WAL is empty")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    pass_result = subparsers.add_parser("pass-result")
    pass_result.add_argument("--result", required=True)
    pass_result.set_defaults(handler=check_pass_result)

    pass_perf = subparsers.add_parser("pass-perf")
    pass_perf.add_argument("--perf", required=True)
    pass_perf.add_argument("--candidate-sha", required=True)
    pass_perf.set_defaults(handler=check_pass_perf)

    xfail_result = subparsers.add_parser("xfail-result")
    xfail_result.add_argument("--candidate-sha", required=True)
    xfail_result.add_argument("--return-code", required=True, type=int)
    xfail_result.add_argument("--result", required=True)
    xfail_result.add_argument("--acks", required=True)
    xfail_result.add_argument("--mount-log", required=True)
    xfail_result.set_defaults(handler=check_xfail_result)

    xfail_perf = subparsers.add_parser("xfail-perf")
    xfail_perf.add_argument("--perf", required=True)
    xfail_perf.add_argument("--candidate-sha", required=True)
    xfail_perf.set_defaults(handler=check_xfail_perf)

    verification = subparsers.add_parser("verification")
    verification.add_argument("--verification", required=True)
    verification.add_argument("--expected-acks", required=True, type=int)
    verification.set_defaults(handler=check_verification)

    pass_authority = subparsers.add_parser("pass-authority")
    pass_authority.add_argument("--directory", required=True)
    pass_authority.add_argument("--prepare", required=True)
    pass_authority.set_defaults(handler=check_pass_authority)

    xfail_authority = subparsers.add_parser("xfail-authority")
    xfail_authority.add_argument("--directory", required=True)
    xfail_authority.add_argument("--template", required=True)
    xfail_authority.set_defaults(handler=check_xfail_authority)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        args.handler(args)
    except (AssertionError, KeyError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"FAIL {args.command}: {exc}", file=sys.stderr)
        return 1
    print(f"PASS {args.command}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
