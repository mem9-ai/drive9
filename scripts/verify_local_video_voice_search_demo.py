#!/usr/bin/env python3

"""Verify a minimal Drive9 video voice semantic-search demo.

This is intentionally a Drive9-native smoke instead of a separate video-search
stack. It uploads a video/audio container that Drive9 can route through the
existing durable `audio_extract_text` path, waits for `files.content_text`, and
optionally verifies search via the existing grep endpoint.

Modes:

- `stub` (default): use a tiny synthetic `.mp4` payload against
  `DRIVE9_AUDIO_EXTRACT_MODE=stub`; asserts the deterministic stub transcript
  and grep for `transcript`.
- `provider`: upload a real media file (`.mp4`, `.m4a`, `.mp3`, `.wav`) against a
  server configured with an ASR provider such as `openai` or `qwen-asr`; asserts
  non-empty transcript text and verifies grep only when `--query` is supplied.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Sequence

from verify_local_audio_extract import (
    DEFAULT_BASE_URL,
    Verifier,
    VerificationResult,
    expected_stub_transcript,
)


DEFAULT_REMOTE_PREFIX = "/video-voice-demo/"
SUPPORTED_MEDIA_EXTENSIONS = frozenset((".mp4", ".m4a", ".mp3", ".wav"))

# Enough MP4 container-looking bytes for a local smoke. The stub extractor does
# not decode media; Drive9 routes this by path/container type and verifies the
# durable task/search plumbing.
STUB_MP4_PAYLOAD = (
    b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"
    + b"\x00" * 256
)


def normalize_remote_prefix(prefix: str) -> str:
    prefix = prefix.strip()
    if not prefix:
        raise ValueError("remote prefix must not be empty")
    if not prefix.startswith("/"):
        prefix = "/" + prefix
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    return prefix


def supported_media_extension(path: str) -> str:
    ext = Path(path).suffix.lower()
    if ext not in SUPPORTED_MEDIA_EXTENSIONS:
        allowed = ", ".join(sorted(SUPPORTED_MEDIA_EXTENSIONS))
        raise ValueError(f"unsupported media extension {ext or '<none>'}; allowed: {allowed}")
    return ext


def make_demo_path(prefix: str, ext: str) -> str:
    if not ext.startswith("."):
        ext = "." + ext
    if ext not in SUPPORTED_MEDIA_EXTENSIONS:
        allowed = ", ".join(sorted(SUPPORTED_MEDIA_EXTENSIONS))
        raise ValueError(f"unsupported media extension {ext}; allowed: {allowed}")
    suffix = uuid.uuid4().hex[:10]
    return f"{normalize_remote_prefix(prefix)}voice-{suffix}{ext}"


def load_provider_media(path: str) -> tuple[bytes, str]:
    ext = supported_media_extension(path)
    p = Path(path).expanduser()
    if not p.is_file():
        raise RuntimeError(f"media file not found or not a file: {path}")
    data = p.read_bytes()
    if not data:
        raise RuntimeError(f"media file is empty: {path}")
    return data, ext


def result_payload(result: VerificationResult) -> dict[str, Any]:
    return {
        "flow": result.flow,
        "path": result.path,
        "file_id": result.file_id,
        "revision": result.revision,
        "task_id": result.task_id,
        "task_type": result.task_type,
        "status": result.status,
        "attempt_count": result.attempt_count,
        "content_text": result.content_text,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("stub", "provider"),
        default="stub",
        help="stub: synthetic MP4 + deterministic transcript; provider: real media + ASR runtime",
    )
    parser.add_argument(
        "--video-file",
        metavar="PATH",
        help="real media file for --mode=provider; supported extensions: .mp4, .m4a, .mp3, .wav",
    )
    parser.add_argument(
        "--query",
        help="spoken phrase to verify with grep. In stub mode, defaults to 'transcript'.",
    )
    parser.add_argument(
        "--remote-prefix",
        default=DEFAULT_REMOTE_PREFIX,
        help=f"Drive9 directory used for uploaded demo objects (default: {DEFAULT_REMOTE_PREFIX})",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="drive9-server-local base URL",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=90.0,
        help="wait timeout for upload/task/search verification",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="poll interval while waiting for the semantic worker",
    )

    args = parser.parse_args(argv)
    if args.mode == "provider" and not args.video_file:
        parser.error("--video-file is required when --mode=provider")
    if args.mode == "stub" and args.video_file:
        parser.error("--video-file is only valid when --mode=provider")
    return args


def run_demo(args: argparse.Namespace) -> dict[str, Any]:
    remote_prefix = normalize_remote_prefix(args.remote_prefix)
    verifier = Verifier(args.base_url, args.timeout_seconds, args.poll_interval_seconds)

    if args.mode == "provider":
        payload, ext = load_provider_media(args.video_file)
    else:
        payload, ext = STUB_MP4_PAYLOAD, ".mp4"

    remote_path = make_demo_path(remote_prefix, ext)
    result = verifier.verify_direct_put_bytes(remote_path, payload)

    if args.mode == "stub":
        want = expected_stub_transcript(remote_path)
        if result.content_text != want:
            raise RuntimeError(
                f"stub content_text={result.content_text!r}, want {want!r}"
            )
    elif not (result.content_text or "").strip():
        raise RuntimeError("provider mode produced empty content_text")

    grep_query = args.query
    if args.mode == "stub" and not grep_query:
        grep_query = "transcript"
    if grep_query:
        verifier.verify_grep_under_prefix(remote_prefix, grep_query, remote_path)

    return {
        "ok": True,
        "mode": args.mode,
        "remote_prefix": remote_prefix,
        "grep_checked": bool(grep_query),
        "grep_query": grep_query or "",
        "result": result_payload(result),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    print(json.dumps(run_demo(args), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
