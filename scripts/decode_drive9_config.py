#!/usr/bin/env python3
"""Decode tenant_id from ~/.drive9/config without revealing credentials.

This script is intended for drive9 CLI versions where ``drive9 ctx`` does not
yet display ``tenant_id``. When your ``drive9 ctx show`` or ``drive9 ctx ls``
output already includes ``tenant_id``, prefer those commands instead of this
script.

Owner and fs_scoped contexts store a drive9 API key shaped as:

    dat9_<base64url(jwt)>

The JWT payload contains tenant_id. This script unwraps that local encoding and
decodes the JWT payload. Delegated contexts store a JWT directly; the script
also decodes that payload when present. It does not verify signatures, TTL, or
revocation, so use the output only for local inspection/debugging.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_PATH = Path.home() / ".drive9" / "config"
DRIVE9_API_KEY_PREFIX = "dat9_"


def b64url_decode(raw: str) -> bytes:
    raw = raw.strip()
    padding = "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode((raw + padding).encode("ascii"))


def decode_jwt_payload(raw_jwt: str) -> dict[str, Any]:
    parts = raw_jwt.strip().split(".")
    if len(parts) != 3:
        raise ValueError(f"expected 3 JWT segments, got {len(parts)}")
    payload = b64url_decode(parts[1])
    return json.loads(payload.decode("utf-8"))


def unwrap_drive9_api_key(api_key: str) -> str:
    api_key = api_key.strip()
    if not api_key.startswith(DRIVE9_API_KEY_PREFIX):
        raise ValueError(f"expected {DRIVE9_API_KEY_PREFIX!r} API key prefix")
    wrapped = api_key.removeprefix(DRIVE9_API_KEY_PREFIX)
    return b64url_decode(wrapped).decode("utf-8")


def decode_context(name: str, ctx: dict[str, Any], current: bool, server: str) -> dict[str, Any]:
    ctx_type = ctx.get("type") or "owner"
    result: dict[str, Any] = {
        "name": name,
        "current": current,
        "type": ctx_type,
        "server": ctx.get("server") or server,
        "tenant_id": None,
        "token_version": None,
        "iat": None,
        "exp": None,
        "grant_id": ctx.get("grant_id") or None,
        "agent": ctx.get("agent") or None,
        "decode_error": None,
    }

    try:
        if ctx_type in ("owner", "fs_scoped"):
            api_key = ctx.get("api_key") or ""
            raw_jwt = unwrap_drive9_api_key(api_key)
            claims = decode_jwt_payload(raw_jwt)
        elif ctx_type == "delegated":
            token = ctx.get("token") or ""
            claims = decode_jwt_payload(token)
        else:
            raise ValueError(f"unsupported context type {ctx_type!r}")
    except Exception as exc:  # Keep script useful across partially migrated configs.
        result["decode_error"] = str(exc)
        return result

    result["tenant_id"] = claims.get("tenant_id")
    result["token_version"] = claims.get("token_version")
    result["iat"] = claims.get("iat")
    result["exp"] = claims.get("exp")
    result["grant_id"] = result["grant_id"] or claims.get("grant_id")
    result["agent"] = result["agent"] or claims.get("agent")
    return result


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def print_text(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        marker = "*" if row["current"] else " "
        print(f"{marker} context: {row['name']}")
        print(f"  type:      {row['type']}")
        print(f"  server:    {row['server']}")
        print(f"  tenant_id: {row['tenant_id'] or '-'}")
        if row["token_version"] is not None:
            print(f"  version:   {row['token_version']}")
        if row["iat"] is not None:
            print(f"  iat:       {row['iat']}")
        if row["exp"] is not None:
            print(f"  exp:       {row['exp']}")
        if row["grant_id"]:
            print(f"  grant_id:  {row['grant_id']}")
        if row["agent"]:
            print(f"  agent:     {row['agent']}")
        if row["decode_error"]:
            print(f"  error:     {row['decode_error']}")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Decode tenant_id from drive9 CLI config contexts."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"config path (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--context",
        help="context name to decode (default: current context)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="decode all contexts instead of only the current context",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print machine-readable JSON",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    contexts = cfg.get("contexts") or {}
    current = cfg.get("current_context") or ""
    server = cfg.get("server") or "https://api.drive9.ai"

    if args.all:
        names = sorted(contexts)
    else:
        name = args.context or current
        if not name:
            raise SystemExit("no current context; pass --context <name> or --all")
        if name not in contexts:
            raise SystemExit(f"context {name!r} not found in {args.config}")
        names = [name]

    rows = [
        decode_context(name, contexts[name] or {}, name == current, server)
        for name in names
    ]

    if args.json:
        json.dump(rows[0] if len(rows) == 1 else rows, sys.stdout, indent=2)
        print()
    else:
        print_text(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
