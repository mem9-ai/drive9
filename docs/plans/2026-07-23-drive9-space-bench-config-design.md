---
title: Drive9 Space Benchmark Input Config Design
---

## Objective

Allow `drive9-space-bench` to read reusable server and TiDB Cloud credentials
from an optional, dedicated input file while preserving command-line overrides
and the existing default of 500 spaces.

## Scope baseline

In scope:

1. Default input path `~/.drive9/bench/config.json`.
2. A `--config PATH` override.
3. JSON fields for `server`, `tidbcloud_public_key`,
   `tidbcloud_private_key`, `spaces`, and
   `tidbcloud_spending_limit`.
4. Precedence `CLI flags > non-empty environment variables > config file >
   built-in defaults`.
5. Strict JSON decoding and a regular input file with mode `0600`.
6. A missing default file is optional; a missing explicitly selected file is
   an error.

Out of scope:

1. Adding every workload or output flag to the input file.
2. Combining static inputs with `spaces.json`.
3. Automatically creating, rewriting, or encrypting `config.json`.
4. Changing provisioning, workload, state, or report schemas.

The expected production change is `100-160 LoC` (Medium).

## Configuration flow

The command parses flags first so `--help` remains independent of local
configuration. It then loads the selected JSON file, if present, and resolves
each supported field according to the precedence rule. Explicit flags always
win. Non-empty `DRIVE9_SERVER`, `DRIVE9_PUBLIC_KEY`, and
`DRIVE9_PRIVATE_KEY` values override corresponding file fields. The final
effective configuration goes through the existing validation.

`spaces` defaults to 500 when neither the file nor `--spaces` provides a value.
An explicit `--spaces N` always overrides the file. The spending limit follows
the same behavior and retains its built-in default of 10000.

## Security and errors

The input file is separate from the generated
`~/.drive9/bench/spaces.json`. Before decoding, the command verifies that the
selected path is a regular file with exact mode `0600`. Unknown JSON fields,
multiple JSON documents, malformed values, and insecure permissions fail
before any HTTP request.

AK/SK may exist in `config.json` only because the user explicitly manages that
secret file. The command never copies them into `spaces.json`, reports, or
console output.

## Verification

Tests cover the default path, an explicit path, missing-file behavior,
permissions, strict JSON decoding, all precedence levels, default space count,
and `--spaces` override. Existing report redaction and command tests continue
to guard secret handling and compatibility.
