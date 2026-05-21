---
title: Drive9 Agent Artifact Archive Proposal
---

## Summary

Create a versioned drive9 archive space for agent consumption. GitHub remains the canonical source of truth for history, pull requests, review, and branch workflow. drive9 becomes an agent-optimized mirror of merged `main` artifacts: exact source snapshots, built binaries, checksums, and machine-readable metadata.

The workflow should run after commits land on `main` and publish immutable commit-scoped artifacts.

## Motivation

Agents often need a reliable way to recover a known-good repository state or fetch a matching binary without rebuilding. A drive9 archive gives agents a stable filesystem-like retrieval surface that can be used across sessions, machines, and sandboxes where a full build or GitHub checkout may be slower or less convenient.

This is not intended to replace git. It is a distribution and recovery layer for agents.

## Goals

1. Store source snapshots for every merged `main` commit.
2. Store built drive9 binaries for the same commit.
3. Store checksums and manifest metadata so agents can verify what they fetched.
4. Provide immutable commit paths for reproducibility.
5. Keep the v1 archive usable through ordinary `drive9 fs cp`, `cat`, and `ls` workflows for artifacts and metadata.
6. Leave file-level source search through `drive9 fs find` and `grep` as a post-v1 extension that requires uploading an expanded source tree.

## Non-Goals

1. Do not make drive9 the canonical VCS.
2. Do not replace GitHub pull request, branch, diff, blame, or review workflows.
3. Do not create a new drive9 space for every merge.
4. Do not require agents to rebuild binaries before using drive9.
5. Do not expose release credentials in logs, artifacts, or checked-in files.

## Archive Model

Use one long-lived drive9 archive space dedicated to this repository. Provision it once, then store its owner API key and server URL in GitHub Actions secrets.

Recommended root:

```text
/drive9/
```

Recommended v1 commit layout:

```text
/drive9/commits/<sha>/manifest.json
/drive9/commits/<sha>/source.tar.gz
/drive9/commits/<sha>/bin/drive9-linux-amd64
/drive9/commits/<sha>/bin/drive9-linux-arm64
/drive9/commits/<sha>/bin/drive9-darwin-amd64
/drive9/commits/<sha>/bin/drive9-darwin-arm64
/drive9/commits/<sha>/bin/drive9-windows-amd64.exe
/drive9/commits/<sha>/bin/drive9-windows-arm64.exe
/drive9/commits/<sha>/checksums.txt
```

Commit paths are immutable after their `manifest.json` is published.

Future optional paths:

```text
/drive9/commits/<sha>/tree/...
/drive9/commits/<sha>/server/...
```

The expanded `tree/` path is post-v1 because current `drive9 fs cp` uploads one local file at a time. Supporting `tree/` should use an explicit file-walking upload script that creates parent directories and uploads files individually.

## Manifest Contract

Each commit should publish a `manifest.json` with enough metadata for agents to decide whether an artifact is usable. The commit manifest is the completion marker for an immutable commit archive and must be written after all referenced artifacts and checksums are present.

Suggested fields:

```json
{
  "schema_version": 1,
  "repository": "mem9-ai/drive9",
  "branch": "main",
  "commit_sha": "<full sha>",
  "short_sha": "<7 chars>",
  "published_at": "<UTC RFC3339>",
  "source_archive": "source.tar.gz",
  "checksums": "checksums.txt",
  "binaries": [
    {
      "path": "bin/drive9-linux-amd64",
      "goos": "linux",
      "goarch": "amd64"
    }
  ]
}
```

## Workflow Shape

Trigger on push to `main`. That represents the final merged commit regardless of merge, squash, or rebase strategy.

High-level workflow:

1. Check out the merged commit.
2. Set up Go using `go.mod`.
3. Install the official drive9 publisher CLI before building the current commit. The newly built CLI is archived as data, not used as the publisher for that same commit.
4. Build release CLI binaries with `make build-cli-release`.
5. Create `source.tar.gz` from the checked-out tree, excluding `.git`, transient build outputs, and local caches.
6. Generate `checksums.txt` and the commit `manifest.json`.
7. Upload source archive, binaries, and checksums to the immutable commit path.
8. Upload the commit `manifest.json` last. This marks the commit path complete.

Server binaries and expanded source `tree/` uploads are out of v1 scope.

## Idempotence and Recovery

The publisher must be safe to rerun for the same commit.

1. If `/commits/<sha>/manifest.json` exists and all referenced checksums match, the commit publish step is complete and should be skipped.
2. If some artifacts exist but the commit manifest is missing, the publisher may repair the commit path by re-uploading missing artifacts and then writing the manifest.
3. If the commit manifest exists but any referenced artifact is missing or has a checksum mismatch, the workflow must fail rather than silently overwrite immutable data.
4. A manual `workflow_dispatch` input `branch` should allow publishing the tip of a specific branch.
5. A scheduled reconciliation job should periodically check recent `main` commits and publish any commit that lacks a complete commit manifest.

## Retrieval Examples

Fetch a specific source archive:

```bash
drive9 fs cp :/drive9/commits/<sha>/source.tar.gz .
```

Fetch a matching CLI binary:

```bash
drive9 fs cp :/drive9/commits/<sha>/bin/drive9-linux-amd64 ./drive9
chmod +x ./drive9
```

## Agent Value

1. Exact restore: agents can fetch the source for a known commit without relying on local cache state.
2. Build avoidance: agents can fetch a known-good binary instead of rebuilding.
3. Cross-session continuity: agents can recover artifacts from drive9 even when the local workspace is gone.
4. Filesystem semantics: agents can list, copy, and inspect v1 artifacts through drive9.
5. Reproducibility: commit-scoped paths plus checksums make artifact identity explicit.

## Risks and Mitigations

1. Bootstrap circularity: a fresh agent may need the drive9 binary to fetch the drive9 binary.
   Mitigation: keep GitHub releases or another bootstrap path available, and store direct HTTP download support if needed later.
2. Credential exposure in CI:
   Mitigation: use GitHub Actions secrets for `DRIVE9_SERVER` and `DRIVE9_API_KEY`; never echo tokens; avoid writing credentials into artifacts.
3. Archive growth:
   Mitigation: start with immutable commit archives, then add retention rules only if storage pressure becomes real.
4. Partial publish:
   Mitigation: publish commit artifacts first, write the commit `manifest.json` last, and allow scheduled reconciliation to repair missing commit manifests.
5. Confusing source of truth:
   Mitigation: document that GitHub remains canonical and drive9 is an artifact mirror.
6. Publisher regression:
   Mitigation: publish with the official drive9 CLI download, not the newly built candidate binary from the commit being archived.

## Open Decisions

1. Whether old commit archives should be retained forever or pruned after a policy is defined.
2. Which stable publisher binary source to pin for the first implementation.
3. Whether a later version should add expanded `tree/` uploads for source-level `grep` and `find`.
4. Whether a later version should archive server binaries in addition to CLI binaries.

## Recommended First Version

Start small:

1. One pre-provisioned archive space.
2. GitHub Actions secrets: `DRIVE9_SERVER`, `DRIVE9_API_KEY`.
3. Triggers: `push` to `main`, `workflow_dispatch` with `branch`, and scheduled reconciliation for recent `main` commits.
4. Artifacts: `source.tar.gz`, CLI release binaries, `checksums.txt`, `manifest.json`.
5. Paths: immutable `commits/<sha>/`.
6. Publisher: official drive9 CLI download.
7. Recovery: idempotent reruns and missing-manifest repair.

This should fit in roughly `80-150 LoC` of workflow YAML and shell if it reuses the existing build targets and drive9 CLI upload path while adding manifest generation, checksum validation, idempotent reruns, and scheduled reconciliation.
