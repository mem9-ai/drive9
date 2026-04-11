# Proposal: Audio Retrieval MVP for TiDB Auto-Embedding Tenants

**Date**: 2026-04-10  
**Purpose**: Based on the current `drive9` codebase and existing proposals, define a smaller audio MVP: establish the minimal business closure only for TiDB auto-embedding tenants, verify that audio files can enter the existing main path through `audio_extract_text -> files.content_text -> grep/search`, and explicitly exclude higher-complexity topics such as upload completion, long-audio retrieval quality, and multi-mode expansion.

## Summary

The scope of [audio-extract-text-proposal.md](../audio-extract-text-proposal.md) is still too broad. It spans:

- audio durable tasks
- integration with three write paths
- worker/runtime integration
- TiDB auto-embedding search closure
- upload completion type contracts
- long-audio representation and retrieval quality

That would significantly enlarge the implementation surface and bundle multiple independent risk areas into the first delivery.

This proposal narrows the target to a smaller MVP:

1. Support only `tidb_zero` and `tidb_cloud_starter`.
2. Support only the direct-write create / overwrite paths.
3. Add only the `audio_extract_text` durable task, executed by the existing `semantic worker`.
4. Validate only the file-level `content_text -> grep/search` business closure.
5. Do not add upload completion support, do not address long-audio retrieval quality, and do not extend DB9 / app-managed embedding.

The success criterion for this MVP is not "audio retrieval is now complete," nor "the long-audio platform capability is validated." It is: small audio files can now enter the current stable semantic main path, and the code surface stays small, reviewable, and easy to validate.

## Context

### Verified Current State

The repository already has most of the infrastructure needed for this MVP:

1. `files.content_text` is already the stable semantic text entry point for retrieval.
  - The TiDB auto-embedding proposal explicitly defines `content_text` as the canonical semantic source, from which the database derives vectors. See [auto-embedding-mode.md](../auto-embedding-mode.md).
2. The main grep/search path for TiDB auto-embedding already exists.
  - `Grep(...)` in [dat9.go](../../pkg/backend/dat9.go) runs the following in parallel:
    - `FTSSearch(...)`
    - `VectorSearchByText(...)`
    - if both ranked paths return no results, it falls back to `KeywordSearch(...)`
  - For a TiDB auto-embedding backend, the vector path directly uses the database-side `VectorSearchByText(...)` and does not require an app-managed `embed` worker.
3. The direct-write create / overwrite paths already write `files` metadata inside the transaction.
  - `createAndWriteCtx(...)` and `overwriteFileCtx(...)` in [dat9.go](../../pkg/backend/dat9.go) already compute:
    - `content_type`
    - `checksum`
    - `content_text`
  - On the TiDB auto-embedding path, [file_tx.go](../../pkg/datastore/file_tx.go)'s `UpdateFileContentAutoEmbeddingTx(...)` keeps `content_text` as the input to database-side embedding, rather than using the app-managed embedding clear/recompute flow.
4. The `semantic worker` is already the delivery owner for durable semantic tasks.
  - The repo already has:
    - `claim / ack / retry / recover`
    - worker-level lease renewal
    - capability routing converged onto `TaskType` sets
  - That means the audio MVP does not need to invent another async system.

### Current Problem

Although the infrastructure is already in place, audio files still do not enter the current main path:

- there is no `audio_extract_text` task
- there is no audio handler/runtime
- there is no writeback path that turns audio into `content_text`

As a result:

- raw audio bytes can be stored
- but audio content cannot enter `grep/search`

### Why a Smaller MVP Is Needed First

If we continue directly toward the full audio proposal, the first implementation would hit three categories of complexity at once:

1. Upload-path problems
  - the MIME input contract for multipart upload completion is not yet complete
  - persisting upload-init MIME would add extra schema/API surface
2. Long-content problems
  - transcript truncation for long audio directly affects retrieval quality
  - chunk / segment retrieval would introduce a new data model
3. Mode-expansion problems
  - DB9 / app-managed embedding needs separate handling for transcript -> embed bridging

These are all real requirements, but they are not necessary for validating whether audio can enter the current main path.

## Goals

1. Introduce the minimal audio text production closure for TiDB auto-embedding tenants: `audio_extract_text -> content_text -> grep/search`.
2. Validate, via only the direct-write create / overwrite paths, that audio files can be recalled by the existing retrieval main path.
3. Keep the implementation surface as small as possible and avoid bundling upload completion, long-audio representation, and multi-mode support into the first delivery.
4. Reuse the existing `semantic_tasks`, `semantic worker`, revision-gated writeback, and TiDB auto-embedding search path.

## Non-Goals

- No support for multipart upload completion.
- No audio closure for DB9 or app-managed embedding mode.
- No attempt to solve retrieval quality for long audio at the file-level `content_text`.
- No chunk / segment transcript storage or retrieval.
- No new audio-specific search API.
- This proposal does not redesign lease renewal, tasktype-routing, or runtime enablement; they are treated as prerequisite capabilities.

## Support Boundary

This MVP covers only the following support surface:

| Category | Supported |
| --- | --- |
| Tenant provider | `tidb_zero`, `tidb_cloud_starter` |
| Write path | create / overwrite |
| Async task | `audio_extract_text` |
| Search path | file-level `content_text -> grep/search` |
| Task model | `single-task / single-file / single-revision` |
| Runtime size model | small audio within a single-task resource boundary |

Explicitly out of scope:

| Category | Not in MVP |
| --- | --- |
| Upload path | multipart upload completion |
| Retrieval granularity | chunk / segment / timeline |
| Quality target | retrieval of later content in long audio |
| Embedding mode | DB9 / app-managed |

### Audio Formats (MVP Closed Set)

Implementation uses a closed set for **Content-Type and path extension**, kept in sync with `allowedAudioMIME` and `audioExtensionMIME` in `pkg/backend/audio_extract.go`.

**Supported in this phase:**

| Format | Typical MIME | Path fallback, only when `content_type` is empty or too generic |
| --- | --- | --- |
| MP3 | `audio/mpeg` | `.mp3` |
| WAV | `audio/wav`, `audio/x-wav` | `.wav` |

**Not included in this phase, with code comments + TODOs left as breadcrumbs for later recovery:**

- **MP4 audio family / M4A, AAC, FLAC, OGG** and their common aliases, such as `audio/mp4a-latm`, `audio/x-aac`, and `audio/x-flac`: `TODO(post-MVP audio)`.
- **WebM** (`audio/webm`, `video/webm`): `TODO(WebM)` for later decisions such as muxed vs audio-only handling.

For files outside the closed set: **do not enqueue** `audio_extract_text`; if historical tasks still exist, the handler should follow the current conservative terminal behavior such as `not_audio`, with no guarantee of writing `content_text`.

For out-of-bound inputs, this MVP guarantees only conservative behavior:

- it does not guarantee that the revision enters the audio semantic closure
- it does not guarantee that `files.content_text` is eventually written
- it does not treat support for out-of-bound inputs as a delivery target for this phase

## Design

### 1. Establish the Closure Only for TiDB Auto-Embedding Tenants

This MVP targets only:

- `tidb_zero`
- `tidb_cloud_starter`

The reason is not that these providers are intrinsically easier for audio itself, but that they already have the minimal closure:

- `content_text` is the canonical semantic source
- vector derivation is handled by the database
- grep/search already uses `FTSSearch + VectorSearchByText + KeywordSearch`

So this MVP only needs to solve:

- durable audio task registration
- revision-safe `content_text` writeback

It does not need to solve the app-managed `text -> vector` lifecycle at the same time.

### 2. Integrate Only the Direct-Write Create / Overwrite Paths

This MVP intentionally does not touch multipart upload completion.

It integrates only these two paths:

- `createAndWriteCtx(...)`
- `overwriteFileCtx(...)`

Reasons:

1. These two paths already synchronously have:
  - raw bytes
  - `content_type`
  - the current revision
2. These two paths are naturally easier places to ensure:
  - audio type recognition
  - in-transaction durable task registration
  - revision correctness
3. This cleanly avoids the current MIME contract issue in upload completion.

The write-path rule for this MVP is:

- only on a TiDB auto-embedding backend
- and only when the runtime is enabled
- and only when the current revision is recognized as an audio type **inside the MVP closed set** (MP3 / WAV only; see "Audio Formats (MVP Closed Set)" above)

will the create / overwrite transaction enqueue `audio_extract_text`.

### 3. Keep the Task Model Minimal and Explicitly Bounded

This MVP adds only one durable task:

- `semantic.TaskTypeAudioExtractText`

In the MVP, that task is fixed as:

- `single-task`
- `single-file`
- `single-revision`
- bounded operation

The handler target also stays minimal:

1. read the current `files` row and apply the revision gate
2. load raw bytes from DB-inline or S3
3. call the ASR / audio-text extraction runtime
4. sanitize and length-trim the output
5. update `files.content_text` through revision-gated writeback

This phase does not introduce:

- chunking
- multi-segment ASR
- summary fallback
- multi-layer text columns

The point of this boundary is that this phase validates whether one revision can produce retrievable `content_text`, not whether audio already has a richer representation pipeline.

### 4. Reuse the Existing `content_text -> grep/search` Main Path

This MVP adds no new search API.

Once the audio handler successfully writes the transcript into `files.content_text`, a TiDB auto-embedding tenant should automatically enter the existing main path:

```text
audio bytes
  -> audio_extract_text
  -> files.content_text
  -> FTS / VectorSearchByText / KeywordSearch
```

That means business acceptance for the MVP should not be defined as "audio retrieval is already good enough." It should be defined as:

- audio content has become `content_text` consumable by the current retrieval system
- no grep/search API changes are needed
- no second audio-only retrieval subsystem needs to be introduced

### 5. Leave Out-of-Bound Inputs and Long-Audio Quality Explicitly for Follow-Up

This MVP does not solve long-audio quality.

The constraints need to be stated directly:

- transcripts may be truncated
- audio beyond a single-task resource boundary is not guaranteed to enter the closure
- if the implementation leaves task results for out-of-bound inputs, they should remain limited terminal states rather than implicitly promising a `content_text` write
- file-level `content_text` only guarantees "entry into the main path," not "full coverage of all content with ideal retrieval quality"

This is an intentionally accepted product boundary, not an omission. Otherwise the MVP would expand again into:

- chunk retrieval
- overview extraction
- multi-layer text representation
- long-audio retrieval quality optimization

### 6. Keep Runtime and Durable Delivery Responsibilities Minimal

This MVP reuses the current durable task delivery system, but it must not copy a new local async audio path just for convenience.

In this phase, the backend is responsible only for:

- holding audio runtime dependencies
- exposing the audio task handling entry point
- conditionally enqueuing durable tasks inside direct-write transactions

In this phase, the `semantic worker` continues to exclusively own:

- claim / ack / retry / recover
- task delivery
- lease ownership
- capability routing converged onto `TaskType` sets

Therefore this MVP explicitly does not allow:

- a new backend-owned audio queue
- backend-owned background goroutines for audio
- an in-memory `enqueueAudioExtract(...)` API on the backend
- copying another task-specific delivery mode just for audio

Worker-level lease renewal, task-type routing convergence, and runtime enablement semantics remain prerequisite capabilities and are not redefined by this proposal.

## Compatibility and Invariants

This MVP must preserve the following invariants:

1. It applies only to TiDB auto-embedding backends.
2. It applies only to create / overwrite; multipart upload completion behavior remains unchanged.
3. It enqueues `audio_extract_text` only when the runtime is enabled and the type is inside the **MVP audio closed set (MP3 / WAV)**.
4. Handler writeback still uses the revision gate as the final correctness boundary.
5. Search entry points remain unchanged; audio only becomes one more producer of `content_text`.
6. The backend does not own a local async delivery path dedicated to audio; durable delivery remains solely the responsibility of the `semantic worker`.

## Rollout Plan

- Phase A: establish the minimal task and runtime capability
  - add `TaskTypeAudioExtractText`
  - add the minimal audio runtime config and handler entry point
- Phase B: integrate only create / overwrite
  - conditionally enqueue `audio_extract_text` in TiDB auto-embedding direct-write paths
  - update `content_text` through revision-gated writeback
- Phase C: validate the main-path closure
  - verify that grep/search can recall audio transcripts
  - verify that overwrite cannot be dirtied by an older revision

## Validation Strategy

- **Write-path validation**
  - TiDB auto-embedding create leaves `audio_extract_text` when it sees an **in-closure** audio type, MP3 / WAV
  - TiDB auto-embedding overwrite enqueues a new task for the new revision when it sees an in-closure audio type
  - out-of-closure formats such as `.m4a` do not enqueue audio tasks
  - when `Enabled=false`, no task is enqueued
- **Correctness validation**
  - an older revision's audio task must not overwrite the newer revision's `content_text`
  - non-audio files must not be mistakenly enqueued as `audio_extract_text`
  - once registered, tasks must follow the existing retry / backoff behavior on byte-loading failure, ASR failure, and writeback failure
- **Search validation**
  - after transcript writeback, `grep` can recall it through FTS
  - in TiDB auto-embedding mode, audio files can enter `VectorSearchByText(...)` and participate in the ranked path
  - when the ranked path has no result, keyword fallback remains in place
- **Scope-guard validation**
  - multipart upload completion behavior remains unchanged
  - DB9 / app-managed embedding behavior remains unchanged
  - the business closure can be validated without adding a new search API
  - no backend-owned local audio queue / goroutine / enqueue path is needed
  - no richer representation such as chunk / segment / timeline / summary is introduced

## Risks and Mitigations

1. **The MVP looks too narrow because it only covers TiDB auto-embedding.**  
  Mitigation: this is an intentional scope reduction to validate whether audio can enter the current main path; DB9 / app-managed bridging should be a separate follow-up proposal.
2. **Without upload completion support, large uploaded audio still cannot enter the closure.**  
  Mitigation: this is a deliberate MVP boundary. The first step is to validate the direct-write path and the core retrieval chain; upload completion can be added in the next phase.
3. **Long-audio retrieval quality may be insufficient, blurring "searchable" and "good retrieval quality."**  
  Mitigation: the MVP only promises entry into the main path, not long-audio quality; those quality concerns should be deferred to chunk retrieval / long-content representation design.
4. **The audio handler may still incur meaningful external cost.**  
  Mitigation: keep relying on the existing or already-defined boundaries such as runtime enablement, lease renewal, max-bytes limits, and output truncation, rather than expanding longer pipelines in the MVP.

## Open Questions

At this stage, there are no open design questions blocking this MVP.

Items not included in the MVP are already intentionally converted into follow-up work:

- multipart upload completion
- DB9 / app-managed embedding
- long-audio quality and chunk retrieval
- expanding the audio-format closed set, such as MP4/M4A, AAC, FLAC, OGG, and WebM
