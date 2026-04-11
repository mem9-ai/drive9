# Proposal: Audio Extract Phase 3 Upload Completion Integration

**Date**: 2026-04-11  
**Purpose**: Based on the minimal TiDB auto-embedding audio closure already established in `Phase 2`, bring `multipart upload completion` into the same `audio_extract_text -> files.content_text -> grep/search` main path while keeping the current MVP MP3/WAV closed set and conservative type-recognition contract unchanged, without adding upload-init MIME persistence, format expansion, or long-audio quality work.

## Summary

The repository has already implemented the core `Phase 2` closure:

- `create / overwrite` conditionally enqueue `audio_extract_text` on the TiDB auto-embedding path
- the `semantic worker` can already claim / dispatch / ack / retry `audio_extract_text`
- `ProcessAudioExtractTask(...)` can already write transcripts back to `files.content_text`
- audio transcripts can already enter the existing `grep/search` main path

But `multipart upload completion` is still a clear gap:

- the finalize-upload path only enqueues durable tasks for images today
- audio still does not enter the closure on upload completion
- existing tests also lock that behavior in as "do not enqueue audio tasks"

So the smallest and highest-value next step is not format expansion, retrieval-quality work, or DB9 support. It is to extend the audio closure that already works for `create / overwrite` to `upload completion`.

The scope of this proposal stays deliberately conservative:

1. Support only TiDB auto-embedding tenants.
2. Extend only the `multipart upload completion` write path.
3. Keep the current MP3/WAV closed set.
4. Keep the current conservative recognition contract: completion continues to guarantee extension-only recognition.
5. Do not add upload-init MIME persistence, do not expand audio formats, and do not address long-audio retrieval quality.

## Context

### Capabilities Already in Place

The current codebase already contains most of the infrastructure required for `Phase 3`:

1. The `audio_extract_text` durable task already exists.
   - [task.go](../../pkg/semantic/task.go) already defines `semantic.TaskTypeAudioExtractText` and `semantic.AudioExtractTaskPayload`.

2. TiDB auto-embedding `create / overwrite` already enqueue audio tasks.
   - [dat9.go](../../pkg/backend/dat9.go) calls `enqueueTiDBAutoSemanticTasksTx(...)` from [semantic_tasks.go](../../pkg/backend/semantic_tasks.go) inside direct-write transactions.
   - Whether `audio_extract_text` is enqueued is gated by `shouldEnqueueAudioExtractTask(...)`.

3. The audio handler and worker dispatch are already implemented.
   - [audio_extract.go](../../pkg/backend/audio_extract.go) already implements `ProcessAudioExtractTask(...)`.
   - [semantic_worker.go](../../pkg/server/semantic_worker.go) already handles `TaskTypeAudioExtractText` in `dispatchTask()`.

4. The current MVP closed set is already fixed.
   - In [audio_extract.go](../../pkg/backend/audio_extract.go), `allowedAudioMIME` and `audioExtensionMIME` currently include only MP3/WAV.

### Current Gap

`upload completion` is still not part of the audio closure.

In the finalize-upload transaction in [upload.go](../../pkg/backend/upload.go):

- the TiDB auto-embedding path calls `enqueueImgExtractTaskTx(...)` for images
- but it does not call `enqueueAudioExtractTaskTx(...)` for audio

Current tests also lock this gap in explicitly:

- `TestConfirmUploadAutoEmbeddingDoesNotEnqueueAudioExtractTask` in [semantic_tasks_test.go](../../pkg/backend/semantic_tasks_test.go)

That means the current audio correctness story is still split across two semantics:

- `create / overwrite`: recognized audio revisions leave durable tasks
- `upload completion`: even when the extension is clearly `.mp3` or `.wav`, the revision still does not enter the audio closure

### Current Input Reality of the Completion Path

This phase must be designed against the current code reality, not against a new ingress contract.

The finalize-upload path currently has only these type inputs:

- `upload.TargetPath`
- the result of `detectContentType(upload.TargetPath, nil)`

In [upload.go](../../pkg/backend/upload.go), the current call is:

```go
contentType := detectContentType(upload.TargetPath, nil)
```

That means when `data == nil`, the completion path is effectively stable only at extension-based recognition. This phase still does not have:

- upload-init MIME / `content_type` persistence
- object-body re-read and sniffing at finalize time
- any extra media-detection workflow during completion

So `Phase 3` must stay converged on:

- bringing completion into the audio closure
- while keeping the recognition contract at an extension-level guarantee

## Goals

1. Bring `multipart upload completion` into the TiDB auto-embedding audio durable-task registration closure.
2. For completion revisions whose extensions are recognizable as MP3/WAV, guarantee that task registration and revision visibility commit in the same transaction.
3. Reuse the existing `audio_extract_text` task, handler, worker routing, and writeback logic, without introducing a second audio-processing path.
4. Keep the current `Phase 2` support boundary unchanged and avoid opportunistic expansion of formats, tenant modes, or long-audio behavior.

## Non-Goals

- Do not add upload-init MIME / `content_type` persistence.
- Do not change the completion contract from "extension-only recognition".
- Do not expand beyond MP3/WAV audio formats.
- Do not support audio closure for DB9 / app-managed embedding.
- Do not address long-audio retrieval quality or chunk / segment / timeline representations.
- Do not do repo-wide worker cleanup or media runtime/delivery refactors.

## Support Boundary

This phase adds support only for the following surface:

| Category | Supported |
| --- | --- |
| Tenant provider | `tidb_zero`, `tidb_cloud_starter` |
| Write path | `multipart upload completion` |
| Audio format closed set | MP3 / WAV |
| Type recognition | completion guarantees extension-only recognition |
| Async task | `audio_extract_text` |

Explicitly out of scope:

| Category | Not in Phase 3 |
| --- | --- |
| MIME source | upload-init MIME persistence |
| Format expansion | M4A / AAC / FLAC / OGG / WebM |
| Embedding mode | DB9 / app-managed |
| Retrieval quality | recall of later content in long audio |
| Retrieval model | chunk / segment / timeline |

## Design

### 1. Extend Completion Only, Not Rebuild Phase 2

This phase does not redefine the audio MVP. It extends the `Phase 2` closure to the last missing write path.

The already-landed pieces continue to be reused:

- `TaskTypeAudioExtractText`
- `AudioExtractTaskPayload`
- `ProcessAudioExtractTask(...)`
- audio dispatch in the `semantic worker`
- revision-gated `content_text` writeback

The only new behavior in this phase is:

- the TiDB auto-embedding finalize-upload transaction will also enqueue `audio_extract_text` when the current revision is recognized as a supported audio type

### 2. Completion Keeps the Conservative Recognition Contract

This phase does not change the type-input reality of completion.

For `multipart upload completion`, the support contract remains:

- use only the inputs that are already stably available inside the completion transaction
- do not restate the priority as "`content_type` first, extension fallback"
- in practical terms, `Phase 3` guarantees only extension-based recognition

Under current implementation conditions, this means:

- `.mp3` and `.wav` completion revisions can enter the audio closure
- extensionless paths, unknown extensions, or extensions outside the closed set are not guaranteed to enter the audio closure

This contract must stay explicit and conservative so the document does not promise behavior stronger than the code can actually provide.

### 3. Task Registration Rules Inside the Completion Transaction

On a TiDB auto-embedding backend, the finalize-upload transaction should use the same class of gates as `create / overwrite`:

- runtime enabled
- the current revision is recognized as a supported audio type
- and that type is inside the current closed set (MP3/WAV)

Only when all those conditions hold should the completion transaction enqueue `audio_extract_text`.

That rule must cover both:

- create-new completion
- overwrite completion

And it must preserve the same in-transaction enqueue semantics already used for images:

- revision is visible
- the durable task already exists

Those two facts must commit together.

### 4. Keep the Code Shape Minimal

This phase should not duplicate another layer of task-recognition logic inside the upload path.

The recommended landing shape is:

- in the TiDB auto-embedding branch of [upload.go](../../pkg/backend/upload.go), reuse the existing audio enqueue helper
- keep the contract aligned with the same judgment already used by [semantic_tasks.go](../../pkg/backend/semantic_tasks.go) for `create / overwrite`

That avoids:

- maintaining another audio-format decision tree inside the upload path
- drift between upload and direct-write behavior for the closed set
- multi-site patching when formats expand later

### 5. Extend Correctness Only to Recognized Completion Revisions

The correctness semantics in this phase must remain conservative.

The system promises in-transaction task registration only for completion revisions that are:

- recognized as supported audio on the current path
- inside the current closed set
- with runtime enabled

The system does not promise closure for:

- completion revisions whose type cannot be confirmed from the extension
- formats outside the closed set
- runtime-disabled cases

This is not a defect. It is an intentionally narrow contract for this phase.

### 6. Tests and Documentation Must Flip Together

Once this phase lands, the current "completion does not enqueue audio" behavior must be flipped from an explicit negative contract into a new positive contract.

That means:

- the current `TestConfirmUploadAutoEmbeddingDoesNotEnqueueAudioExtractTask` no longer represents the target behavior
- it should be replaced by positive validation that:
  - `.mp3` completion enqueues
  - `.wav` completion enqueues
  - overwrite completion enqueues for the new revision
  - `.m4a` and other closed-set-external formats still do not enqueue
  - `Enabled=false` still does not enqueue

## Compatibility and Invariants

This phase must preserve the following invariants:

1. It applies only to TiDB auto-embedding backends.
2. It extends only `multipart upload completion`; it does not change the existing `create / overwrite` semantics.
3. The completion path continues to guarantee extension-only recognition and does not gain new MIME sources.
4. Completion enqueues `audio_extract_text` only when runtime is enabled and the extension matches the MP3/WAV closed set.
5. Handler writeback still uses the revision gate as the final correctness boundary.
6. The backend does not gain a local audio-specific queue / goroutine / enqueue path; durable delivery remains solely the responsibility of the `semantic worker`.

## Rollout Plan

- Step A: Converge the completion support contract
  - make explicit that this phase adds only MP3/WAV
  - make explicit that completion continues to guarantee extension-only recognition

- Step B: Integrate audio enqueue into the finalize-upload transaction
  - enqueue `audio_extract_text` for create-new completion when the closed set matches
  - enqueue `audio_extract_text` for overwrite completion on the new revision when the closed set matches

- Step C: Flip tests and validation
  - cover completion enqueue with positive tests
  - keep negative tests for closed-set-external formats and runtime-disabled cases

## Validation Strategy

- **Write-path validation**
  - `.mp3` completion enqueues `audio_extract_text` for the final visible revision
  - `.wav` completion enqueues `audio_extract_text` for the final visible revision
  - overwrite completion enqueues an audio task for the new revision
  - `.m4a` and other closed-set-external formats do not enqueue audio tasks
  - when `Enabled=false`, completion does not enqueue audio tasks

- **Correctness validation**
  - task registration and revision visibility commit in the same transaction on the completion path
  - overwrite completion does not let an older revision transcript dirty-write a newer revision
  - audio tasks that enter the worker after completion still follow the existing ack / retry / recover semantics

- **Scope-guard validation**
  - `create / overwrite` behavior remains unchanged
  - no upload-init MIME persistence is added
  - the MP3/WAV closed set is not expanded
  - no new search API or richer representation is introduced

## Risks and Mitigations

1. **Completion work could opportunistically widen media recognition.**  
   Mitigation: lock this phase to MP3/WAV plus extension-only recognition, without introducing a stronger MIME contract.

2. **The upload path could duplicate another audio-decision tree and drift from `create / overwrite`.**  
   Mitigation: reuse the existing audio enqueue helper and closed-set judgment as much as possible.

3. **Completion integration could turn into another platform-refactor entry point.**  
   Mitigation: limit this phase to in-transaction enqueue inside finalize upload; do not absorb worker cleanup, runtime refactors, or long-audio work.

4. **Users may misread completion support as "audio is now fully supported."**  
   Mitigation: keep the document explicit that this only connects the last missing write path; it does not imply format expansion, long-audio quality, or app-managed mode support.

## Open Questions

There are no blocking open questions for this phase.

## Follow-up

- [audio-extract-text-proposal.md](../audio-extract-text-proposal.md)
  - Continue tracking format expansion, stronger media recognition, long-audio quality, worker cleanup, and app-managed bridging.
