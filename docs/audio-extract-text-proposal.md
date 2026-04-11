# Proposal: drive9 Audio File Storage and Retrieval Design

**Date**: 2026-04-09  
**Purpose**: Based on the current `drive9` codebase and existing design documents, propose a production-oriented audio file storage and retrieval design. The design reuses the existing `raw bytes -> content_text -> embedding/search` main path, brings "audio to searchable text" into the durable `semantic_tasks` infrastructure, and prioritizes support for TiDB auto-embedding tenants.

## Summary

`drive9` should not introduce a separate "audio search system" parallel to the existing semantic retrieval stack. The smaller and safer design is:

- keep using the current file storage model, with raw audio bytes still stored in `files` plus `content_blob` / S3 object storage
- converge the searchable representation of audio into `files.content_text`
- add a durable `audio_extract_text` task type so audio transcription or semantic extraction is handled by the existing `semantic_tasks` and `semantic worker`
- in TiDB auto-embedding mode, let the database continue deriving `embedding` from `content_text`
- keep DB9 and app-managed embedding modes out of the first-phase closure

This design keeps the current search APIs and high-level search behavior unchanged. The only change is that the source of `content_text` expands from "direct text write / image extraction" to "audio extraction."

## Context

### Capabilities the Current System Already Has

The current codebase already has a stable semantic main line:

- raw file contents are managed through `files.storage_type`, `files.storage_ref`, and `files.content_blob`
- searchable semantic text is uniformly written to `files.content_text`
- search uniformly runs FTS, keyword fallback, and vector ranking over `content_text`
- durable async work uniformly uses `semantic_tasks`
- `semantic worker` already supports claim / ack / retry / recover by `task_type`

Verified current behavior includes:

- small-file create / overwrite already try synchronous text extraction and write the result into `content_text`, see `pkg/backend/dat9.go`
- large-file upload completion already updates `files` rows and inode bindings, see `pkg/backend/upload.go`
- async image extraction already has a complete handler structure: read current file, apply revision gate, load raw bytes, extract text, sanitize, and write back to `content_text` with a revision-gated update, see `pkg/backend/image_extract.go`
- TiDB auto-embedding mode already defines `content_text` as the canonical semantic source and lets the database generate `embedding` from it automatically, see `docs/auto-embedding-mode.md` and `pkg/tenant/schema/tidb_auto.go`
- the image durable-task design already established an important pattern: async text-production tasks must be registered in the same transaction that makes the file revision visible, see `docs/img-extract-text-proposal.md`

### What the Current System Is Missing for Audio

The repository still does not have an audio-specific text production path:

- `extractText()` supports only synchronous text fast paths such as `text/*`, `json`, `xml`, and `yaml`, but not audio, see `pkg/backend/dat9.go`
- durable semantic tasks currently cover only `embed` and `img_extract_text`; there is no audio task type yet, see `pkg/semantic/task.go`
- `semantic worker` currently dispatches only `embed` and `img_extract_text`; it does not handle audio yet, see `pkg/server/semantic_worker.go`
- the current retrieval stack has no segment/chunk-level audio index structure; it only has file-level `content_text`

### Design Constraints

This proposal is shaped by the following verified constraints:

1. `content_text` is already the system's only stable entry point for searchable semantic text. Audio support should extend that path rather than bypass it.
2. In TiDB auto-embedding mode, the current contract is that the database derives vectors from `content_text`; the application should not take over `text -> vector` in that mode.
3. The system already has durable `semantic_tasks`, `semantic worker`, and revision-gated writeback; audio should not introduce a second async system.
4. The current `files` table is still a file-level model and does not support transcript chunks, timeline fragments, or multiple semantic representation columns.

## Goals

1. Introduce an `audio -> content_text -> search` production path that is consistent with the existing semantic stack.
2. In TiDB auto-embedding mode, ensure that once `content_text` is written back successfully, audio files can be recalled by the existing grep / FTS / vector ranking paths.
3. Reuse the existing durable `semantic_tasks` and `semantic worker` for audio async processing, including claim / retry / recover semantics.
4. Preserve revision correctness across create, overwrite, and upload completion, so the system never reaches a state where a new visible revision exists but the corresponding audio task is missing.
5. Keep the existing search APIs and high-level behavior unchanged rather than adding an audio-specific query entrypoint.

## Non-Goals

- do not design chunk-level or timeline-level audio index models in this phase
- do not introduce multiple searchable representation columns such as `transcript_text`, `summary_text`, or `keywords_text` in this phase
- do not change the `grep` API or introduce audio-specific search APIs in this phase
- do not design online migration, historical audio backfill, or cross-tenant bulk rebuild in this phase
- do not introduce a new workflow engine, media orchestration layer, or provider-neutral media runtime framework in this phase
- do not require DB9 to behave identically to TiDB auto-embedding in the first-phase closure

## Design

### 1. Keep Using the Current File Storage Model for Raw Audio

Raw audio bytes do not require a new storage layer. `drive9` already supports:

- inline `content_blob` storage for small files
- S3 / mock S3 storage for large files, with object location recorded in `storage_ref`
- create, overwrite, and multipart upload completion updating `files` and `file_nodes`

So the "storage" part of audio can keep the current behavior:

- small audio follows the existing `shouldStoreInDB()` decision
- large audio continues to use object storage
- create / overwrite can continue using the `content_type` already available during synchronous writes
- multipart upload completion in Phase 1 guarantees only stable extension-based recognition

The implementation focus here is not schema expansion. It is to make the actual type-input conditions of each write path explicit, and then register tasks conservatively based on those inputs.

### 2. Converge the Searchable Representation of Audio into `files.content_text`

This design follows the principle from `docs/auto-embedding-mode.md`: `content_text` is the canonical semantic source, not a complete and lossless copy of raw content.

For audio, this definition must be made more explicit:

- `content_text` exists to hold the text representation used for retrieval and semantic ranking
- in this phase, it is the trimmed transcript
- it does not promise to preserve a full verbatim transcript, full timeline, or rich structured transcription result

Phase 1 also needs another equally important runtime boundary: `audio_extract_text` is a single-task, single-file, single-revision processing model. This phase does not introduce:

- chunking
- segmented ASR
- streaming transcription
- a longer chain such as "full transcript first, then summary / keywords"

As a result, Phase 1 does not support "arbitrarily long audio can always produce semantic text." It only supports audio files that fit within a single-task resource boundary. At minimum, the current phase should use `max audio bytes` to bound task size; duration-based constraints can be added later during runtime hardening.

Under that boundary, the recommended Phase 1 behavior is:

- short audio: write normalized transcript text
- longer audio that still stays within the single-task resource boundary: write a truncated transcript
- audio that exceeds the single-task resource boundary: do not do multi-part processing, do not do summary fallback, and end with a limited terminal outcome without guaranteeing `content_text`

The current phase must make these limitations explicit:

- for longer audio that is still within the single-task boundary, storing only a truncated transcript creates a strong front-of-file bias and weak coverage for later content, so retrieval for long audio is naturally fragile
- for audio beyond the single-task resource boundary, this phase does not guarantee semantic retrieval closure

Both limitations are acceptable in the current phase and are explicitly deferred to later work together with long-text overview extraction, chunked retrieval, and runtime hardening. This proposal should not introduce an extra chain such as "long-audio ASR -> long transcript -> summary + keywords."

This boundary matters. The current `files` model is still file-level search. Writing arbitrarily long verbatim transcripts into `content_text` without control would directly amplify:

- embedding cost
- TiDB generated embedding cost
- FTS noise
- ranking skew between long audio and short documents

### 3. Add a Durable `audio_extract_text` Task Type

Audio text production should follow the same durable-task model as images rather than extending the historical backend in-memory queue.

This proposal adds only one durable task type:

- `semantic.TaskTypeAudioExtractText`

Its task semantics are:

- perform one audio text extraction attempt for one file revision
- in the handler, perform the revision gate, raw-byte loading, ASR call, text trimming, and `content_text` writeback

Its authoritative identity in `semantic_tasks` remains:

- `task_type + resource_id + resource_version`

Where:

- `resource_id` maps to `file_id`
- `resource_version` maps to `files.revision`

To let `audio_extract_text` carry the minimum useful runtime hints, the proposal also defines a companion payload structure:

- `semantic.AudioExtractTaskPayload`

This is not a second durable task. It is only the structure serialized into `semantic_tasks.payload_json` for `audio_extract_text`. `payload_json` stores only non-authoritative hint fields, for example:

- `path`
- `content_type`

These fields belong in the payload instead of the task identity because:

- task deduplication and revision correctness should depend only on `task_type + resource_id + resource_version`
- the handler still needs `path` and `content_type` for logging, fast type checks, and fallback handling
- these are runtime hint fields and should not change task identity semantics

The design principles stay aligned with the image path:

- the authoritative identity is still `resource_id + resource_version`
- the handler must re-read the current `files` row and apply the revision gate
- the payload is only for logging, fast type checks, and runtime assistance; it is not the final source of truth

### 4. All Three Write Paths Must Register `audio_extract_text` in the Same Transaction

This is one of the most important correctness requirements in the audio design.

Two premises must be explicit first. This correctness guarantee applies only when:

- the revision has already been recognized as a supported audio type
- and the current backend has `AudioExtractRuntimeOptions.Enabled`

Under those premises, once an audio revision becomes visible to the tenant, the corresponding `audio_extract_text` task must already durably exist. Otherwise the system can end up in states such as:

- the file exists but never gets a transcript
- there is no compensation path after worker restart
- create / overwrite / upload completion have inconsistent semantics

So, just like the image durable-task path, audio tasks should be registered by in-transaction enqueue at the following write paths:

- create / write
- overwrite
- multipart upload completion

The recommended landing points follow the current image implementation:

- the create / overwrite transaction blocks in `pkg/backend/dat9.go`
- the finalize upload transaction block in `pkg/backend/upload.go`

### 5. Audio Recognition Must Stay Conservative and Be Feasible Inside the Transaction

For "register `audio_extract_text` inside the transaction" to be implementable, Phase 1 must explicitly define the supported recognition boundary for audio rather than loosely assuming "the system can always identify MIME."

The recognition contract in this proposal must be defined per write path rather than described as if every path had the same MIME recognition ability:

- create / overwrite:
  - the write path registers `audio_extract_text` only when the revision has been recognized as a supported audio type
  - and only when `AudioExtractRuntimeOptions.Enabled` is set on the current backend
  - recognition prefers the `content_type` already available on the current write path
  - if `content_type` is missing or empty, it falls back to path-extension recognition
- multipart upload completion:
  - Phase 1 does not assume a reliable MIME input exists inside the completion transaction
  - the completion transaction currently has only `target_path` as a stable type input
  - and only when `AudioExtractRuntimeOptions.Enabled` is set on the current backend does completion actually register tasks
  - therefore, in Phase 1 the completion path guarantees only extension-based recognition of supported audio types
- across all write paths:
  - the system treats a revision as entering the audio semantic closure only when the available inputs explicitly match the Phase 1 supported audio set
  - if the available inputs on the current path cannot confirm type, or runtime is disabled, Phase 1 does not register `audio_extract_text` for that revision

That means the correctness semantics of this proposal should be understood as:

- for revisions recognized as supported audio, task registration must commit in the same transaction as revision visibility
- for revisions whose type cannot be confirmed cheaply, the system does not guarantee `audio_extract_text` registration

An implementation boundary also needs to be explicit here: the current multipart upload completion path does not persist client-declared MIME / `content_type`, and it does not re-read object contents for sniffing during finalize. So the proposal must not describe completion as "`content_type` first, extension fallback," because that would be stronger than the contract current code can evolve into.

This boundary is intentional. In Phase 1, the upload completion path does not introduce:

- extra object probing
- re-downloading objects for media sniffing
- relying on the ASR provider to probe file type
- a separate media metadata preprocessing step
- MIME / `content_type` persistence at upload-init time

Those approaches would all expand the transaction boundary and implementation complexity significantly, weakening the "minimal shippable closure" that the proposal is trying to preserve.

The recommended Phase 1 supported set should remain conservative, for example:

- MIME:
  - `audio/mpeg`
  - `audio/wav`
  - `audio/x-wav`
  - `audio/mp4`
  - `audio/x-m4a`
  - `audio/aac`
  - `audio/ogg`
  - `audio/webm`
  - `audio/flac`
- extensions:
  - `.mp3`
  - `.wav`
  - `.m4a`
  - `.aac`
  - `.ogg`
  - `.flac`

This also implies a clear limitation: some files that are truly audio, but lack reliable MIME and also lack standard extensions, may not enter the audio semantic retrieval closure in Phase 1. This limitation is stronger on multipart upload completion, because that path guarantees only extension-level recognition in the current phase. This is acceptable for now; if higher recognition rates are needed later, they should be handled as a separate enhancement.

### 6. The Audio Handler Should Reuse the Overall Structure of the Image Handler

The recommended implementation is an audio handler parallel to `ProcessImageExtractTask()`, for example:

- `ProcessAudioExtractTask(ctx, task AudioExtractTaskSpec) (AudioExtractResult, error)`

Its processing flow should preserve the same correctness structure as the image path:

1. check whether runtime is configured
2. read the current `files` row by `file_id`
3. if the file does not exist, ack obsolete
4. if the file is unconfirmed, not audio, or the revision has changed, ack obsolete
5. check whether the current file size fits within the single-task resource boundary; if not, finish with a terminal business outcome
6. load raw bytes from the current file location
7. run audio transcription
8. sanitize and length-trim the output
9. if the trimmed result is empty, finish with a terminal business outcome
10. perform revision-gated writeback with `UpdateFileSearchTextTx(...)` inside a transaction
11. in auto-embedding mode, stop there
12. leave DB9 and app-managed embedding bridging to a later proposal

This structure is beneficial because it:

- reuses the already-verified revision safety pattern
- reuses the delivery semantics of the existing `semantic worker`
- reuses the backend-as-runtime-dependency-container model
- avoids designing a separate audio task scheduling framework

### 7. Audio Runtime and Delivery Must Be Explicitly Separated

The audio design should continue reusing the backend as the runtime dependency container, but it must not duplicate the historical image pattern where the backend both owns the runtime and starts its own async queue / worker.

The minimal implementation recommendation is:

- add `AudioExtractRuntimeOptions` to `backend.Options`
- store the following in `Dat9Backend`:
  - `audioExtractEnabled`
  - `audioExtractor`
  - `audioExtractTimeout`
  - `audioExtractMaxBytes`
  - `maxAudioExtractTextBytes`
- expose audio runtime capability from the fallback backend or tenant backends created via `tenant.Pool`

The first provider implementation should be fixed to an OpenAI-compatible ASR. That keeps the integration style aligned with the current image extraction path:

- configuration shape stays similar, which lets the current server env wiring pattern be reused
- runtime ownership stays in the backend rather than introducing a new media service abstraction
- Phase 1 can focus on durable tasks, revision-gated writeback, and search closure rather than expanding the provider matrix at the same time

Two responsibilities must be explicitly separated here:

- **runtime ownership**: the backend owns execution dependencies such as extractor, timeout, and max bytes
- **delivery ownership**: `semantic worker` is the only async dispatcher responsible for claim / retry / recover

So, in Phase 1 the audio path must follow these rules:

- the backend may hold audio runtime configuration
- the backend may expose execution entry points such as `SupportsAudioExtractRuntime()` and `ProcessAudioExtractTask(...)`
- the backend must not own an audio-specific queue
- the backend must not start audio-specific worker goroutines
- the backend must not provide in-memory dispatch entry points such as `enqueueAudioExtract(...)`
- all async audio work must be registered only by transactional writes into `semantic_tasks`

With that split, the responsibility of `semantic worker` remains only:

- claim tasks
- locate the target backend / store
- invoke the backend handler
- ack / retry based on the result

The worker does not directly own media runtime configuration and does not need direct awareness of S3, the ASR provider, or byte limits. The backend does not take on any async scheduling responsibility; it only owns the runtime dependencies needed to execute the task.

The design intent must also be explicit here: `audioExtractMaxBytes` is not an optimization. It is part of the Phase 1 support boundary. Its purpose is to ensure `audio_extract_text` remains a bounded single-task operation rather than implicitly requiring the system to support complete transcription of arbitrarily long audio.

The semantics of `AudioExtractRuntimeOptions.Enabled` must also be restricted explicitly to:

- the global switch for the audio path: only when `Enabled=true` do recognized audio revisions register `audio_extract_text`
- allowing the backend to hold and expose the runtime dependencies needed to execute `audio_extract_text`

This means:

- `Enabled=false` produces no durable audio backlog
- `retry / backoff` applies only to execution-time failures after task registration, such as byte-loading failure, ASR failure, or writeback failure

It does not mean the backend schedules audio tasks by itself, nor does it mean the system starts backend-owned async worker threads.

### 8. `semantic worker` Capability Routing Should Converge on `TaskType` Sets

Phase 1 should not duplicate another image-specific routing branch just for audio. Current capability checks in the image path are still image-specific. Once audio lands, if the worker grows more parallel branches such as `hasAudioHandler()` and `allowedAudioTaskTypes`, `semantic worker` will increasingly depend on task-specific booleans, and maintainability will degrade as new semantic task types are added.

This proposal recommends a small but explicit convergence:

- do not introduce a generic handler registry
- do not introduce a workflow engine
- do not rewrite the current explicit `dispatchTask()` style
- only converge the worker capability routing from image-specific booleans to the minimal capability model based on `semantic.TaskType` sets

The concrete principles are:

- an auto-embedding backend exposes the set of auto semantic task types it can currently execute
- `semantic worker` uses that set to decide:
  - whether a tenant target should be scanned
  - which task types to claim for a target
- actual task execution still stays in an explicit switch, for example:
  - `embed`
  - `img_extract_text`
  - `audio_extract_text`

That implies a Phase 1 code shape close to:

- auto-embedding backend: return the currently supported auto task types
- app-managed backend: if an embed handler exists, continue to allow only `embed`
- worker target filtering and `ClaimSemanticTask(...taskTypes)` should derive from task-type sets rather than task-specific boolean names such as `hasImageHandler()`

The design intent of this boundary is:

- solve the real problem in this proposal, namely that durable text-production tasks should not continue depending on image-specific routing
- keep the change small by converging only capability routing rather than inventing a more generic execution framework at the same time
- provide a more stable evolution path for future task types such as `.abstract.md` / `.overview.md`

### 9. TiDB Auto-Embedding as the First Fully Closed Support Surface

This proposal recommends supporting the following providers first:

- `tidb_zero`
- `tidb_cloud_starter`

The reason is that the system already has a closure under those providers:

- file write paths no longer manually clear embeddings
- once `content_text` is written back, the database automatically generates vectors
- search already uses `VectorSearchByText(...)`

So in that mode, audio only needs to solve:

- durable task registration
- revision-safe `content_text` writeback

The first phase therefore does not need to expand the app-managed embedding `text -> vector` lifecycle at the same time.

### 10. Defer DB9 and App-Managed Embedding Instead of Making Them First-Phase Targets

To keep the proposal small, the first phase should not treat DB9 and app-managed embedding modes as the main support surface.

The explicit scope of this proposal is:

- TiDB auto-embedding providers: full first-phase support
- DB9 / app-managed embedding: deferred to a later proposal

If app-managed embedding support is added later, it can follow the current image pattern and bridge to an `embed` task after transcript writeback succeeds. But that path is not part of the current phase rollout or validation scope.

### 11. Keep Search Interfaces Unchanged and Reuse Existing Grep Logic for Audio

Current `grep` already runs in parallel:

- FTS over `content_text`
- vector ranking
- keyword fallback

After audio lands, it should not introduce another API branch. For TiDB auto-embedding tenants that have entered the audio text-production closure in this phase, once the audio task writes back `content_text` successfully:

- those tenants automatically participate in vector ranking
- they also continue participating in FTS and keyword fallback

This preserves:

- unchanged APIs
- unchanged CLI
- unchanged routing
- unchanged ranking and degraded-path semantics

## Incremental Plan

### Phase 1: Define the Task and Runtime Boundaries

1. add `TaskTypeAudioExtractText` in `pkg/semantic/task.go`
2. define the minimal `AudioExtractTaskPayload`
3. add audio runtime configuration to `backend.Options` and `Dat9Backend`
4. define the `AudioTextExtractor` interface and basic result enums

### Phase 2: Integrate the Write Paths

1. register `audio_extract_text` for audio revisions in create / overwrite transactions
2. define the audio recognition contract per write path:
   - create / overwrite: `content_type` first, extension fallback
   - multipart upload completion: Phase 1 guarantees only extension-based recognition
3. register `audio_extract_text` in the multipart upload completion transaction only for revisions recognized as supported audio
4. make explicit that Phase 1 does not introduce upload-init MIME / `content_type` persistence
5. ensure task registration and revision visibility commit in the same transaction

### Phase 3: Integrate the Worker and Handler

1. add an `audio_extract_text` dispatch case in `semantic worker`
2. converge worker capability routing to filter targets and claim scope by `TaskType` sets
3. implement `ProcessAudioExtractTask(...)`
4. connect ack / retry / recover and logs / metrics

### Phase 4: Search Closure and Validation

1. validate that audio written back in TiDB auto-embedding mode can be recalled by grep
2. validate that vector-path failure still degrades to the lexical path
3. validate that overwrite and repeated upload do not write back stale transcripts

## Validation Strategy

- **Write-path validation**
  - a create operation recognized as supported audio leaves a durable `audio_extract_text` task
  - an overwrite operation recognized as supported audio registers a new task for the new revision
  - upload completion whose extension is recognizable as supported audio registers a new task for the final visible revision
  - upload completion whose type cannot be confirmed does not register `audio_extract_text`
  - when `AudioExtractRuntimeOptions.Enabled=false`, even recognized audio revisions do not register tasks

- **Correctness validation**
  - audio tasks for old revisions must not overwrite `content_text` of new revisions
  - deleted files, unconfirmed files, and non-audio files are safely acked as obsolete
  - registered tasks enter retry / backoff on byte-loading failure, ASR failure, and writeback failure
  - audio beyond the single-task resource boundary ends with an explicit terminal result instead of retrying forever

- **Search validation**
  - once transcript text is written back, it can be recalled by FTS
  - in TiDB auto-embedding mode, it can participate in vector ranking
  - when the ranked path has no result, keyword fallback still applies

- **Operational validation**
  - queue depth, success rate, retry count, and dead-letter count are observable
  - unfinished tasks can be recovered after worker restart
  - large audio does not cause unbounded transcript writes and abnormal blow-up
  - in Phase 1, only audio within the single-task resource boundary is guaranteed to enter the semantic retrieval closure
  - long audio currently depends only on truncated transcript, and the resulting retrieval fragility is explicitly documented and kept within a known range

## Risks and Mitigations

1. **Phase 1 does not support full semantic text production for arbitrarily long audio**
   - Mitigation: explicitly limit audio handling to the single-task resource boundary, with `max audio bytes` as the first guardrail; out-of-bound audio ends with a terminal result instead of entering multi-part handling or a second-stage summary chain.

2. **Long audio stores only truncated transcript, so retrieval coverage for later content is weak**
   - Mitigation: within the single-task boundary, Phase 1 explicitly accepts this limitation and guarantees only a minimal closure that is workable, searchable, and cost-controlled; stronger long-content representations are deferred and can be designed together with long-text overview extraction.

3. **Audio processing tends to be much slower than image processing, increasing timeout and retry cost**
   - Mitigation: configure audio task timeout, maximum bytes, and maximum extracted text size independently; do not reuse the image defaults.

4. **Byte limits alone do not accurately bound audio processing cost**
   - Mitigation: in Phase 1 the payload requires only `path` and `content_type`; `duration_ms` is deferred to a later enhancement and can be added with duration-based guardrails during runtime hardening.

5. **File-level `content_text` has limited retrieval quality for long audio**
   - Mitigation: this proposal explicitly defers chunk / segment retrieval to later proposals instead of introducing a new data model in the current phase.

6. **A conservative recognition strategy leaves some real audio files outside the semantic closure**
   - Mitigation: bind correctness explicitly to revisions recognized as supported audio on the current write path; in Phase 1, create / overwrite use `content_type` first with extension fallback, while multipart upload completion guarantees only extension-based recognition, and higher recognition rates are deferred to later enhancement.

7. **Including DB9 / app-managed embedding in the first phase would significantly increase complexity**
   - Mitigation: Phase 1 uses TiDB auto-embedding as the primary support surface; DB9 / app-managed audio bridging is deferred to a later proposal.

## Open Questions

There are no blocking design questions at this stage. The audio provider has been fixed to an OpenAI-compatible ASR, and the remaining deferred items have been moved into `Follow-up`.

## Follow-up

| Priority | Topic | Description |
| --- | --- | --- |
| P1 | Audio Runtime Hardening | Add duration guardrails, provider fallback, cost controls, and finer-grained observability for audio tasks, and evolve the current Phase 1 single-task resource boundary from `max audio bytes` to a more complete bytes + duration strategy. |
| P1 | Media Type Detection Hardening | Improve media type recognition rates in upload completion and other write paths, evaluate stronger metadata detection strategies without breaking transaction boundaries, and separately assess whether upload-init MIME / `content_type` persistence is needed. |
| P1 | Long-Content Overview Extraction | Bring long audio and long text files into a shared overview-extraction design, addressing the current retrieval fragility caused by using only truncated transcripts for long content. |
| P1 | Chunked Audio Retrieval | Design segment/chunk-level transcript storage and retrieval, addressing the limited retrieval quality of file-level `content_text` for long audio. |
| P2 | Audio Format Expansion | Gradually expand the audio closed set while keeping the current closure shape unchanged, adding common formats such as M4A / AAC / FLAC / OGG and their MIME aliases across create, overwrite, and upload completion paths; WebM and stronger MIME contracts remain separate evaluation topics. |
| P2 | App-Managed Audio Bridging | Turn transcript writeback and `embed` bridging under app-managed embedding into a separate implementation proposal. |
| P3 | Media Runtime/Delivery Decoupling Cleanup | Gradually converge historical media tasks such as image extraction away from the backend-owned queue / worker model toward the same runtime / delivery split as audio, so the backend holds only runtime dependencies and `semantic worker` exclusively owns delivery. |
| P3 | Semantic Worker Capability Cleanup | Further converge `semantic worker` away from task-specific capability checks toward a more stable task-capability model, and evaluate whether the runtime representation should be unified across image, audio, and future generated task types. |
