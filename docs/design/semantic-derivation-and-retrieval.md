# RFC: dat9 Semantic Derivation and Retrieval

## 1. Goal

This RFC defines how dat9 derives semantic artifacts and how derived artifacts participate in retrieval.

It focuses on:

- L0 / L1 / L2
- summaries, overviews, relations, parsed text, chunks, vectors, and indexes
- the difference between small-file and large-file participation in retrieval
- aggregate derivation requirements

## 2. Non-goals

This RFC does not define:

- the full resource-version protocol
- low-level queue task schemas
- full ranking, reranking, or agent-planning logic

## 3. Definitions

- **L0**: short summary for quick scanning
- **L1**: structured overview for navigation and quick understanding
- **L2**: full underlying content
- **derived artifact**: any output generated from resource processing, including summaries, overviews, parsed text, chunks, vectors, and relation sidecars
- **semantic artifact**: a user-visible derived artifact such as `.abstract.md`, `.overview.md`, or `.relations.json`
- **aggregate task**: a task that derives content from a collection, directory, or bounded set of inputs rather than from one file alone

## 4. Design

### 4.1 User-facing semantic model

dat9 should preserve a visible, inspectable semantic layer.

Representative artifacts include:

- `.abstract.md`
- `.overview.md`
- `.relations.json`

Representative layered model:

| Layer | Typical artifact | Purpose | Recommended storage |
| --- | --- | --- | --- |
| L0 | `.abstract.md` | ultra-short summary for quick scanning and broad recall | `db9` small file |
| L1 | `.overview.md` | structured overview and navigation aid | `db9` small file |
| L2 | original file content | full detail loaded on demand | `db9` or `S3` depending on size |

Representative directory shape:

```text
/data/training-v3/
  .abstract.md
  .overview.md
  .relations.json
  metadata.json
  images.tar.gz
```

These semantic artifacts should remain visible as ordinary files where practical, even if the backend also keeps additional internal state for processing and retrieval.

### 4.2 Retrieval participation

- small files may participate directly in retrieval
- large files should not be required to participate in full-content retrieval by default
- large files should participate through summaries, overviews, or derived parsed text

The intended retrieval surface is:

- small L2 text files may be embedded and indexed directly
- large L2 binaries or large documents usually participate through L0/L1 and optional parsed/chunked text
- retrieval may recall chunk-level evidence, but answer assembly should still be able to backtrack to the resource or directory level

### 4.3 Derivation model

```text
L2 raw content
   -> parsed text / chunks
   -> summary (.abstract.md)
   -> overview (.overview.md)
   -> vectors / indexes
```

Representative semantic pipeline:

```text
file write or version change
        |
        v
   parse_resource
        |
        +--> parsed text / chunk extraction
        |
        +--> generate L0 summary
        |
        +--> generate L1 overview
        |
        +--> embed summary / overview / chunks
        |
        +--> refresh retrieval indexes
```

This pipeline is version-aware. Every stage operates on explicit input versions and must not advance current semantic state with stale outputs.

### 4.4 Retrieval flow

Retrieval should be understood as a three-stage pipeline:

```text
query
  |
  v
query preparation
  - tenant routing
  - choose vector / keyword / hybrid mode
  - generate query embedding if needed
  |
  v
candidate recall
  - metadata filtering
  - vector recall
  - keyword recall
  - optional chunk-level recall
  |
  v
result fusion
  - backtrack chunk hits to resource or directory
  - combine L0, L1, and direct file hits
  - optional rerank
```

Representative large-file retrieval path:

```text
user query
  -> vector / keyword search hits /data/training-v3/.abstract.md
  -> read /data/training-v3/.overview.md
  -> decide whether to open metadata.json or images.tar.gz
```

This is the reason large files do not need to be directly embedded in their full original form to remain retrievable.

### 4.5 Representative logical state for derivation and retrieval

The old design contained a useful implementation-grade split between semantic material, vector material, and processing state. The new docs should preserve that distinction.

Representative logical records:

- semantic material
  - keyed by `resource_id` + `resource_version` + semantic type
  - stores summaries, overviews, parsed text, or relation sidecars
- vector material
  - keyed by `resource_id` + `resource_version` + source type
  - may contain resource-level vectors and chunk-level vectors at the same time
- processing state
  - keyed by `resource_id`
  - tracks which current version has been parsed, summarized, overview-generated, and indexed

Representative mental model:

```text
resource_id
  -> current_version = v7
  -> semantic outputs:
       summary(v7)
       overview(v7)
       parsed_chunks(v7)
  -> vector outputs:
       summary_vec(v7)
       chunk_vecs(v7)
  -> processing state:
       parsed_version = v7
       indexed_version = v7
```

This split is important because index health should not be inferred only from "some vectors exist". The system needs explicit state that says whether the current version has been fully processed.

### 4.6 Aggregate derivation

Aggregate tasks such as overview generation should not operate on unbounded dynamic trees.

They should operate on bounded snapshot inputs or explicitly versioned aggregate inputs.

Representative aggregate boundary:

```text
directory /data/project/
  current children may keep changing

aggregate task input must be one of:
  - snapshot S42 of the child set
  - explicit list of resource versions

not:
  - "whatever is currently under /data/project/ when the worker happens to finish"
```

Recommended aggregate rules:

- aggregate tasks declare their input boundary explicitly
- bottom-up propagation may trigger parent refresh, but each refresh still binds a concrete snapshot
- stale aggregate outputs may be retained for debugging, but must not become the current overview for a newer snapshot

### 4.7 Relation sidecars

`.relations.json` remains useful as a visible, advisory artifact.

Representative shape:

```json
{
  "relations": [
    {
      "target": "/data/imagenet/",
      "type": "derived_from",
      "description": "Training subset extracted from ImageNet"
    }
  ]
}
```

Current design assumptions:

- relation sidecars are inspectable files, not hidden metadata only
- they are advisory by default
- deletion of one target does not imply cascading lifecycle changes elsewhere

## 5. Invariants / Correctness Rules

- semantic artifacts should be inspectable in the product model
- small-file direct retrieval and large-file summary-based retrieval should coexist
- aggregate derivation must operate on bounded inputs
- derived artifacts should remain rebuildable from authoritative state
- chunk-level recall may exist, but final current-state progression remains resource-version-aware
- processing state should explicitly indicate whether the current version has been parsed or indexed

## 6. Failure / Recovery

- missing or stale derived artifacts must be regenerable through reconcile and async reruns
- failed derivation must not invalidate authoritative resource state

## 7. Open Questions

- exact snapshot protocol for aggregate tasks
- whether relation sidecars remain purely advisory or gain stronger system meaning over time
- how much parsed/chunked text should be materialized by default for large files

## 8. References / Dependencies

- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/write-path-and-reconcile.md`
- `docs/design/api-and-ux-contract.md`
