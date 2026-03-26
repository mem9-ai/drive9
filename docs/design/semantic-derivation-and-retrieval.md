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

Examples include:

- `.abstract.md`
- `.overview.md`
- `.relations.json`

These semantic artifacts may be visible as ordinary files in the product interface even if the backend keeps additional internal state.

### 4.2 Retrieval participation

- small files may participate directly in retrieval
- large files should not be required to participate in full-content retrieval by default
- large files should participate through summaries, overviews, or derived parsed text

### 4.3 Derivation model

```text
L2 raw content
   -> parsed text
   -> summary (.abstract.md)
   -> overview (.overview.md)
   -> vectors / indexes
```

### 4.4 Aggregate derivation

Aggregate tasks such as overview generation should not operate on unbounded dynamic trees.

They should operate on bounded snapshot inputs or explicitly versioned aggregate inputs.

## 5. Invariants / Correctness Rules

- semantic artifacts should be inspectable in the product model
- small-file direct retrieval and large-file summary-based retrieval should coexist
- aggregate derivation must operate on bounded inputs
- derived artifacts should remain rebuildable from authoritative state

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
