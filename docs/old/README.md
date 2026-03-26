# Archived Design Notes

This directory keeps historical design material that was useful during the project's early exploration and initial implementation phase.

These documents remain in the repo for reference, but they are **not** the current source of truth.

## Status

Use `docs/design/*.md` and `docs/overview.md` as the current normative design set.

Use the documents in `docs/old/` only for:

- early-stage implementation context
- historical rationale
- recovering details that may still need to be migrated into the current RFC set

## Conflict Rule

If a statement in `docs/old/` conflicts with a statement in `docs/design/` or `docs/overview.md`, the current docs win.

In particular:

- canonical tables, columns, and indexes are now defined in `docs/design/canonical-schema.md`
- current write, reconcile, and lock protocols are defined in `docs/design/write-path-and-reconcile.md`
- current async-runtime direction is defined in `docs/design/durable-queue-runtime.md`

## Known Stale Areas

The old documents contain assumptions that should not be copied forward without verification.

Examples:

- old assumptions that db9 provides generated-column helpers such as `EMBED_TEXT()` and similar automatic embedding/chunking behavior
- old schema descriptions that predate `docs/design/canonical-schema.md`
- older async/runtime assumptions that were later narrowed into the current phased P0 path

## File Guide

- `design-overview.md`
  - archived early implementation guide
  - still useful for understanding how the first code paths were shaped
  - contains some implementation detail that may still be worth migrating into current RFCs

- `dat9_layer_draft.md`
  - archived architecture exploration
  - useful for layered system thinking and terminology history
  - not a current rollout plan or canonical implementation target
