# dat9 Overview

## 1. What dat9 is

dat9 is a filesystem-like data platform for agents.

It gives users and agents a unified way to:

- store files
- organize them in a path-based namespace
- search them by metadata, keywords, and semantics
- inspect lightweight summaries and overviews before loading full content
- work with large and small files through one consistent interface

From the outside, dat9 should feel simple: a network-drive-like system with built-in retrieval and inspectable semantic artifacts.

From the inside, dat9 is designed as a recoverable, multi-tenant, asynchronous system with explicit state boundaries.

## 2. User-facing model

Users and agents interact with dat9 through a filesystem-like model:

- `cp` to upload or copy
- `cat` to read
- `ls` to browse
- `mv` to rename or move
- `rm` to delete
- `search` to retrieve by meaning or metadata

The namespace is path-based, but dat9 is not just a thin wrapper around object storage. It also provides:

- semantic summaries
- hierarchical overviews
- lightweight semantic artifacts for relations
- async processing for derived artifacts

dat9 follows an L0 / L1 / L2 content model:

- **L0**: short summaries for fast scanning
- **L1**: structured overviews for navigation and quick understanding
- **L2**: full underlying content

Semantic artifacts such as `.abstract.md`, `.overview.md`, and `.relations.json` should remain visible and inspectable through the filesystem-like interface.

## 3. Core product principles

The core product principles are:

- small files live in `db9`
- large files live in `S3`
- tenants are isolated by tenant-local state and execution boundaries
- the external model stays filesystem-like and simple
- the internal model remains versioned, asynchronous, and recoverable
- semantic artifacts should be inspectable rather than hidden behind opaque internal APIs

These principles mean dat9 aims to combine a simple user experience with a disciplined backend design.

## 4. High-level architecture

At a high level, dat9 is organized as a global control plane plus tenant-local cells.

```text
+------------------------------------------------------+
|                Global Control Plane                  |
| auth | tenant routing | provisioning | quota | o11y |
+-------------------------------+----------------------+
                                |
                                v
+------------------------------------------------------+
|                    Tenant Cell                       |
|                                                      |
|  Resource & Retrieval Control Layer                  |
|                  |                                   |
|                  v                                   |
|            AGFS Access Plane                         |
|        /local                /queue                  |
|          |                     |                     |
|          v                     v                     |
|    S3 Namespace        durable queuefs               |
|   (large objects)       (db9 backend)               |
|                \         /                           |
|                 \       /                            |
|                  v     v                             |
|                db9 State Plane                       |
|  small files | metadata | vectors | indexes | tasks |
|                        ^                             |
|                        |                             |
|               Worker / Async Runtime                 |
+------------------------------------------------------+
```

In this model:

- the **Global Control Plane** handles auth, tenant routing, provisioning, quota, and cross-tenant observability
- each **Tenant Cell** owns an isolated state and execution unit
- **AGFS** provides the unified access surface
- `db9` holds small-file content, metadata, indexes, vectors, and async task state
- `S3` holds large-file content in a tenant-scoped namespace
- durable `queuefs` provides tenant-local asynchronous execution primitives

## 5. Internal design stance

dat9 intentionally separates two layers that should not be collapsed into one:

- **path/file semantics** for user-visible naming, browsing, copying, and moving
- **resource/version semantics** for asynchronous derivation, writeback safety, indexing, and recovery

In this terminology:

- a **file** is the user-visible path-addressable item
- a **logical object** is the internal content identity behind one or more file paths
- a **resource** is the internal versioned processing unit used for async correctness
- a **derived artifact** is any generated output produced from resource processing
- a **semantic artifact** is a user-visible derived artifact such as `.abstract.md` or `.overview.md`

This distinction is important.

An inspectable filesystem-like product does not imply that the entire internal architecture should be modeled only as files. Internally, dat9 still needs:

- versioned resource state
- processing state
- task state
- reconcile and recovery behavior

Similarly, AGFS is the access plane, not the source of business correctness. Correctness comes from the system's versioning, task-runtime, and reconciliation rules.

## 6. Non-goals

dat9 does not aim to provide:

- full POSIX semantics
- a global shared queue that solves multi-tenant fairness by itself
- exactly-once async execution semantics
- one giant monolithic metadata model for every subsystem
- a purely file-row-based internal architecture

## 7. Document map

This overview is intentionally short. Detailed design belongs in focused RFCs.

Recommended core RFCs:

- `docs/design/system-architecture.md`
- `docs/design/canonical-schema.md`
- `docs/design/storage-and-namespace.md`
- `docs/design/resource-versioning-and-async-correctness.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/semantic-derivation-and-retrieval.md`
- `docs/design/write-path-and-reconcile.md`
- `docs/design/control-plane-and-provisioning.md`

Use these RFCs for the detailed definitions of:

- tenant isolation
- storage tiering and namespace rules
- async task lifecycle
- writeback correctness
- retrieval and semantic derivation
- upload, reconcile, and recovery behavior

## 8. Summary

dat9 should be understood as:

- a filesystem-like data platform for agents
- simple on the outside
- versioned and recoverable on the inside
- built around tenant-local `db9 + durable queuefs + S3 namespace`

The role of this overview is to define the product shape and the reader's mental model.

The role of the RFC set is to define how that product remains correct in implementation.
