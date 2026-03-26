# RFC: Layered Design for `dat9` on Per-Tenant S3 + TiDB + Durable QueueFS (Layer V3)

**Status**: Archived architecture exploration

This document is kept as a historical layered architecture draft.
It is useful for terminology and architectural exploration, but it is **not** the current rollout plan or canonical implementation target.

Use current docs first:

- `docs/overview.md`
- `docs/design/*.md`
- `docs/design/canonical-schema.md`

Conflict rule:

- if this document conflicts with the current RFC set, the current RFC set wins

Known stale areas in this document include:

- stronger early assumptions about queue/runtime rollout than the current phased P0 path
- schema and state-model discussions that are now split across focused RFCs
- architecture exploration detail that exceeds the current implementation target

## 1. Overview

This document defines the target layered architecture for `dat9`.

It assumes the system runs on the following infrastructure:

- Files and raw objects are stored in **S3 / S3-compatible object storage**
- Metadata, retrieval indexes, vector data, and asynchronous task state are stored in **TiDB**
- Asynchronous task dispatch, lease, acknowledgment, and recovery rely on **durable `queuefs`**

This document adopts the following deployment and isolation assumptions:

- Each tenant has its own dedicated metadata and task execution unit
- Each tenant has its own `TiDB` cluster or dedicated TiDB service unit
- Each tenant has its own durable `queuefs`
- Each tenant has a tenant-scoped object storage namespace, represented by default as an isolated prefix under a shared bucket using `tenant_id`; when stronger physical isolation is needed, this may also be implemented as a dedicated bucket or another equivalent isolation unit

Under this model:

- `S3` is responsible for file and raw object storage
- `TiDB` is responsible for metadata, retrieval indexes, vector data, and asynchronous task state
- `dat9` organizes and consumes these infrastructure capabilities using the **Tenant Cell** as the isolation and deployment boundary

In this document, `queuefs` is positioned as:

- **the durable task execution substrate within a tenant**

Accordingly:

- Cross-tenant routing, instance discovery, and tenant lifecycle management belong to the global control plane
- Resource metadata, task state, index state, and asynchronous execution recovery belong to the tenant-local data and execution plane

- This document defines how `dat9` organizes and depends on these infrastructure capabilities; it does not redefine the queue substrate itself
- The low-level capabilities of durable `queuefs` are defined by the prerequisite RFC; this document only defines how `dat9` depends on those capabilities

---

## 2. Prerequisites

This document depends on the following prerequisite capability already being available:

- `dat9/docs/queuefs-durable-task-queue-rfc`

This document assumes that RFC provides at least the following capabilities:

- `enqueue / dequeue / ack / nack / recover / stats`
- `lease / renew / heartbeat`
- `retry / dead letter`
- at-least-once delivery
- stale processing recovery

This document does not redefine the queue substrate semantics above. It only further specifies:

- how `dat9` organizes these capabilities by Tenant Cell
- how async tasks are bound to `resource_version` as their input boundary
- how worker writeback is constrained by version validation, state advancement, and stale suppression

At the same time, this document does not assume a single shared `queuefs` directly handles cross-tenant fair scheduling; that responsibility belongs to the global control plane.

---

## 3. Design Goals

### 3.1 Goals

- Provide a unified resource naming and access facade
- Support directory-style resource organization at massive object-storage scale
- Support asynchronous content understanding, summarization, vectorization, and index refresh
- Support multi-tenant isolation, auditable execution, and recoverable task processing
- Support decoupling between the control plane and the tenant-local data/execution plane
- Support horizontally scalable global API nodes and tenant-local worker nodes
- Support decoupling between retrieval control logic and underlying index execution
- Bring both file access and task access under the AGFS access plane while preserving their domain-specific state semantics
- Support resource lifecycle, version advancement, task recovery, and index rebuild within a single tenant

### 3.2 Key Architectural Constraints

- Every `Tenant Cell` has its own dedicated `TiDB` and durable `queuefs`
- The `S3` side uses tenant-scoped namespace isolation and does not require one bucket per tenant physically
- Every asynchronous task must bind to an explicit `resource_version`
- Every asynchronous writeback must pass version validation, or only write into historical-version results without advancing the current state
- AGFS unifies the access facade, not the domain model of objects and tasks

### 3.3 Non-Goals

- Do not attempt full POSIX semantics
- Do not pretend object storage is strongly consistent block storage
- Do not require a distributed strong transaction spanning `S3 + TiDB`
- Do not require object storage to use a physically isolated "one bucket per tenant" model
- Do not require a globally shared queue substrate to provide cross-tenant fair scheduling
- Do not require exactly-once semantics
- Do not equate the durable queue directly with a full workflow engine

---

## 4. Architecture Model

This document adopts a **Global Control Plane + Tenant Cell** model.

Where:

- **Global Control Plane** is responsible for tenant discovery, routing, authentication, quotas, observability aggregation, and instance orchestration
- **Tenant Cell** is responsible for resource state, task execution, index maintenance, and recovery within a tenant

Each `Tenant Cell` includes at least:

- a tenant-scoped AGFS access plane
- a tenant-scoped durable `queuefs`
- a tenant-scoped `TiDB`
- a tenant-scoped S3 namespace, represented by default as a `tenant_id`-isolated prefix under a shared bucket, but optionally as a dedicated bucket when stronger physical isolation is required
- one or more tenant-scoped worker pools

The direct effect of this design is:

- `queuefs` only needs to solve durable execution within a single tenant
- multi-tenant fairness, global routing, and noisy-neighbor control no longer need to be handled by `queuefs` itself

---

## 5. Overall Principles

### 5.1 Tenant Cell as the Unit

System scaling, isolation, recovery, and operations all use the `Tenant Cell` as the basic unit.

Task backlog, index failures, or TiDB faults in one tenant must not directly contaminate other tenants.

### 5.2 Externalized State

All critical persistent state must be stored in shared infrastructure or tenant-scoped infrastructure, rather than on the local disk of a single process. This includes:

- resource body
- resource metadata
- semantic material metadata
- vector index records
- asynchronous task state
- processing result state

### 5.3 Separation of Control Plane and Execution Plane

- **Global control plane** is responsible for tenant routing, auth, quota, provisioning, and fleet-level observability
- **Tenant-local control layer** is responsible for resource semantics, lifecycle orchestration, retrieval strategy, and version advancement
- **Tenant-local execution layer** is responsible for task claiming, lease renewal, processing, writeback, and recovery

### 5.4 Async-First

After resource import, recomputation steps are asynchronous by default, including:

- document parsing
- single-resource summary generation
- directory/collection overview generation
- vectorization
- index refresh
- reconciliation and repair

The synchronous path only guarantees:

- resource submission succeeds
- minimal visibility is established
- follow-up async tasks are reliably submitted or can be compensated and recovered later

### 5.5 Idempotency-First

Any asynchronous task, index write, or state advancement must support retry and remain logically correct under repeated execution.

### 5.6 Version-First

The async system in `dat9` does not execute "against a resource name," but rather "against a resource version."

Therefore:

- tasks must bind to an explicit `resource_version`
- worker writeback must validate the target version
- stale tasks may only be safely discarded or marked stale; they must never overwrite the result of a newer version

### 5.7 Unified Entry, Distinct Semantics

AGFS still serves as the unified access plane and provides:

- `/local`: object access
- `/queue`: task access

But this document explicitly states:

- objects are not tasks
- tasks are not files
- they share the same access plane and mounting model, but they do not share identical state semantics

### 5.8 The Boundary of AGFS

To avoid confusion, this document explicitly distinguishes between a "unified entry" and a "unified model":

- AGFS unifies the access method, mount method, and access facade
- AGFS does not define a unified domain model for objects and tasks
- `/local` exposes object access primitives
- `/queue` exposes durable task queue primitives

Therefore, in `dat9`:

- AGFS is the unified access layer, not the unified resource semantics layer
- resource naming, lifecycle, version advancement, and retrieval orchestration are defined by upper control layers
- task dependencies, aggregation triggers, and state advancement protocols are also defined by upper control layers

This boundary must remain clear at all times:

- tasks must not be mistaken for ordinary file objects just because `/queue` is mounted under AGFS
- and the shared AGFS access shell of `/local` and `/queue` must not be taken to imply identical state transition rules

---

## 6. Overall Architecture

### 6.1 Global Control Plane

The global control plane is responsible for:

- exposing a unified API / CLI / SDK
- authentication, authorization, and tenant identification
- routing requests to the target tenant cell
- managing provisioning / upgrade / migration / decommission for tenant cells
- aggregating cross-tenant observability
- managing global quota, rate limiting, and billing dimensions

The global control plane does not directly hold the final business state of tenant resources.

### 6.2 Tenant Cell

The tenant cell is responsible for:

- resource tree and naming
- resource metadata and version state
- tenant-local durable queue
- semantic processing and index refresh
- tenant-local retrieval
- failure recovery and reconciliation

Each tenant cell is a failure domain that can be observed, recovered, and scaled independently.

### 6.3 Layered View

Logically, `dat9` adopts a six-layer architecture:

1. Global access and routing layer
2. Tenant-local resource and retrieval control layer
3. AGFS access plane layer
4. Tenant-local task and processing layer
5. Tenant-local shared state and index layer
6. Global operations and observability layer

---

## 7. Logical Layers

### 7.1 Global Access and Routing Layer

Primary responsibilities:

- expose a unified HTTP API / CLI / SDK
- receive resource import, directory browsing, search, and task query requests
- identify `tenant_id`
- route requests to the target `Tenant Cell`
- manage tenant configuration, quota, feature flags, and lifecycle

Design requirements:

- stateless or nearly stateless
- horizontally scalable
- must not directly depend on tenant-local physical table schemas
- must not directly execute tenant-local task logic

### 7.2 Tenant-Local Resource and Retrieval Control Layer

This is the main control plane within a single tenant, responsible for resource semantics, lifecycle orchestration, and retrieval strategy.

Primary responsibilities:

- define a unified resource identifier, such as `dat9://tenant/...`
- manage resource trees, directories, files, labels, relationships, and ownership
- handle resource creation, deletion, rename, move, soft delete, and restore
- manage resource versions, semantic versions, and index versions
- orchestrate resource operations into object writes, metadata commits, task dispatch, and compensation registration
- orchestrate keyword / metadata / vector / hybrid retrieval
- manage query rewrite, rerank, score fusion, and related logic

Design requirements:

- all resource operations must go through controlled orchestration
- commit points, compensation points, and recovery points must be defined
- version-based compare-and-set writeback must be supported
- retrieval strategy must remain independent of the concrete database backend

### 7.3 AGFS Access Plane Layer

In this design, AGFS is the unified access plane, not the high-level business control plane.

It provides two types of capability:

- `/local`: object/file access, mapped by `s3fs` to the tenant-scoped S3 namespace
- `/queue`: task access, mapped by tenant-scoped durable `queuefs` to task state in tenant-scoped TiDB

Primary responsibilities:

- hide low-level protocol details of the object store and task system from upper layers
- provide a consistent mount entry for object access and task access
- provide a basic observability view

Design requirements:

- AGFS does not handle high-level resource semantic orchestration
- AGFS is not responsible for cross-tenant scheduling
- `queuefs` in this layer represents the durable task queue within a tenant, not a globally shared queue fabric
- AGFS unifies the entry and mount facade, not the domain model of objects and tasks

### 7.4 Tenant-Local Task and Processing Layer

The task and processing layer is the asynchronous execution engine within a single tenant.

Primary responsibilities:

- claim tasks from tenant-scoped durable `queuefs`
- read object bodies or derived text through `/local`
- invoke Parser / LLM / Embedding / OCR / ASR / VLM and similar capabilities
- generate parsed results, summaries, overviews, and embeddings
- write processing results back to tenant-scoped TiDB and object storage
- perform the corresponding `ack / nack / renew / recover` task actions
- advance the resource state machine and index state machine

Design requirements:

- workers are stateless and horizontally scalable
- every task type has explicit input, output, retry, and timeout semantics
- lease renew / heartbeat must be supported
- version-based idempotent writeback must be supported
- stale task detection and stale writeback suppression must be supported

### 7.5 Tenant-Local Shared State and Index Layer

This layer is the state landing zone within a tenant and consists of that tenant's own `TiDB` and `S3`.

Primary responsibilities:

- S3 stores resource bodies and optionally large derived artifacts
- TiDB stores authoritative metadata, version state, vector records, task state, and necessary semantic summaries
- TiDB stores task state, consumption records, and failure records needed by durable `queuefs`
- TiDB executes tenant-local filtered queries, vector retrieval, sorting, and pagination

Design requirements:

- resource metadata, task state, and index state must be auditable
- rebuildable derived data and authoritative metadata must be layered separately
- independent recovery within a tenant must be supported

### 7.6 Global Operations and Observability Layer

Primary responsibilities:

- aggregate tenant-level metrics / logs / traces
- maintain tenant-level health, backlog, dead letter, cost, and throughput statistics
- support cross-tenant query and alerting

This layer does not directly execute tenant tasks, but it must be able to observe all tenant cells.

---

## 8. Module Relationship Diagram

### 8.1 Overall Relationship

```text
+--------------------------------------+
| Global Access & Control Plane        |
| API / Auth / Routing / Provisioning  |
+-------------------+------------------+
                    |
                    v
        +-----------------------------+
        |         Tenant Cell         |
        |                             |
        | +-------------------------+ |
        | | Resource & Retrieval    | |
        | | Control Layer           | |
        | +-----------+-------------+ |
        |             |               |
        |             v               |
        | +-------------------------+ |
        | | AGFS Access Plane       | |
        | | /local  /queue          | |
        | +-----+-----------+-------+ |
        |       |           |         |
        |       v           v         |
        |   Tenant S3   Tenant Queue  |
        |                 (queuefs)   |
        |                 on Tenant   |
        |                 TiDB        |
        |       ^           |         |
        |       |           v         |
        | +-----+-------------------+ |
        | | Task & Processing Layer | |
        | +-------------+-----------+ |
        |               |             |
        |               v             |
        |     Tenant TiDB Metadata    |
        |     / Versions / Vectors    |
        +-----------------------------+
                    |
                    v
+--------------------------------------+
| Global Observability & Operations    |
+--------------------------------------+
```

### 8.2 Write and Async Processing Relationship

```text
Client
  |
  v
Global Access Layer
  |
  +--> resolve tenant -> route to Tenant Cell
                                |
                                v
                    Resource Control Layer
                                |
                                +--> write object to /local -----------> Tenant S3
                                |
                                +--> commit resource + version -------> Tenant TiDB
                                |
                                +--> enqueue versioned task to /queue -> Tenant QueueFS
                                                                     |
                                                                     v
                                                                Worker dequeues
                                                                     |
                                   +---------------------------------+------------------+
                                   |                                                    |
                                   v                                                    v
                      read object/version from /local                         renew / ack / nack
                                   |                                                    |
                                   v                                                    v
                                Tenant S3                                          Queue state
                                   |
                                   v
               parse / summarize / overview / embed / refresh_index
                                   |
                                   +---------------------------> CAS write to Tenant TiDB
```

---

## 9. Positioning of the Queue and Processing Layer

This document explicitly positions `queuefs` as:

- **task execution infrastructure within a tenant**

It is responsible for:

- enqueue
- dequeue
- lease
- renew
- ack
- nack
- recover
- stats

It is not responsible for:

- global multi-tenant fair scheduling
- tenant routing
- the full state of complex DAG orchestration
- resource naming and permission models

Therefore:

- `queuefs` is the durable substrate for a single-tenant async runtime
- higher-level workflow dependencies, aggregation triggers, and version gates are additionally defined by the tenant-local control layer

---

## 10. Core Data Model

### 10.1 Primary Resource Table `resources`

Suggested fields:

- `resource_id`
- `tenant_id`
- `uri`
- `parent_uri`
- `resource_type`
- `object_key`
- `current_version`
- `content_digest`
- `size_bytes`
- `mime_type`
- `status`
- `created_at`
- `updated_at`
- `deleted_at`

Design constraints:

- `resource_id` is the authoritative logical identifier
- `uri` is logical naming and is not required to map one-to-one to `object_key`
- `object_key` should be as stable as possible and should not change frequently because of logical rename/move operations

### 10.2 Resource Version Table `resource_versions`

Suggested fields:

- `resource_version`
- `resource_id`
- `tenant_id`
- `object_key`
- `content_digest`
- `size_bytes`
- `mime_type`
- `version_state`
- `created_at`
- `created_by`

Purpose:

- serve as the input boundary for asynchronous tasks
- allow worker writeback to perform version validation
- support stale task discard and auditability

### 10.3 Semantic Material Table `resource_semantics`

Suggested fields:

- `semantic_id`
- `resource_id`
- `resource_version`
- `tenant_id`
- `semantic_level`, such as `summary` / `overview` / `detail`
- `storage_mode`, such as `inline` / `object_ref`
- `text_body` or `object_ref`
- `generator_type`
- `generator_version`
- `generated_at`

Purpose:

- store retrievable semantic material
- distinguish between small text stored inline and large text stored externally

### 10.4 Vector Record Table `resource_vectors`

Suggested fields:

- `vector_id`
- `tenant_id`
- `resource_id`
- `resource_version`
- `source_type`, such as `summary` / `overview` / `chunk`
- `source_id`
- `dense_vector`
- `sparse_vector` (optional)
- `embedding_model`
- `embedding_version`
- `created_at`

Notes:

- This document explicitly allows chunk-level vectors and resource-level vectors to coexist
- recall may happen at the chunk layer, while fusion completes at the resource layer

### 10.5 Resource Processing State Table `resource_processing_state`

Suggested fields:

- `resource_id`
- `tenant_id`
- `current_version`
- `parsed_version`
- `summarized_version`
- `overview_version`
- `indexed_version`
- `last_success_at`
- `last_error`
- `updated_at`

Purpose:

- avoid inferring index state solely from whether semantic/vector records exist
- provide an explicit basis for reconciliation and compensation

### 10.6 Task State Table

The primary task state is carried by the TiDB backend of tenant-scoped durable `queuefs`.

At minimum, tasks should express:

- `queued`
- `processing`
- `succeeded`
- `failed`
- `dead_lettered`

And should include at least the following field semantics:

- `task_id`
- `tenant_id`
- `task_type`
- `resource_id`
- `resource_version`
- `attempt_count`
- `leased_by`
- `lease_until`
- `last_heartbeat_at`
- `last_error`
- `created_at`
- `updated_at`

---

## 11. Durable QueueFS Semantic Requirements

This section does not rewrite the queue RFC. It defines `dat9`'s dependency on tenant-scoped durable `queuefs`.

### 11.1 Basic Requirements

`dat9` requires tenant-scoped `queuefs` to provide at least:

- `enqueue`
- `dequeue`
- `ack`
- `nack`
- `recover`
- `stats`
- `renew`, or an equivalent lease extension mechanism

### 11.2 Reliability Requirements

- at-least-once delivery
- lease / visibility timeout
- explicit ack
- timeout recovery
- retry / dead letter
- heartbeat / lease renew
- friendly to idempotent execution

### 11.3 Input Boundary Requirements

Each task must bind to explicit input:

- `resource_id`
- `resource_version`
- `task_type`
- `input_ref` or business parameters

### 11.4 Writeback Requirements

When writing results back, a worker must:

- validate that the task input version is still the target version, or
- allow writing as a historical-version record without advancing the current version state

### 11.5 Recovery Requirements

Recovery may only recover tasks that are:

- truly disconnected and whose lease has already expired

Long-running tasks must rely on `renew` / heartbeat to avoid being recovered incorrectly.

---

## 12. Write Flow

When a user uploads or updates a resource, the recommended flow is:

1. The global access layer receives the request and identifies `tenant_id`
2. Route to the target `Tenant Cell`
3. The tenant-local control layer allocates `resource_id` and a new `resource_version`
4. Write the object into the tenant's S3 namespace through AGFS `/local`
5. Commit the following in tenant TiDB:
   - current state update in `resources`
   - new version record in `resource_versions`
   - initial processing state
   - task outbox or equivalent compensation marker
6. Dispatch follow-up tasks such as `parse_resource` through AGFS `/queue`
7. If step 6 fails, redeliver through outbox / reconciliation
8. The API returns resource creation success

This flow requires:

- `resources` / `resource_versions` to be the control-layer commit point
- cross-system inconsistency between object write and task dispatch to be solved by compensation rather than strong distributed transactions
- the user to see a minimally visible resource immediately after upload succeeds

---

## 13. Semantic Processing Flow

### 13.1 Single-Resource Processing Chain

A typical chain includes:

1. `parse_resource`
2. `generate_summary`
3. `embed_resource`
4. `refresh_index`

Every step must:

- read a specific `resource_version`
- renew the lease periodically
- write a versioned result explicitly
- `ack` on success
- `nack` on failure

### 13.2 Aggregation Tasks

Tasks such as `generate_overview` and `reconcile_resource` are not simple linear jobs; they are aggregation tasks.

Their input boundary must be defined as:

- a snapshot version of a directory/collection, or
- an explicit version of an input set

They must not aggregate directly over a "current dynamic subtree" without a boundary.

### 13.3 Handling Stale Tasks

If a worker discovers that the `resource_version` bound to a task is no longer the current version, then it may:

- record the task as a business result such as `succeeded_stale`, or
- discard it safely and `ack`

But it must not advance an old result into the current state.

---

## 14. Retrieval Flow

Retrieval executes in three stages:

### 14.1 Query Preparation

- identify `tenant_id`
- route to the target `Tenant Cell`
- choose the retrieval mode: vector / keyword / hybrid
- generate query embedding if needed

### 14.2 Candidate Recall

- execute metadata filtering in tenant TiDB
- execute vector recall in tenant TiDB
- if hybrid is enabled, merge lexical and vector candidates
- support chunk-level recall followed by backtracking to the resource level

### 14.3 Result Fusion

- fuse directory overviews, resource summaries, and chunk hits in layers
- optionally weight by semantic score, freshness, popularity, and type priority
- optional reranking

If body content is needed, read the object from the tenant S3 namespace again through AGFS `/local`.

---

## 15. Multi-Tenant Design

### 15.1 Isolation Unit

The foundational unit of multi-tenant isolation in this document is not a shared task table plus a tenant field, but rather:

- one dedicated TiDB per tenant
- one dedicated durable `queuefs` per tenant
- one dedicated S3 namespace boundary per tenant

It is important to distinguish clearly between an independent namespace and a dedicated physical bucket:

- `TiDB` and durable `queuefs` are tenant-scoped independent state units
- on the `S3` side, the requirement is a tenant-scoped namespace boundary, not necessarily a physically separate bucket per tenant
- the default recommended shape is multiple tenants sharing the same bucket, with tenant-scoped namespaces formed using isolated prefixes under that bucket based on `tenant_id`
- under stronger compliance requirements, independent KMS, independent lifecycle policy, or stronger deletion-proof requirements, a tenant may also receive a dedicated bucket

Therefore, whenever this document refers to a "tenant-scoped S3 namespace," it should by default be understood as one of the following:

- a tenant-scoped prefix under a shared bucket
- a dedicated bucket
- another object storage unit that provides equivalent isolation guarantees

Regardless of which form is used, the following must hold:

- `tenant_id` is the hard boundary for object key planning, access control, audit, cost attribution, and garbage collection
- rename / move should preferably be implemented as metadata-layer changes rather than frequent physical renames in the object storage layer
- `object_key` should remain as stable as possible and should not cause frequent migration of large objects when logical names change

### 15.2 Responsibilities of the Global Control Plane

The global control plane must handle:

- tenant -> cell mapping
- tenant routing
- provisioning / migration / decommission
- cross-tenant observability and cost attribution

### 15.3 Tenant-Local Responsibilities

The tenant cell is responsible for:

- metadata consistency
- task recovery
- index rebuild
- single-tenant resource and task scheduling

### 15.4 What This Means

This means:

- `queuefs` no longer carries responsibility for global multi-tenant fair scheduling
- the multi-tenant scaling problem is transformed into a tenant-cell scaling and orchestration problem

### 15.5 Benefits Compared with OpenViking Local Queue Storage

Compared with the current single-machine local-persistence-oriented `queuefs` in OpenViking, this document defines durable `queuefs` as execution infrastructure backed by tenant-scoped `TiDB`.

The major benefits are:

- multiple AGFS / API / worker nodes can share the same tenant queue, rather than each binding to local queue state
- task state is externalized from a single-machine local disk into tenant-scoped infrastructure, which better supports instance replacement, process crash recovery, and continuing execution after node-level failure
- queue state lives together with tenant metadata, version state, and processing state in tenant `TiDB`, which makes reconciliation, auditing, stale task judgment, and versioned writeback control easier
- scaling, migration, decommission, and fault isolation become more natural when done on tenant boundaries, matching the operational boundary of the Tenant Cell
- backlog, lease, retry, dead letter, and similar states become easier to observe, count, and analyze operationally

It should be made clear that these benefits mainly target multi-node, multi-tenant, and standardized operations scenarios. For single-node deployment, the value of a `TiDB` backend is more about architectural scalability than simply replacing local persistence.

---

## 16. Consistency and Recovery

### 16.1 Consistency Between Objects and Metadata

Object writes to S3 and metadata writes to TiDB cannot naturally form a distributed transaction, so the system needs:

- a defined write order
- a defined commit point
- an outbox or compensation mechanism
- reconciliation tasks

### 16.2 Resource Version Consistency

The core of resource consistency is not "atomic commit across systems," but rather:

- tasks bind to versions
- writeback validates versions
- processing state explicitly records version advancement

### 16.3 Task Recovery

Any interrupted processing should be recovered through leased-task recovery in tenant-scoped durable `queuefs`, rather than relying on in-process state.

But:

- long-running tasks must prove they are still executing through heartbeat / renew
- recovery logic must avoid reclaiming long-running tasks incorrectly

### 16.4 Index Recovery

Index state should not be inferred from whether vector records exist. It should explicitly record:

- current source version
- current indexed version
- last successful indexing time

This allows the system to determine accurately whether a resource needs index rebuilding.

---

## 17. Authoritative State and Rebuildable State

Within a single `Tenant Cell`, authoritative state and rebuildable state must be clearly separated.

### 17.1 Authoritative State

The following state serves as the authoritative basis for system operation and recovery:

- `resources`, `resource_versions`, and `resource_processing_state`
- task state and execution state in durable `queuefs`
- raw objects in `S3` and large derived artifacts that are explicitly retained

Together, these states determine:

- what the current resource version is
- which asynchronous tasks have been submitted, are processing, succeeded, failed, or are recoverable
- which indexes and semantic derivatives correspond to the current version

### 17.2 Rebuildable State

The following state should in principle be treated as rebuildable derived data, not the unique source of truth:

- semantic derivatives such as resource summaries, directory overviews, and chunk text
- vector records and retrieval index material that can be recomputed
- other intermediate results produced by asynchronous flows

When such derived data is missing, stale, or corrupted, the system should preferentially rely on authoritative state to trigger reconciliation and rebuild, rather than treating the derived data itself as the sole basis for judgment.

---

## 18. Observability

### 18.1 Global Observability

The global control plane should aggregate:

- tenant request volume
- tenant-level backlog
- tenant-level dead letter
- tenant-level cost and throughput
- tenant-level health state

### 18.2 Tenant-Local Observability

The tenant cell should expose:

- `/local` object read/write counts
- `/queue` enqueue/dequeue/renew/ack/nack/recover counts
- queue depth / retry / dead letter
- processing latency for each task type
- embedding / rerank / retrieval latency

### 18.3 Correlation Keys

All logs, metrics, and audit records should preserve as much as possible:

- `tenant_id`
- `resource_id`
- `resource_version`
- `task_id`
- `semantic_version`
- `index_version`

These correlation keys are the foundation for fault diagnosis and auditability.

---

## 19. Deployment Recommendations

### 19.1 Global Layer

- multiple stateless API / gateway nodes
- an independent tenant routing / control service
- a global observability aggregation system

### 19.2 Tenant Layer

Each tenant should have at least:

- a tenant-scoped AGFS access plane
- a tenant-scoped TiDB
- a tenant-scoped durable `queuefs`
- a tenant-scoped S3 namespace, by default implemented as a `tenant_id`-isolated prefix under a shared bucket
- one or more tenant-scoped worker pools

### 19.3 Worker Model

Depending on the resource model, the system may choose:

- a dedicated worker pool per tenant
- a shared worker fleet routed by tenant into the target `queuefs`
- elastic workers started on demand

No matter which model is chosen, the following must be guaranteed:

- workers execute with the target tenant context
- task lease renewal and version validation are fully available

---

## 20. Design Summary

The core ideas of `layer_v3` can be summarized as follows:

- use a **Global Control Plane** to manage tenant routing, orchestration, quotas, and cross-tenant observability
- use **Tenant Cells** to carry resource state, asynchronous execution, and retrieval within a single tenant
- use **S3** to store raw objects and some large derived artifacts
- by default, S3 tenant isolation is represented as tenant-scoped prefixes under a shared bucket based on `tenant_id`; when stronger physical isolation is required, this can be upgraded to dedicated buckets
- use **TiDB** to store authoritative metadata, version state, index state, and task state
- use **Durable QueueFS** as the asynchronous execution substrate within a tenant, not as a globally shared task bus
- durable `queuefs` adopts a tenant-scoped `TiDB`-backed execution substrate to support multi-node sharing, tenant isolation, and recoverable operations
- use **version binding + CAS writeback + reconciliation** to keep the asynchronous system logically correct under at-least-once semantics

Ultimately, `dat9` is not a system that relies on a globally shared queue substrate to maintain task order for all tenants. It is a distributed context and retrieval platform that is:

- **tenant-cell-based as the isolation boundary**
- **version-based as the asynchronous consistency boundary**
- **built on a durable queue as tenant-local execution infrastructure**
- **centered on a global control plane for multi-tenant orchestration**

---

## 21. Established Principles and Protocols Still to Be Filled In

### 21.1 Established Principles

This document has already established the following principles to constrain the consistency boundary between resource lifecycle operations and asynchronous derived-data writeback:

- `resource_version` as the async input boundary
- version validation during worker writeback
- compare-and-set style state advancement
- stale task suppression
- aggregation tasks must bind to snapshot versions rather than read a dynamic subtree

Taken together, these principles express the following core judgment:

- the consistency boundary of the asynchronous system should be built on **versions**, not on the hope that "tasks happen to finish on time"

At the same time, this document also makes it clear that these principles alone are not yet sufficient to form a complete implementation specification, and several protocol details still need to be filled in.

### 21.2 Protocols Still Needed: Lifecycle Operations and Async Task Rules

The following still need to be defined more precisely:

- how `delete / soft delete / restore / rename / move` each affect older tasks that are still running
- whether logically deleted resources may keep historical-version writeback while being forbidden from advancing current state
- how the writeback target of an old task should be determined after a resource is moved or renamed

### 21.3 Protocols Still Needed: Landing Point and Granularity of Compare-and-Set

The following still need to be clarified:

- whether CAS applies to `resource_processing_state`, the pointer in `resources`, writes into `resource_semantics`, or a combination of these
- which writes are allowed to be retained as historical versions
- which state transitions must require that "the current version still matches"

### 21.4 Protocols Still Needed: Snapshot Generation for Aggregation Tasks

The following still need to be defined more precisely:

- how directory/collection snapshot versions are generated
- how child add/delete/update operations advance the parent snapshot version
- whether aggregation tasks such as `generate_overview` are triggered by events, batch processing, or reconciliation

### 21.5 Protocols Still Needed: Whether Additional Lifecycle Fencing Primitives Are Required

This document leans toward using:

- version binding
- CAS writeback
- stale suppression

instead of a heavier global lock model.

But it still needs to clarify:

- which scenarios can be handled by version validation alone
- which scenarios still require lightweight fencing, short-term mutual exclusion, or explicit tombstone protection

### 21.6 Follow-Up Requirement

The issues above do not change the overall architectural direction of this document, but they directly affect:

- the semantic boundary of lifecycle operations
- the worker writeback protocol
- aggregation task correctness
- recoverable behavior after delete/move

Therefore, this group of issues must be refined further in follow-up documents. At least one dedicated RFC is recommended, such as:

- `resource-lifecycle-and-async-writeback-consistency.md`

Before that RFC is finalized, this document can be regarded as:

- having defined the overall architecture, responsibility boundaries, and key consistency principles
- but not yet having formed a complete implementation protocol for resource lifecycle and aggregation snapshot behavior
