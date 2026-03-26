# RFC: dat9 System Architecture

## 1. Goal

This RFC defines the top-level architecture of dat9.

It establishes:

- system layers
- control-plane versus tenant-local responsibilities
- the role of AGFS
- the role of `db9`, `S3`, and durable `queuefs`
- tenant isolation and deployment assumptions

This RFC is normative for system structure, but not for low-level storage schemas or API details.

## 2. Non-goals

This RFC does not define:

- detailed path/file schemas
- exact task state machine fields
- exact write path transaction shapes
- retrieval algorithms in detail
- complete API request/response contracts

Those are defined in subsystem RFCs.

## 3. Definitions

- **Global Control Plane**: the globally shared layer responsible for auth, tenant routing, provisioning, quota, and fleet-level observability
- **Tenant Cell**: the per-tenant isolation, recovery, and execution unit
- **AGFS Access Plane**: the filesystem-like access surface used to expose object/file access and queue access
- **db9 State Plane**: the tenant-local backend for small-file content, metadata, retrieval indexes, vector data, and async task state
- **S3 Namespace**: the tenant-scoped object storage namespace used for large-file content
- **durable queuefs**: the queuefs interface backed by tenant-local durable task state, used for async task execution
- **file**: the user-visible path-addressable item in the filesystem-like namespace
- **logical object**: the internal content identity behind one or more file paths
- **resource**: the internal versioned processing unit used for async correctness and derived-state progression
- **derived artifact**: any output generated from resource processing
- **semantic artifact**: a user-visible derived artifact such as `.abstract.md`, `.overview.md`, or `.relations.json`

## 4. Design

### 4.1 System shape

dat9 is organized as a global control plane plus Tenant Cells.

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
|  +-----------------------------------------------+   |
|  | Resource & Retrieval Control Layer            |   |
|  | naming | lifecycle | versioning | retrieval   |   |
|  +-------------------+---------------------------+   |
|                      |                               |
|                      v                               |
|  +-----------------------------------------------+   |
|  | AGFS Access Plane                             |   |
|  | /local -> object/file access                  |   |
|  | /queue -> durable task queue access           |   |
|  +-----------+-------------------+---------------+   |
|              |                   |                   |
|              v                   v                   |
|      +---------------+   +-------------------+       |
|      | S3 Namespace  |   | durable queuefs   |       |
|      | large objects |   | (db9 backend)     |       |
|      +---------------+   +-------------------+       |
|                      \         /                     |
|                       \       /                      |
|                        v     v                       |
|                  +------------------+               |
|                  | db9 State Plane  |               |
|                  | small files      |               |
|                  | metadata         |               |
|                  | versions         |               |
|                  | vectors/indexes  |               |
|                  | task state       |               |
|                  +------------------+               |
|                             ^                       |
|                             |                       |
|                  +------------------------+         |
|                  | Worker / Async Runtime |         |
|                  +------------------------+         |
+------------------------------------------------------+
```

### 4.2 Global Control Plane responsibilities

The Global Control Plane is responsible for:

- authentication and authorization entry points
- tenant identification and request routing
- tenant provisioning and lifecycle management
- quota, rate-limiting, and fleet-level policy enforcement
- global observability aggregation

The Global Control Plane does not own tenant business state.

### 4.3 Tenant Cell responsibilities

Each Tenant Cell is responsible for:

- path and resource naming within the tenant
- versioned resource state
- semantic derivation and retrieval state
- tenant-local async execution
- failure recovery and reconcile

Each Tenant Cell is a failure domain and an operational boundary.

### 4.4 AGFS role

AGFS is the unified access plane.

It provides:

- `/local` for object/file access
- `/queue` for durable queue access
- one consistent access surface for upper layers

AGFS and queuefs should be treated as the canonical access contract and integration surface.
They do not imply that every internal runtime component must literally use filesystem-shaped operations if a direct runtime interface is clearer for workers.

AGFS does not define:

- lifecycle correctness
- writeback correctness
- semantic derivation rules
- task orchestration semantics by itself

### 4.5 Backend roles

The core backend roles are:

- `db9`
  - small-file content
  - metadata
  - resource/version state
  - retrieval indexes
  - vector data
  - async task state
- `S3`
  - large-file content only
- durable `queuefs`
  - async task execution substrate
  - exposed via AGFS-style queuefs interface
  - backed by tenant-local `db9`

### 4.6 Layered responsibility map

The old layered design is still useful as an implementation map.

| Layer | Owns | Does not own |
| --- | --- | --- |
| Global Access and Control Plane | auth, tenant routing, provisioning, quota, fleet policy | tenant business truth, tenant-local version state |
| Tenant-local Resource and Retrieval Control Layer | naming, lifecycle, versioning, retrieval planning, aggregate-trigger decisions | raw object bytes, global rate limits |
| AGFS Access Plane | `/local` and `/queue` access surface, path/queue entry normalization, integration boundary | async correctness rules, version policy |
| Tenant-local Task and Processing Layer | dequeue, lease, renew, ack/nack, retries, worker execution | path naming, auth, global fairness |
| Tenant-local Shared State and Index Layer | metadata, version state, semantic state, vector/index state, task state | global routing and provisioning |
| Global Observability and Operations | fleet view, tenant health aggregation, cost attribution | tenant commit path semantics |

This map matters because the new RFC set splits concerns by document, but implementation still needs one place that says which layer owns which decisions.

### 4.7 Representative write and async processing relationship

The critical synchronous-to-asynchronous handoff should look like this:

```text
Client
  |
  v
Global Access Layer
  |
  +--> authenticate tenant -> route to Tenant Cell
                                      |
                                      v
                         Resource Control Layer
                                      |
                     +----------------+----------------+
                     |                                 |
                     v                                 v
            write/read through /local          commit metadata/version
                     |                                 |
           +---------+---------+                       |
           |                   |                       |
           v                   v                       v
      small body in db9   large object in S3   enqueue durable task
                                                      or durable
                                                      reconcile marker
                                                           |
                                                           v
                                                     Async Runtime
                                                           |
                                      +--------------------+--------------------+
                                      |                                         |
                                      v                                         v
                           read versioned input                         renew / ack / nack
                           from db9 or S3                               update task state
                                      |                                         |
                                      +--------------------+--------------------+
                                                           |
                                                           v
                                          parse / summarize / overview / embed
                                                           |
                                                           v
                                         version-aware writeback into db9 state
```

The architectural invariant is:

- user-visible commit happens before async derivation finishes
- the handoff from committed state to async work must be durable or recoverable
- workers always read and write against tenant-local authoritative state

### 4.8 Operational boundary and observability

The old design also carried an important operational distinction: global and tenant-local observability are different layers.

Global observability should aggregate at least:

- tenant request volume
- tenant backlog and dead-letter counts
- tenant cost and throughput
- tenant health and provisioning state

Tenant-local observability should expose at least:

- `/local` read/write counts and latency
- `/queue` enqueue/dequeue/renew/ack/nack/recover counts
- per-task-type processing latency
- retrieval, embedding, and indexing latency

Correlation keys should be preserved across logs, metrics, and audit events whenever possible:

- `tenant_id`
- `resource_id`
- `resource_version`
- `task_id`
- `semantic_version`
- `index_version`

### 4.9 Deployment shape and worker placement

The tenant-cell abstraction allows multiple deployment shapes without changing the semantic model.

Representative shape:

- multiple stateless API / gateway nodes in front of the control plane
- durable control-plane metadata for routing and provisioning
- tenant-scoped `db9` state, tenant-scoped object namespace, and tenant-scoped queue runtime
- one or more worker pools, either tenant-dedicated or a shared fleet that executes with explicit tenant context

No matter which worker placement model is used, the following must hold:

- workers execute against the target tenant context
- task lease renewal and recovery remain available
- version-aware writeback rules do not depend on one specific process staying alive

## 5. Invariants / Correctness Rules

- external filesystem-like simplicity must not erase internal versioned state
- tenant-local state and tenant-local execution must remain isolated by Tenant Cell
- AGFS must remain an access plane, not the only semantic model of the system
- async work must be durable and recoverable
- large-file storage must remain tenant-scoped at the object-namespace level

In other words:

- everything user-relevant should be inspectable through the file-like interface when practical
- not every internal correctness state must be modeled as a user-visible file row

## 6. Failure / Recovery

The architecture assumes failure and recovery are normal system behavior.

- worker failure must not lose durable task state
- interrupted writes must be recoverable or reconcilable
- derived state must be rebuildable from authoritative state
- tenant-local failures should remain tenant-local whenever possible

Detailed rules are defined in:

- `resource-versioning-and-async-correctness.md`
- `durable-queue-runtime.md`
- `write-path-and-reconcile.md`

## 7. Open Questions

- whether a shared worker fleet should be a common deployment mode or an optional scaling optimization
- how much deployment flexibility should be allowed while preserving the tenant-cell abstraction

## 8. References / Dependencies

- `docs/overview.md`
- `docs/design/control-plane-and-provisioning.md`
- `docs/design/storage-and-namespace.md`
- `docs/design/semantic-derivation-and-retrieval.md`
- `docs/design/queuefs-durable-task-queue-rfc.md`
- `docs/design/durable-queue-runtime.md`
- `docs/design/write-path-and-reconcile.md`
- `docs/design/api-and-ux-contract.md`
