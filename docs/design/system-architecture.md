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
- `docs/design/queuefs-durable-task-queue-rfc.md`
- `docs/design/api-and-ux-contract.md`
