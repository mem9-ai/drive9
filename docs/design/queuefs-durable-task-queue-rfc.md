# RFC: Upgrading the Current `./agfs` `queuefs` into a Durable Task Queue

## 1. Overview

This document proposes an upgrade plan based on the current `./agfs/agfs-server/pkg/plugins/queuefs` implementation, evolving `queuefs` from a "filesystem-style FIFO message queue" into a "durable task queue for asynchronous job processing."

The goal is not to replace the filesystem interface of `queuefs`, but to preserve the existing `/queue/<name>/...` usage pattern as much as possible while adding the state machine, lease, acknowledgment, recovery, and observability semantics required by a reliable task queue on top of the TiDB backend.

This RFC is grounded in the current source code and focuses on the following facts:

- The existing `queuefs` already exposes a directory-based queue model: `enqueue`, `dequeue`, `peek`, `size`, `clear`
- The current TiDB backend uses one table per queue plus a `queuefs_registry` table
- The current `Dequeue()` selects the first non-deleted message inside a transaction and immediately marks it as `deleted = 1`
- The current implementation lacks reliable task semantics such as `processing`, `ack`, `lease timeout`, `retry`, and `dead letter`

Therefore, the core of this RFC is: **without giving up the filesystem interface of `queuefs`, upgrade the TiDB backend from "dequeue means delete" to a task system where "dequeue means lease, success requires ack, and failure is recoverable."**

---

## 2. Summary of the Current Implementation

### 2.1 Current Interface Model

The current `queuefs` exposes each logical queue as a directory containing the following fixed entries:

- `enqueue`
- `dequeue`
- `peek`
- `size`
- `clear`

Relevant documentation and implementation entry:

- `agfs/agfs-server/pkg/plugins/queuefs/queuefs.go:31`

### 2.2 Current TiDB Backend Model

The key structures of the TiDB backend are:

- `queuefs_registry(queue_name, table_name)`
- One dedicated message table per queue: `queuefs_queue_<name>`

Table creation logic:

- `agfs/agfs-server/pkg/plugins/queuefs/db_backend.go:202`
- `agfs/agfs-server/pkg/plugins/queuefs/db_backend.go:245`

The current message table mainly includes:

- `id`
- `message_id`
- `data`
- `timestamp`
- `created_at`
- `deleted`
- `deleted_at`

### 2.3 Current `dequeue` Semantics

The current `Dequeue()` flow is:

1. Start a transaction
2. `SELECT id, data FROM <table> WHERE deleted = 0 ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`
3. Immediately `UPDATE ... SET deleted = 1, deleted_at = CURRENT_TIMESTAMP WHERE id = ?`
4. Commit the transaction

Implementation reference:

- `agfs/agfs-server/pkg/plugins/queuefs/backend.go:348`

This means the current behavior is:

- Once a consumer successfully receives a message, it logically disappears
- If the consumer crashes afterward, the system cannot recover that message

---

## 3. Problem Statement

If `queuefs` is used for asynchronous task processing, the current implementation has the following structural gaps:

### 3.1 No `processing` State

Today there are only:

- `deleted = 0` -> still in queue
- `deleted = 1` -> consumed

Missing states include:

- `leased/processing`
- `succeeded`
- `failed`
- `dead_lettered`

### 3.2 No `ack` Mechanism

There is currently no explicit `ack(task_id)`. `dequeue` itself is equivalent to permanent consumption.

### 3.3 No Crash Recovery

There is currently no support for:

- `lease_until`
- `processing_timeout`
- `RecoverStale()`

As a result, tasks are lost when a worker crashes.

### 3.4 No Retry or Dead Letter Semantics

There is currently no support for:

- `attempt_count`
- `max_attempts`
- `last_error`
- `next_retry_at`
- dead letter queue / dead letter table

### 3.5 `size` Semantics Are Too Coarse

The current `size` only counts messages where `deleted = 0`, so it cannot answer questions such as:

- How many tasks are currently queued
- How many tasks are currently processing
- How many tasks have failed

### 3.6 The Current Model Behaves More Like a Message Queue Than a Task Queue

If the goal is only short-lived FIFO message passing, the current implementation is barely adequate. But if it needs to carry durable async tasks, an explicit task state machine is required.

---

## 4. Upgrade Goals

The upgraded `queuefs` TiDB backend should satisfy the following goals:

### 4.1 Reliable Consumption

- `dequeue` should no longer permanently delete a task immediately
- The task should first enter `processing` / leased state
- It should only enter a completed state after `ack`

### 4.2 Crash Recovery

- If a worker crashes, timed-out tasks should be automatically or explicitly re-queued
- Stale processing tasks should be recoverable

### 4.3 Retry Control

- Support attempt counts
- Support delayed retry
- Support dead letter isolation

### 4.4 Compatibility with the Filesystem Interface

Preserve the existing `/queue/<name>/...` interaction model as much as possible, only adding control files where necessary.

### 4.5 Preserve FIFO as the Primary Semantic

By default, tasks should still be consumed in enqueue order, while allowing lease and retry policies to locally interrupt strict FIFO when necessary.

---

## 5. Overall Approach

### 5.1 High-Level Idea

Instead of treating a task as "deleted on dequeue," split the task lifecycle into the following states:

- `queued`
- `processing`
- `succeeded`
- `failed`
- `dead_lettered`

Corresponding rules:

- `enqueue`: create a `queued` task
- `dequeue`: atomically move one `queued` task into `processing`
- `ack`: move `processing` to `succeeded`
- `nack/retry`: move `processing` back to `queued` or into `failed`
- `recover`: scan timed-out `processing` tasks and re-queue them

### 5.2 Keep the `queuefs` Directory Model

Retain the current entries:

- `enqueue`
- `dequeue`
- `peek`
- `size`
- `clear`

Recommended new control files:

- `ack`: write a task ID to confirm completion
- `nack`: write a task ID plus error information to trigger failure or retry
- `recover`: trigger stale task recovery
- `stats`: return task-state statistics

That would make a durable task queue directory look like this:

```text
/queue/tasks/
  enqueue
  dequeue
  peek
  size
  stats
  ack
  nack
  recover
  clear
```

---

## 6. Data Model Changes

### 6.1 Keep the Queue Registry Structure

Still retain:

- `queuefs_registry(queue_name, table_name)`

Reasons:

- The current implementation is already built around one table per queue
- It remains compatible with the existing `ListQueues`, `CreateQueue`, and `RemoveQueue` logic

### 6.2 New Schema for Queue Tables

The current message table should be upgraded from:

- the `deleted` model

to:

- an explicit task state machine model

Recommended schema:

```sql
CREATE TABLE IF NOT EXISTS queuefs_queue_<name> (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  message_id VARCHAR(64) NOT NULL,
  data LONGBLOB NOT NULL,
  timestamp BIGINT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  status VARCHAR(32) NOT NULL DEFAULT 'queued',
  attempt_count INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 16,

  leased_by VARCHAR(255) NULL,
  leased_at TIMESTAMP NULL,
  lease_until TIMESTAMP NULL,

  acked_at TIMESTAMP NULL,
  failed_at TIMESTAMP NULL,
  dead_lettered_at TIMESTAMP NULL,
  last_error TEXT NULL,
  next_retry_at TIMESTAMP NULL,

  dedupe_key VARCHAR(255) NULL,
  priority INT NOT NULL DEFAULT 0,

  INDEX idx_status_id (status, id),
  INDEX idx_status_retry_id (status, next_retry_at, id),
  INDEX idx_lease_until (status, lease_until),
  INDEX idx_dedupe_key (dedupe_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 6.3 Why the `deleted` Model Should Not Be Kept

`deleted` is better suited for a one-dimensional question: "is it still in the queue?" A durable task queue must explicitly distinguish:

- processing
- succeeded
- retryable failure
- non-retryable failure

So `status` is a necessary upgrade.

---

## 7. Filesystem Interface Changes

### 7.1 `enqueue`

Keep the current semantics: writing creates a task.

Recommended enhancements:

- Support JSON payloads
- Support optional fields:
  - `id`
  - `data`
  - `max_attempts`
  - `dedupe_key`
  - `priority`

If the input is not JSON, preserve backward compatibility by wrapping the raw string as task data using default task fields.

### 7.2 `dequeue`

New semantics:

- On read, select one executable task
- Atomically change `status` from `queued` to `processing`
- Set:
  - `attempt_count = attempt_count + 1`
  - `leased_at = now`
  - `lease_until = now + lease_duration`
  - `leased_by = consumer_id`

Suggested response payload:

```json
{
  "id": "...",
  "data": "...",
  "timestamp": "...",
  "attempt": 3,
  "lease_until": "..."
}
```

### 7.3 `peek`

Keep it as a side-effect-free read, but only look at tasks where:

- `status = queued`
- and `next_retry_at IS NULL OR next_retry_at <= now`

### 7.4 `ack`

Add a new write-only file `ack`.

The minimum payload is:

```json
{"id": "task-id"}
```

Processing logic:

- Only allow it for tasks in `processing`
- On success, update:
  - `status = succeeded`
  - `acked_at = now`
  - `lease_until = NULL`

### 7.5 `nack`

Add a new write-only file `nack`.

Suggested payload:

```json
{
  "id": "task-id",
  "error": "...",
  "retry": true,
  "retry_after_seconds": 30
}
```

Processing logic:

- If `retry = true` and `attempt_count < max_attempts`
  - `status = queued`
  - `next_retry_at = now + retry_after`
  - clear lease fields
- If the attempt limit has been reached
  - `status = dead_lettered`
  - `dead_lettered_at = now`
- If `retry = false`
  - `status = failed`

### 7.6 `recover`

Add a new write-only file `recover`.

Purpose:

- Restore tasks where `status = processing AND lease_until < now` back to `queued`
- Clear lease fields
- Optionally record recovery count or recovery logs

### 7.7 `stats`

Add a new read-only file `stats`, returning:

```json
{
  "queued": 12,
  "processing": 3,
  "succeeded": 1024,
  "failed": 4,
  "dead_lettered": 2
}
```

### 7.8 `size`

To preserve backward compatibility, `size` should continue to mean:

- the number of currently consumable tasks, i.e. tasks in `queued`

More detailed status counts should be handled by `stats`.

---

## 8. Backend Interface Changes

### 8.1 Gaps in the Current Interface

The current `QueueBackend` only covers:

- `Enqueue`
- `Dequeue`
- `Peek`
- `Size`
- `Clear`
- `ListQueues`
- `GetLastEnqueueTime`
- `CreateQueue`
- `RemoveQueue`
- `QueueExists`

To support a durable task queue, it needs to be extended.

### 8.2 Proposed New Interface

Suggested addition or replacement:

```go
type QueueBackend interface {
    Initialize(cfg map[string]interface{}) error
    Close() error
    GetType() string

    Enqueue(queueName string, task QueueTask) error
    Dequeue(queueName string, consumerID string, leaseDuration time.Duration) (QueueTask, bool, error)
    Peek(queueName string) (QueueTask, bool, error)

    Ack(queueName string, taskID string) error
    Nack(queueName string, taskID string, lastError string, retry bool, retryAfter time.Duration) error
    RecoverStale(queueName string, now time.Time) (int, error)

    Size(queueName string) (int, error)
    Stats(queueName string) (QueueStats, error)
    Clear(queueName string) error

    ListQueues(prefix string) ([]string, error)
    GetLastEnqueueTime(queueName string) (time.Time, error)
    CreateQueue(queueName string) error
    RemoveQueue(queueName string) error
    QueueExists(queueName string) (bool, error)
}
```

### 8.3 New Task Structure

Recommended addition:

```go
type QueueTask struct {
    ID          string    `json:"id"`
    Data        string    `json:"data"`
    Timestamp   time.Time `json:"timestamp"`
    Attempt     int       `json:"attempt,omitempty"`
    MaxAttempts int       `json:"max_attempts,omitempty"`
    DedupeKey   string    `json:"dedupe_key,omitempty"`
    Priority    int       `json:"priority,omitempty"`
    LeaseUntil  time.Time `json:"lease_until,omitempty"`
}
```

---

## 9. TiDB Backend Behavior Changes

### 9.1 New `dequeue` SQL

The core `dequeue` behavior should change from:

- `deleted = 0` -> `deleted = 1`

to:

- `status = 'queued'` -> `status = 'processing'`

Suggested flow:

1. Start a transaction
2. Select the first task that satisfies:
   - `status = 'queued'`
   - `next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP`
3. Use `FOR UPDATE SKIP LOCKED`
4. Update it to:
   - `status = 'processing'`
   - `attempt_count = attempt_count + 1`
   - `leased_at = CURRENT_TIMESTAMP`
   - `lease_until = CURRENT_TIMESTAMP + INTERVAL ? SECOND`
   - `leased_by = ?`
5. Commit the transaction

### 9.2 `ack` SQL

```sql
UPDATE <table>
SET status = 'succeeded',
    acked_at = CURRENT_TIMESTAMP,
    lease_until = NULL,
    leased_at = NULL,
    leased_by = NULL
WHERE message_id = ? AND status = 'processing'
```

### 9.3 `nack/retry` SQL

Retry path:

```sql
UPDATE <table>
SET status = 'queued',
    last_error = ?,
    next_retry_at = CURRENT_TIMESTAMP + INTERVAL ? SECOND,
    lease_until = NULL,
    leased_at = NULL,
    leased_by = NULL
WHERE message_id = ? AND status = 'processing'
```

Dead letter path:

```sql
UPDATE <table>
SET status = 'dead_lettered',
    dead_lettered_at = CURRENT_TIMESTAMP,
    last_error = ?,
    lease_until = NULL,
    leased_at = NULL,
    leased_by = NULL
WHERE message_id = ? AND status = 'processing'
```

### 9.4 `recover stale` SQL

```sql
UPDATE <table>
SET status = 'queued',
    lease_until = NULL,
    leased_at = NULL,
    leased_by = NULL
WHERE status = 'processing' AND lease_until < CURRENT_TIMESTAMP
```

---

## 10. HandleFS Semantic Requirements

The current `queuefs` already implements `HandleFS`, which is an important foundation for upgrading it into a durable task queue.

Design requirements:

- For `dequeue`, perform the real dequeue operation on the first read from the handle
- Subsequent reads on the same handle should read only the cached result and must not claim another task
- For `ack` / `nack`, execution should ideally be atomic when the single write handle is closed

This avoids:

- repeated dequeue on the same open handle
- partially written `ack/nack` JSON triggering error-state transitions too early

---

## 11. Compatibility Strategy

### 11.1 Backward Compatibility for Base Control Files

Keep the current entries:

- `enqueue`
- `dequeue`
- `peek`
- `size`
- `clear`

Existing clients can still run, except that `dequeue` will no longer imply permanent completion.

### 11.2 Add a New Durable Mode Configuration

Recommended config:

```toml
[plugins.queuefs.config]
backend = "tidb"
mode = "task"
lease_seconds = 300
max_attempts = 16
enable_dead_letter = true
```

Where:

- `mode = "message"`: keep the old semantics
- `mode = "task"`: enable durable task queue semantics

This allows smooth migration.

### 11.3 Data Migration Strategy

For existing TiDB backend queue tables, the recommendation is:

- Detect missing columns at startup
- Automatically run `ALTER TABLE` to add missing columns
- Map `deleted = 0` to `status = 'queued'`
- Map `deleted = 1` to `status = 'succeeded'`

---

## 12. Observability Design

To make the task system operable, add the following metrics:

- `queued` count per queue
- `processing` count per queue
- `dead_lettered` count per queue
- dequeue success count
- ack success count
- nack/retry count
- stale recovery count
- average processing duration
- maximum lease timeout count

At the same time, the `stats` file should return structured JSON so it can be collected easily by shell tooling and upper-layer systems.

---

## 13. Non-Goals

This RFC does not attempt to solve the following in this round:

- cross-queue priority scheduling
- multi-tenant queue permission models
- full-featured cron / delayed task scheduling
- exactly-once semantics

The goal of this iteration is: "at-least-once + crash recovery + explicit task state machine."

---

## 14. Implementation Recommendations

Implementation is recommended in three phases:

### Phase 1: Schema + Backend Upgrade

- Add `status`, `attempt_count`, `lease_until`, and related fields to the TiDB backend
- Implement `Ack`, `Nack`, `RecoverStale`, and `Stats`
- Change `Dequeue` to lease semantics

### Phase 2: Filesystem Interface Upgrade

- Add `ack`, `nack`, `recover`, and `stats` to the `queuefs` directory model
- Update `ReadDir`, `Stat`, `Read`, and `Write`
- Update the README

### Phase 3: Compatibility + Observability

- Add `mode = message/task`
- Add automatic schema migration
- Add metrics and log fields

---

## 15. Conclusion

Based on the current `./agfs` `queuefs`, the minimal but critical upgrade direction is not simply "add a few helper APIs," but rather:

- upgrade the queue backend from the `deleted` model to an explicit task state machine
- change `dequeue` from "consume and delete" to "claim a lease"
- add `ack` / `nack` / `recover` / `stats`
- persist the task lifecycle in TiDB instead of storing only FIFO message order

Once these changes are complete, `queuefs + TiDB` is no longer just a message queue with a filesystem facade. It becomes infrastructure that can serve as a durable task queue for asynchronous job processing.
