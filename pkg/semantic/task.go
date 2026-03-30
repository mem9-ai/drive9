// Package semantic defines durable task types for async semantic processing.
package semantic

import (
	"errors"
	"time"
)

// TaskType identifies the durable semantic work to execute.
type TaskType string

const (
	// TaskTypeEmbed generates or refreshes file embeddings.
	TaskTypeEmbed TaskType = "embed"
)

// TaskStatus describes the delivery state of a semantic task.
type TaskStatus string

const (
	TaskQueued       TaskStatus = "queued"
	TaskProcessing   TaskStatus = "processing"
	TaskSucceeded    TaskStatus = "succeeded"
	TaskFailed       TaskStatus = "failed"
	TaskDeadLettered TaskStatus = "dead_lettered"
)

var (
	// ErrTaskNotFound reports that the requested task row does not exist.
	ErrTaskNotFound = errors.New("semantic task not found")
	// ErrTaskLeaseMismatch reports that the supplied receipt is stale, wrong, or expired.
	ErrTaskLeaseMismatch = errors.New("semantic task lease mismatch")
)

// Task is a durable semantic task persisted in the tenant data plane.
type Task struct {
	TaskID          string
	TaskType        TaskType
	ResourceID      string
	ResourceVersion int64
	Status          TaskStatus
	AttemptCount    int
	MaxAttempts     int
	Receipt         string
	LeasedAt        *time.Time
	LeaseUntil      *time.Time
	AvailableAt     time.Time
	PayloadJSON     []byte
	LastError       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	CompletedAt     *time.Time
}

// ClaimResult returns one claimed task when the queue has work available.
type ClaimResult struct {
	Task  Task
	Found bool
}
