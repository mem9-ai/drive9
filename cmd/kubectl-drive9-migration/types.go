package main

import (
	"encoding/json"
	"time"
)

const (
	fixedSelector     = "app.kubernetes.io/name=drive9-migration,app.kubernetes.io/component=worker"
	instanceLabel     = "app.kubernetes.io/instance"
	workerContainer   = "drive9-migration"
	workerBinary      = "/drive9-migration"
	missingBatch      = "<missing>"
	maxConcurrentExec = 8
	maxErrorLength    = 512
)

const defaultExecTimeout = 10 * time.Second

type podList struct {
	Items []pod `json:"items"`
}

type pod struct {
	Metadata podMetadata `json:"metadata"`
	Spec     podSpec     `json:"spec"`
	Status   podStatus   `json:"status"`
}

type podMetadata struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	Labels            map[string]string `json:"labels"`
	DeletionTimestamp *string           `json:"deletionTimestamp"`
}

type podSpec struct {
	NodeName   string         `json:"nodeName"`
	Containers []podContainer `json:"containers"`
}

type podContainer struct {
	Name string `json:"name"`
}

type podStatus struct {
	Phase string `json:"phase"`
}

type workerStatus struct {
	VolumeID         string             `json:"volume_id"`
	NodeName         string             `json:"node_name,omitempty"`
	SpaceRef         string             `json:"space_ref,omitempty"`
	Prefix           string             `json:"prefix,omitempty"`
	Phase            string             `json:"phase"`
	StartupPhase     string             `json:"startup_phase"`
	RecoveryComplete bool               `json:"recovery_complete"`
	Round            workerRound        `json:"round"`
	Conditions       workerConditions   `json:"conditions"`
	SourceCount      int                `json:"source_count"`
	CandidateCounts  workerCandidates   `json:"candidate_counts"`
	FindingCounts    map[string]int     `json:"finding_counts"`
	GraceCandidates  int                `json:"grace_candidates"`
	CASRetry         int                `json:"cas_retry"`
	Backlog          int                `json:"backlog"`
	PendingRepairs   int                `json:"pending_repairs"`
	InFlight         int                `json:"in_flight"`
	Verification     workerVerification `json:"full_verification"`
	FenceIntent      bool               `json:"fence_intent"`
	FenceComplete    bool               `json:"fence_complete"`
	AttentionReason  string             `json:"attention_reason,omitempty"`
}

type workerRound struct {
	ID           string    `json:"id,omitempty"`
	Mode         string    `json:"mode,omitempty"`
	StartedAt    time.Time `json:"started_at,omitempty"`
	CompletedAt  time.Time `json:"completed_at,omitempty"`
	ScanComplete bool      `json:"scan_complete"`
	Converged    bool      `json:"round_converged"`
	FailureClass string    `json:"failure_class,omitempty"`
}

type workerConditions struct {
	ReadyForRollout  bool `json:"ready_for_rollout"`
	CurrentConverged bool `json:"current_converged"`
	Attention        bool `json:"attention"`
}

type workerCandidates struct {
	Mtime              int `json:"mtime"`
	SourceTokenChanged int `json:"source_token_changed"`
	NewPath            int `json:"new_path"`
	Filtered           int `json:"filtered"`
}

type workerVerification struct {
	Status        string    `json:"status,omitempty"`
	RequestedAt   time.Time `json:"requested_at,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	SourceCount   int64     `json:"source_count"`
	MismatchCount int64     `json:"mismatch_count"`
}

type jobResult struct {
	Namespace     string          `json:"namespace"`
	Batch         string          `json:"batch"`
	Pod           string          `json:"pod"`
	Node          string          `json:"node,omitempty"`
	PodPhase      string          `json:"pod_phase,omitempty"`
	JobID         string          `json:"job_id,omitempty"`
	Phase         string          `json:"phase,omitempty"`
	DisplayStatus string          `json:"status"`
	Error         string          `json:"error,omitempty"`
	Worker        json.RawMessage `json:"worker_status,omitempty"`

	parsed           *workerStatus
	collectionFailed bool
}

type batchSummary struct {
	Namespace    string `json:"namespace"`
	Batch        string `json:"batch"`
	Status       string `json:"status"`
	ObservedJobs int    `json:"observed_jobs"`
	Available    int    `json:"available_jobs"`
	Attention    int    `json:"attention_jobs"`
	Unavailable  int    `json:"unavailable_jobs"`
}

type queryScope struct {
	Namespace     string `json:"namespace,omitempty"`
	AllNamespaces bool   `json:"all_namespaces"`
	Batch         string `json:"batch,omitempty"`
	Context       string `json:"context,omitempty"`
}

type aggregateOutput struct {
	GeneratedAt time.Time      `json:"generated_at"`
	Scope       queryScope     `json:"scope"`
	Batches     []batchSummary `json:"batches"`
	Jobs        []jobResult    `json:"jobs"`
}
