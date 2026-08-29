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
	JobID            string             `json:"job_id"`
	VolumeID         string             `json:"volume_id"`
	NodeName         string             `json:"node_name,omitempty"`
	EBSRoot          string             `json:"ebs_root,omitempty"`
	Subpath          string             `json:"subpath,omitempty"`
	SpaceRef         string             `json:"space_ref,omitempty"`
	Prefix           string             `json:"prefix,omitempty"`
	RuntimeState     string             `json:"runtime_state"`
	RuntimeAttempts  int                `json:"runtime_attempts,omitempty"`
	RuntimeError     string             `json:"runtime_error,omitempty"`
	Phase            string             `json:"phase,omitempty"`
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
	LargeScale       bool               `json:"large_scale,omitempty"`
	Generation       *workerGeneration  `json:"generation,omitempty"`
}

type workerGeneration struct {
	Stage                  string                  `json:"stage,omitempty"`
	SourceGenerationID     string                  `json:"source_generation_id"`
	TargetGenerationID     string                  `json:"target_generation_id"`
	DiffGenerationID       string                  `json:"diff_generation_id"`
	SourceComplete         bool                    `json:"source_complete"`
	TargetComplete         bool                    `json:"target_complete"`
	DiffComplete           bool                    `json:"diff_complete"`
	SourceCount            int64                   `json:"source_count"`
	TargetCount            int64                   `json:"target_count"`
	BlockerCount           int64                   `json:"blocker_count"`
	PendingCount           int64                   `json:"pending_count"`
	ActiveCount            int64                   `json:"active_count"`
	UnknownCount           int64                   `json:"unknown_count"`
	FindingCounts          map[string]int64        `json:"finding_counts,omitempty"`
	WorkCounts             map[string]int64        `json:"work_counts,omitempty"`
	Stages                 []string                `json:"stages"`
	MemoryUsedBytes        int64                   `json:"memory_used_bytes"`
	MemoryPeakBytes        int64                   `json:"memory_peak_bytes"`
	MemoryLimitBytes       int64                   `json:"memory_limit_bytes"`
	HashReuseCount         int64                   `json:"hash_reuse_count"`
	HashNewCount           int64                   `json:"hash_new_count"`
	SourceDirectories      int64                   `json:"source_directories"`
	SourceFiles            int64                   `json:"source_files"`
	SourceLogicalBytes     int64                   `json:"source_logical_bytes"`
	SourceWarnings         int64                   `json:"source_warnings"`
	SourceBlockers         int64                   `json:"source_blockers"`
	SourceScanDurationMS   int64                   `json:"source_scan_duration_ms"`
	SourceHashDurationMS   int64                   `json:"source_hash_duration_ms"`
	SourceScanRate         float64                 `json:"source_scan_rate"`
	SourceHashRate         float64                 `json:"source_hash_rate"`
	SourceQueueCapacity    int64                   `json:"source_queue_capacity"`
	ManifestPages          int64                   `json:"manifest_pages"`
	ManifestCursor         string                  `json:"manifest_cursor,omitempty"`
	ManifestRawEntries     int64                   `json:"manifest_raw_entries"`
	ManifestResponseBytes  int64                   `json:"manifest_response_bytes"`
	ManifestEmptyPages     int64                   `json:"manifest_empty_pages"`
	ManifestCursorAdvances int64                   `json:"manifest_cursor_advances"`
	ManifestSortRuns       int64                   `json:"manifest_sort_runs"`
	ManifestLastPageAt     time.Time               `json:"manifest_last_page_at,omitempty"`
	ArtifactBytes          int64                   `json:"artifact_bytes"`
	ApplyTotal             int64                   `json:"apply_total"`
	ApplyVerified          int64                   `json:"apply_verified"`
	ApplyPending           int64                   `json:"apply_pending"`
	ApplyUnknown           int64                   `json:"apply_unknown"`
	ApplyInFlight          int64                   `json:"apply_in_flight"`
	ApplyRetry             int64                   `json:"apply_retry"`
	ApplyFailed            int64                   `json:"apply_failed"`
	InlineWorkers          int                     `json:"inline_workers"`
	MultipartWorkers       int                     `json:"multipart_workers"`
	CacheStatus            string                  `json:"cache_status,omitempty"`
	RebuildReason          string                  `json:"rebuild_reason,omitempty"`
	LastProgressAt         time.Time               `json:"last_progress_at,omitempty"`
	BatchCount             int64                   `json:"batch_count"`
	BatchPayloadBytes      int64                   `json:"batch_payload_bytes"`
	BatchLatencyMS         int64                   `json:"batch_latency_ms"`
	InlineFiles            int64                   `json:"inline_files"`
	InlineBytes            int64                   `json:"inline_bytes"`
	MultipartFiles         int64                   `json:"multipart_files"`
	MultipartBytes         int64                   `json:"multipart_bytes"`
	RetryableErrors        int64                   `json:"retryable_errors"`
	RetryDelayMS           int64                   `json:"retry_delay_ms"`
	BackoffUntil           time.Time               `json:"backoff_until,omitempty"`
	RecentErrors           []workerGenerationError `json:"recent_errors,omitempty"`
}

type workerGenerationError struct {
	Stage string    `json:"stage"`
	Class string    `json:"class"`
	At    time.Time `json:"at"`
}

type workerStatusEnvelope struct {
	VolumeID string            `json:"volume_id"`
	NodeName string            `json:"node_name"`
	EBSRoot  string            `json:"ebs_root"`
	Jobs     []json.RawMessage `json:"jobs"`
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
	VolumeID      string          `json:"volume_id,omitempty"`
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
