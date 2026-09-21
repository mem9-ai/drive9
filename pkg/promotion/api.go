package promotion

import "time"

// AllocateImportRequest is the public allocation request. Tenant identity and
// the allocation-authority lease are supplied by the authenticated server,
// never by the client.
type AllocateImportRequest struct {
	Target               string `json:"target"`
	ExpectedTargetAbsent bool   `json:"expected_target_absent"`
	IdempotencyKey       string `json:"idempotency_key"`
}

// Allocation is the durable identity returned by AllocateImportID.
type Allocation struct {
	MigrationID        string    `json:"migration_id"`
	AllocationEpoch    uint64    `json:"allocation_epoch"`
	AllocationSequence uint64    `json:"allocation_sequence"`
	AllocationProof    string    `json:"allocation_proof"`
	CreateBefore       time.Time `json:"create_before"`
}

// CreateImportRequest binds an allocated identity to one complete immutable
// manifest. Owner and recovery tokens are client-generated opaque secrets.
type CreateImportRequest struct {
	MigrationID          string          `json:"migration_id"`
	AllocationProof      string          `json:"allocation_proof"`
	Target               string          `json:"target"`
	ExpectedTargetAbsent bool            `json:"expected_target_absent"`
	Manifest             []ManifestEntry `json:"manifest"`
	Plan                 ImportPlan      `json:"plan"`
	OwnerToken           string          `json:"owner_token"`
	RecoveryToken        string          `json:"recovery_token"`
}

// ImportStatus is the common accepted-state and terminal response.
type ImportStatus struct {
	MigrationID          string    `json:"migration_id"`
	AllocationEpoch      uint64    `json:"allocation_epoch"`
	AllocationSequence   uint64    `json:"allocation_sequence"`
	Target               string    `json:"target"`
	ManifestHash         string    `json:"manifest_hash"`
	QuotaReservationID   string    `json:"quota_reservation_id"`
	State                string    `json:"state"`
	StateVersion         uint64    `json:"state_version"`
	OwnerEpoch           uint64    `json:"owner_epoch"`
	ActivityDeadline     time.Time `json:"activity_deadline"`
	LeaseExpiresAt       time.Time `json:"lease_expires_at"`
	RestoreGeneration    uint64    `json:"restore_generation"`
	DatabaseIncarnation  string    `json:"database_incarnation"`
	WriterGeneration     uint64    `json:"writer_generation"`
	TerminalResultBlob   string    `json:"terminal_result_blob,omitempty"`
	TerminalResultDigest string    `json:"terminal_result_digest,omitempty"`
}

// InlineContentMetadata accompanies a bounded raw request body.
type InlineContentMetadata struct {
	OwnerEpoch     uint64 `json:"owner_epoch"`
	OwnerToken     string `json:"owner_token"`
	RelativePath   string `json:"relative_path"`
	EntryHash      string `json:"entry_hash"`
	SizeBytes      uint64 `json:"size_bytes"`
	ChecksumSHA256 string `json:"checksum_sha256"`
	IdempotencyKey string `json:"idempotency_key"`
}

// InlineContent is the immutable content identity bound to a manifest entry.
type InlineContent struct {
	ContentID      string `json:"content_id"`
	RelativePath   string `json:"relative_path"`
	SizeBytes      uint64 `json:"size_bytes"`
	ChecksumSHA256 string `json:"checksum_sha256"`
	State          string `json:"state"`
	StateVersion   uint64 `json:"state_version"`
}

// OwnerRequest authenticates an owner-scoped continuation.
type OwnerRequest struct {
	OwnerEpoch uint64 `json:"owner_epoch"`
	OwnerToken string `json:"owner_token"`
}

// VerifyImportRequest freezes the manifest after every file is staged.
type VerifyImportRequest struct {
	OwnerEpoch   uint64 `json:"owner_epoch"`
	OwnerToken   string `json:"owner_token"`
	ManifestHash string `json:"manifest_hash"`
}

// GetImportRequest authenticates a status lookup with exactly one credential.
type GetImportRequest struct {
	RecoveryToken   string `json:"recovery_token,omitempty"`
	AllocationProof string `json:"allocation_proof,omitempty"`
}

// TakeOverImportRequest transfers an expired owner lease.
type TakeOverImportRequest struct {
	ExpectedOwnerEpoch uint64 `json:"expected_owner_epoch"`
	RecoveryToken      string `json:"recovery_token"`
	NewOwnerToken      string `json:"new_owner_token"`
}

// RetireImportRequest serializes an undispatched allocation with CreateImport.
type RetireImportRequest struct {
	AllocationProof string `json:"allocation_proof"`
}

// RetireImportResult reports the sole durable winner.
type RetireImportResult struct {
	Retired bool          `json:"retired"`
	Import  *ImportStatus `json:"import,omitempty"`
}

// AcknowledgeImportResultRequest acknowledges a durably recorded terminal result.
type AcknowledgeImportResultRequest struct {
	RecoveryToken        string `json:"recovery_token"`
	TerminalState        string `json:"terminal_state"`
	TerminalResultDigest string `json:"terminal_result_digest"`
}
