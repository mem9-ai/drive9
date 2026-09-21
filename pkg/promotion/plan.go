package promotion

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// StorageMode is a closed promotion staging capability.
type StorageMode string

const StorageModeDB9Inline StorageMode = "db9_inline"

// StorageCapability is the cluster-visible capability row and the exact
// process adapter classification used to mint or admit a plan.
type StorageCapability struct {
	Generation       uint64
	ConfigGeneration uint64
	InlineEnabled    bool
	InlineThreshold  uint64
	AllowedMode      StorageMode
}

// PlanImportRequest is the side-effect-free planning input.
type PlanImportRequest struct {
	TenantID             string
	Target               string
	ExpectedTargetAbsent bool
	Manifest             []ManifestEntry
}

// ImportPlan is the authenticated immutable result of PlanImport.
type ImportPlan struct {
	Target                      string         `json:"target"`
	ExpectedTargetAbsent        bool           `json:"expected_target_absent"`
	ManifestHash                string         `json:"manifest_hash"`
	EntryTotal                  uint64         `json:"entry_total"`
	ByteTotal                   uint64         `json:"byte_total"`
	MaxContentSize              uint64         `json:"max_content_size"`
	StorageMode                 StorageMode    `json:"storage_mode"`
	StoragePlanDigest           string         `json:"storage_plan_digest"`
	BackendCapabilityGeneration uint64         `json:"backend_capability_generation"`
	ConfigGeneration            uint64         `json:"config_generation"`
	Limits                      ManifestLimits `json:"limits"`
	PlanExpiresAt               time.Time      `json:"plan_expires_at"`
}

type planClaims struct {
	Version                     string         `json:"version"`
	TenantID                    string         `json:"tenant_id"`
	Target                      string         `json:"target"`
	ExpectedTargetAbsent        bool           `json:"expected_target_absent"`
	ManifestHash                string         `json:"manifest_hash"`
	EntryTotal                  uint64         `json:"entry_total"`
	ByteTotal                   uint64         `json:"byte_total"`
	MaxContentSize              uint64         `json:"max_content_size"`
	StorageMode                 StorageMode    `json:"storage_mode"`
	BackendCapabilityGeneration uint64         `json:"backend_capability_generation"`
	ConfigGeneration            uint64         `json:"config_generation"`
	Limits                      ManifestLimits `json:"limits"`
	PlanExpiresAtUnixMilli      int64          `json:"plan_expires_at_unix_milli"`
}

// Planner validates and authenticates side-effect-free plans.
type Planner struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

// NewPlanner constructs a planner. A 256-bit secret is required so plan
// digests cannot be forged into new quota or staging claims.
func NewPlanner(secret []byte, ttl time.Duration) (*Planner, error) {
	return NewPlannerWithClock(secret, ttl, time.Now)
}

// NewPlannerWithClock is NewPlanner with an injected clock for deterministic
// admission and expiry tests.
func NewPlannerWithClock(secret []byte, ttl time.Duration, now func() time.Time) (*Planner, error) {
	if len(secret) < sha256.Size {
		return nil, fmt.Errorf("%w: plan signing secret must contain at least %d bytes", ErrInvalidPlan, sha256.Size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("%w: plan TTL must be positive", ErrInvalidPlan)
	}
	if now == nil {
		return nil, fmt.Errorf("%w: plan clock is required", ErrInvalidPlan)
	}
	return &Planner{key: append([]byte(nil), secret...), ttl: ttl, now: now}, nil
}

// PlanImport validates a complete canonical manifest without creating any
// durable row or quota reservation.
func (p *Planner) PlanImport(req PlanImportRequest, capability StorageCapability, limits ManifestLimits) (*ImportPlan, error) {
	if req.TenantID == "" {
		return nil, fmt.Errorf("%w: tenant is required", ErrInvalidPlan)
	}
	if !req.ExpectedTargetAbsent {
		return nil, fmt.Errorf("%w: P0 requires expected_target_absent=true", ErrInvalidPlan)
	}
	target, err := canonicalTarget(req.Target)
	if err != nil {
		return nil, err
	}
	if err := validateCapability(capability, limits); err != nil {
		return nil, err
	}
	manifest, err := ValidateCanonicalManifest(req.Manifest, limits)
	if err != nil {
		return nil, err
	}
	expiresAt := p.now().UTC().Add(p.ttl).Truncate(time.Millisecond)
	plan := &ImportPlan{
		Target:                      target,
		ExpectedTargetAbsent:        true,
		ManifestHash:                manifest.ManifestHash,
		EntryTotal:                  manifest.EntryTotal,
		ByteTotal:                   manifest.ByteTotal,
		MaxContentSize:              manifest.MaxContentSize,
		StorageMode:                 StorageModeDB9Inline,
		BackendCapabilityGeneration: capability.Generation,
		ConfigGeneration:            capability.ConfigGeneration,
		Limits:                      limits,
		PlanExpiresAt:               expiresAt,
	}
	digest, err := p.signPlan(req.TenantID, plan)
	if err != nil {
		return nil, err
	}
	plan.StoragePlanDigest = digest
	return plan, nil
}

// VerifyCreatePlan revalidates the exact manifest and current capability before
// a first CreateImport transaction. Accepted-row recovery must happen before
// calling this method because recovery intentionally ignores later expiry or
// capability rollout.
func (p *Planner) VerifyCreatePlan(req PlanImportRequest, plan ImportPlan, capability StorageCapability) (*CanonicalManifest, error) {
	if req.TenantID == "" || !req.ExpectedTargetAbsent || !plan.ExpectedTargetAbsent {
		return nil, fmt.Errorf("%w: P0 requires an absent target", ErrInvalidPlan)
	}
	target, err := canonicalTarget(req.Target)
	if err != nil {
		return nil, err
	}
	if target != plan.Target {
		return nil, fmt.Errorf("%w: target mismatch", ErrInvalidPlan)
	}
	manifest, err := ValidateCanonicalManifest(req.Manifest, plan.Limits)
	if err != nil {
		return nil, err
	}
	if manifest.ManifestHash != plan.ManifestHash || manifest.EntryTotal != plan.EntryTotal ||
		manifest.ByteTotal != plan.ByteTotal || manifest.MaxContentSize != plan.MaxContentSize {
		return nil, fmt.Errorf("%w: manifest summary mismatch", ErrInvalidPlan)
	}
	if !p.now().Before(plan.PlanExpiresAt) {
		return nil, ErrPlanExpired
	}
	if capability.Generation != plan.BackendCapabilityGeneration ||
		capability.ConfigGeneration != plan.ConfigGeneration ||
		capability.InlineThreshold != plan.Limits.InlineThreshold {
		return nil, ErrPlanStale
	}
	if err := validateCapability(capability, plan.Limits); err != nil {
		return nil, err
	}
	expected, err := p.signPlan(req.TenantID, &plan)
	if err != nil {
		return nil, err
	}
	got, err := hex.DecodeString(plan.StoragePlanDigest)
	if err != nil {
		return nil, ErrInvalidPlan
	}
	want, _ := hex.DecodeString(expected)
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return nil, fmt.Errorf("%w: plan MAC mismatch", ErrInvalidPlan)
	}
	return manifest, nil
}

func (p *Planner) signPlan(tenantID string, plan *ImportPlan) (string, error) {
	claims := planClaims{
		Version:                     "p1",
		TenantID:                    tenantID,
		Target:                      plan.Target,
		ExpectedTargetAbsent:        plan.ExpectedTargetAbsent,
		ManifestHash:                plan.ManifestHash,
		EntryTotal:                  plan.EntryTotal,
		ByteTotal:                   plan.ByteTotal,
		MaxContentSize:              plan.MaxContentSize,
		StorageMode:                 plan.StorageMode,
		BackendCapabilityGeneration: plan.BackendCapabilityGeneration,
		ConfigGeneration:            plan.ConfigGeneration,
		Limits:                      plan.Limits,
		PlanExpiresAtUnixMilli:      plan.PlanExpiresAt.UnixMilli(),
	}
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal plan claims: %w", err)
	}
	mac := hmac.New(sha256.New, p.key)
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func validateCapability(capability StorageCapability, limits ManifestLimits) error {
	if err := validateLimits(limits); err != nil {
		return err
	}
	if !capability.InlineEnabled || capability.AllowedMode != StorageModeDB9Inline {
		return ErrStorageBackendUnsupported
	}
	if capability.InlineThreshold == 0 || capability.InlineThreshold != limits.InlineThreshold {
		return ErrPlanStale
	}
	return nil
}

func canonicalTarget(raw string) (string, error) {
	target, err := pathutil.Canonicalize(raw)
	if err != nil {
		return "", fmt.Errorf("%w: target: %v", ErrInvalidPlan, err)
	}
	if target == "/" {
		return "", fmt.Errorf("%w: root cannot be a promotion target", ErrInvalidPlan)
	}
	return target, nil
}

// CanonicalTarget validates and canonicalizes an absent-target promotion path.
func CanonicalTarget(raw string) (string, error) {
	return canonicalTarget(raw)
}
