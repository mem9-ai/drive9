package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

func (s *Server) handleExtentMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	b := backendFromRequest(r)
	if b == nil {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	op := strings.TrimSpace(r.Header.Get("X-Drive9-Extent-Op"))
	if op == "" {
		op = strings.TrimSpace(r.URL.Query().Get("op"))
	}
	if op == "" {
		errJSON(w, http.StatusBadRequest, "missing extent op")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body")
		return
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	body, errno, err := b.Store().RunExtentMetaOp(r.Context(), op, json.RawMessage(raw), extentQuotaLimit(r.Context(), b))
	if err != nil {
		logger.Error(r.Context(), "extent_meta_failed", eventFields(r.Context(), "extent_meta_failed", "op", op, "error", err)...)
		errJSON(w, http.StatusInternalServerError, "extent meta failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if errno == int(syscall.EIO) {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(body)
}

func (s *Server) handleDataCredential(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	scope := ScopeFromContext(r.Context())
	if scope == nil || scope.TenantID == "" {
		errJSON(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if scope.IsScoped {
		errJSON(w, http.StatusForbidden, "scoped token cannot mint data credentials")
		return
	}
	b := backendFromRequest(r)
	var s3 s3client.S3Client
	if b != nil {
		s3 = b.S3()
	}
	if s3 == nil && s.localS3 != nil {
		s3 = s.localS3
	}
	objMode, objKeyID := b.ExtentObjectEncryption()
	if requiresObjectSSE(objMode) {
		// JuiceFS's S3 backend cannot set SSE headers on the blocks it writes
		// (pkg/object/s3.go has no such option), so an extent mount on this
		// deployment would store unencrypted objects while the file metadata
		// claims otherwise. Refuse the credential instead of downgrading the
		// policy; a bucket-level default encryption rule is the supported way
		// to encrypt extent blocks today.
		errJSON(w, http.StatusNotImplemented, fmt.Sprintf(
			"extent data plane cannot honour the deployment's S3 encryption policy %q (key %q): its blocks are written by JuiceFS's S3 backend, which cannot set per-object SSE headers; use bucket-default encryption or disable extent on this deployment",
			objMode, objKeyID))
		return
	}
	cred, err := mintDataCredential(s3, scope.TenantID)
	if err != nil {
		errJSON(w, http.StatusNotImplemented, err.Error())
		return
	}
	// Tell the client what the deployment's object encryption policy is. It is
	// always a policy the data plane can honour (no per-object SSE header) —
	// otherwise the mint above refused — so the client can log it and the
	// projection's storage_encryption_mode stays truthful.
	cred.EncryptionMode = string(objMode)
	cred.EncryptionKeyID = objKeyID
	writeJSON(w, http.StatusOK, cred)
}

// mintDataCredential hands the extent data plane its object-store session. The
// credential is always tenant-prefixed, but the *isolation* behind that prefix
// differs per branch:
//
//   - STS (CanMintSTS): a real AssumeRole session with an inline session policy
//     scoped to t/<tenant>/ (pkg/s3client/aws.go tenantPrefixSessionPolicy).
//     This is the only branch with enforced isolation.
//   - static MinIO (CanMintStatic): the server's own static keys. The prefix is
//     a client-side convention only — IAM does not enforce it, so any tenant
//     can reach every other tenant's prefix. Refused unless the operator sets
//     DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS=1 to declare that acceptable
//     (local/dev MinIO); a tenant-facing production deployment must use STS.
//   - file mock (LocalS3Client): a local directory; there is no isolation at
//     all beyond the path convention. Test-only.
//
// Do not add tenant-facing features that assume isolation on the last two
// branches; require DRIVE9_S3_ROLE_ARN if that is ever needed.
func mintDataCredential(s3 s3client.S3Client, tenantID string) (*dataCredentialResponse, error) {
	prefix := "t/" + tenantID + "/"
	if local, ok := s3.(*s3client.LocalS3Client); ok && local != nil {
		return &dataCredentialResponse{
			Scheme:    extent.SchemeFile,
			Endpoint:  local.ObjectsDir(),
			Prefix:    prefix,
			TenantID:  tenantID,
			ExpiresAt: "",
		}, nil
	}
	aws, ok := s3.(*s3client.AWSS3Client)
	if !ok || aws == nil {
		return nil, errNoExtentCredentials
	}
	if aws.CanMintSTS() {
		out, err := aws.MintTenantPrefix(context.Background(), tenantID, time.Hour)
		if err != nil {
			return nil, err
		}
		return dataCredentialFromS3(out, tenantID), nil
	}
	if aws.CanMintStatic() {
		if !staticDataCredentialsAllowed() {
			return nil, fmt.Errorf("%w: static S3 keys are the server's own credentials and the tenant prefix is a client-side convention only, so they are handed out only when DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS=1 declares every tenant of this deployment trusted (local/dev MinIO)", errNoExtentCredentials)
		}
		out, err := aws.MintStaticPrefix(tenantID)
		if err != nil {
			return nil, err
		}
		return dataCredentialFromS3(out, tenantID), nil
	}
	return nil, errNoExtentCredentials
}

// staticDataCredentialsAllowed reports whether this deployment may hand its own
// static S3 keys to tenants. The static branch exists for a local MinIO that
// has no STS; the prefix it returns is not enforced by IAM, so a deployment has
// to opt in instead of getting it by configuring a static-key endpoint.
//
// The opt-in is the environment variable alone. DRIVE9_TENANT_PROVIDER=local
// selects a provisioning mode, not a trust boundary: a deployment can serve
// several tenants through it, and a static key is unscoped, so it is not
// treated as consent to hand one tenant the server's credentials. Local
// tooling (`make run-server-local`, scripts/e2e-local.sh) sets the variable
// explicitly instead.
func staticDataCredentialsAllowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func dataCredentialFromS3(out *s3client.TenantPrefixCreds, tenantID string) *dataCredentialResponse {
	exp := ""
	if !out.Expiration.IsZero() {
		exp = out.Expiration.UTC().Format(time.RFC3339)
	}
	return &dataCredentialResponse{
		Scheme:          extent.SchemeS3,
		Endpoint:        out.Endpoint,
		Bucket:          out.Bucket,
		Prefix:          out.Prefix,
		TenantID:        tenantID,
		AccessKeyID:     out.AccessKeyID,
		SecretAccessKey: out.SecretAccessKey,
		SessionToken:    out.SessionToken,
		Region:          out.Region,
		ForcePathStyle:  out.ForcePathStyle,
		ExpiresAt:       exp,
	}
}

// extentQuotaLimit snapshots the tenant's quota for the extent write
// transaction. It is the same soft source the classic write path reads
// (per-tenant quota config + cached central usage + this process's pending
// central deltas), plus the extent bytes no central report has accounted for
// yet — the tenant's extent total minus the reported marker.
//
// That additive term must be the unreported part and never the whole extent
// total: reportExtentQuotaUsage already pushes every reported extent byte into
// the central counters these lines read, so adding the total as well would count
// those bytes twice and refuse a quota-configured tenant at about half its real
// usage. A missing quota CONFIG fails open like the classic path (no limit, no
// check); a missing central usage is stricter than the classic path, which skips
// its check entirely: the limit is still enforced against the pending deltas and
// the unreported extent bytes alone, which can only refuse a tenant that is
// already close to its limit.
func extentQuotaLimit(ctx context.Context, b *backend.Dat9Backend) *datastore.ExtentQuotaLimit {
	if b == nil {
		return nil
	}
	cfg := b.QuotaConfigView(ctx)
	if cfg == nil || (cfg.MaxFileSizeBytes <= 0 && cfg.MaxStorageBytes <= 0) {
		return nil
	}
	limit := &datastore.ExtentQuotaLimit{
		MaxFileSizeBytes: cfg.MaxFileSizeBytes,
		MaxStorageBytes:  cfg.MaxStorageBytes,
	}
	if usage := b.QuotaUsageView(ctx); usage != nil {
		limit.UsedBytes = usage.StorageBytes + usage.ReservedBytes
	}
	limit.UsedBytes += b.PendingCentralStorageDelta(ctx)
	if unreported, err := b.Store().ExtentUnreportedUsageBytes(ctx); err == nil {
		limit.UsedBytes += unreported
	} else {
		// Fail open like the classic soft checks, but say so: silently dropping
		// the term would stop checking extent bytes against the limit with no
		// signal at all.
		logger.Warn(ctx, "server_quota_extent_usage_fail_open", zap.Error(err))
	}
	if limit.UsedBytes < 0 {
		// Accounting drift must never read as "negative usage" and admit
		// unlimited writes; the soft limit stays fail-closed at zero.
		limit.UsedBytes = 0
	}
	return limit
}

// requiresObjectSSE reports whether the resolved policy needs a per-object SSE
// header. ""/legacy/none mean "no header", which is what the extent data plane
// writes.
func requiresObjectSSE(mode s3client.EncryptionMode) bool {
	switch mode {
	case "", s3client.EncryptionModeLegacy, s3client.EncryptionModeNone:
		return false
	default:
		return true
	}
}

var errNoExtentCredentials = fmt.Errorf("extent credentials unavailable: configure STS AssumeRole (DRIVE9_S3_ROLE_ARN) or static keys on an S3-compatible endpoint")

type dataCredentialResponse struct {
	Scheme   string `json:"scheme"`
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket,omitempty"`
	Prefix   string `json:"prefix"`
	// TenantID labels the credential for client-side logs and metrics; the
	// prefix stays the only tenant identity the data plane is given.
	TenantID        string `json:"tenant_id,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
	Region          string `json:"region,omitempty"`
	ForcePathStyle  bool   `json:"force_path_style,omitempty"`
	// EncryptionMode/EncryptionKeyID are the deployment's resolved object
	// encryption policy. The extent data plane can only run where no
	// per-object SSE header is required, so these describe bucket-default or
	// no encryption, not a header the client has to send.
	EncryptionMode  string `json:"encryption_mode,omitempty"`
	EncryptionKeyID string `json:"encryption_key_id,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
}

func extentBlockKey(tenantID, key string) string {
	key = strings.TrimPrefix(key, "/")
	prefix := "t/" + tenantID + "/"
	if strings.HasPrefix(key, prefix) {
		return key
	}
	return prefix + key
}

// Extent maintenance batch sizes bound one pass of the extent jobs. A pass
// that completes a full batch means more work is waiting, which is how the
// tenant worker decides to re-kick itself (WorkExtent).
const (
	extentFileGCBatchSize   = 8
	extentBlockGCBatchSize  = 32
	extentSessionSweepBatch = 16
)

// runExtentFileGC drains deleted-file records whose blocks were not reclaimed
// inline (the unlink transaction only records jfs_delfile). It is the safety
// net for a drain that was interrupted; block_gc_tasks produced here are
// deleted by runExtentBlockGC in the same pass. Returns the number of records
// drained.
func runExtentFileGC(ctx context.Context, store *datastore.Store) int {
	if store == nil {
		return 0
	}
	n, _ := store.DrainPendingDeletedFiles(ctx, extentFileGCBatchSize)
	return n
}

// runExtentBlockGC deletes the blocks of superseded slices and marks their
// tasks done. Returns how many tasks completed: a delete (or the follow-up
// mark) that fails leaves its task pending for a later pass and does not count
// as progress, so a failing object store cannot make the worker spin.
func runExtentBlockGC(ctx context.Context, store *datastore.Store, s3 s3client.S3Client, tenantID string) int {
	if store == nil || s3 == nil {
		return 0
	}
	keys, err := store.ListPendingBlockGC(ctx, extentBlockGCBatchSize)
	if err != nil || len(keys) == 0 {
		return 0
	}
	done := 0
	for _, key := range keys {
		full := extentBlockKey(tenantID, key)
		if err := s3.DeleteObject(ctx, full); err != nil && !extentObjectGone(err) {
			// Record the attempt: the task stays visible until max_attempts is
			// exhausted, then it is parked as FAILED, because a block that can
			// never be deleted is a leak rather than a transient error.
			retry, rerr := store.RequeueBlockGC(ctx, key, err)
			if rerr != nil {
				logger.Warn(ctx, "extent block gc requeue failed", zap.String("block", key), zap.Error(rerr))
			} else if !retry {
				logger.Error(ctx, "extent block gc gave up", zap.String("block", key), zap.Error(err))
			}
			continue
		}
		if err := store.MarkBlockGCDone(ctx, key); err != nil {
			continue
		}
		done++
	}
	return done
}

func extentObjectGone(err error) bool {
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "nosuchkey") || strings.Contains(s, "not found") || strings.Contains(s, "not exist")
}

// runExtentSessionSweep reclaims sessions left behind by mounts that died
// without closing them (the jfs_session2 churn the compact fallback used to
// add to). Returns the number of sessions cleaned.
func runExtentSessionSweep(ctx context.Context, store *datastore.Store) int {
	if store == nil {
		return 0
	}
	n, _ := store.SweepStaleExtentSessions(ctx, extentSessionSweepBatch)
	return n
}

// runExtentCompactFallback executes one queued chunk compaction with the
// tenant's cached data-plane runtime (see extentRuntimePool: building a
// Runtime per task leaked a JuiceFS session and a cache-monitor goroutine per
// task). The runtime is built by the caller, so a runtime that cannot be built
// does not lease work.
//
// Every claimed task is settled here. ExecuteCompact reports success for a
// chunk that no longer needs work as well as for one it rewrote, so a success
// completes the row; a failure requeues it and counts the attempt, which is
// what makes max_attempts reachable instead of leaving the chunk to be retried
// by every mount for the life of the tenant.
func runExtentCompactFallback(ctx context.Context, store *datastore.Store, tenantID string, rt *extent.Runtime) {
	if store == nil || rt == nil {
		return
	}
	ino, indx, taskID, err := store.ClaimCompactTask(ctx)
	if err != nil || taskID == "" || ino == 0 {
		return
	}
	if err := extent.ExecuteCompact(ctx, rt, ino, indx); err != nil {
		// A cancelled run (worker shutdown, deleted tenant) is not a failed
		// attempt: the chunk was never tried, so requeueing would count it
		// against max_attempts. Leave the task leased and let the lease expire.
		if ctx.Err() != nil {
			return
		}
		logger.Warn(ctx, "tenant_worker_extent_compact_failed",
			zap.String("tenant_id", tenantID),
			zap.Uint64("inode", ino),
			zap.Uint32("chunk", indx),
			zap.Error(err))
		if rerr := store.RequeueCompactTask(ctx, taskID, err); rerr != nil {
			logger.Warn(ctx, "tenant_worker_extent_compact_requeue_failed",
				zap.String("tenant_id", tenantID),
				zap.String("task_id", taskID),
				zap.Error(rerr))
		}
		return
	}
	if cerr := store.CompleteCompactTask(ctx, taskID); cerr != nil {
		logger.Warn(ctx, "tenant_worker_extent_compact_complete_failed",
			zap.String("tenant_id", tenantID),
			zap.String("task_id", taskID),
			zap.Error(cerr))
	}
}
