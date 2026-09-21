package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

const (
	maxPromotionJSONBodyBytes   int64 = 16 << 20
	maxPromotionInlineBodyBytes int64 = 128 << 20
)

// PromotionRuntime supplies a tenant-scoped store and short-lived leases from
// the non-rollback authority. Implementations must not derive either lease
// from process-local state. Nil runtime keeps the public API disabled.
type PromotionRuntime interface {
	PromotionStore(context.Context, string, *datastore.Store) (*datastore.PromotionStore, error)
	AllocationLease(context.Context, string) (string, error)
	WriterLease(context.Context, string) (string, error)
}

// PromotionRequestAuthorizer is the datastore authorization adapter for HTTP
// requests. Continuation methods resolve the stored target in the datastore,
// then call this adapter using the current request context.
type PromotionRequestAuthorizer struct{}

func (PromotionRequestAuthorizer) AuthorizePromotionRead(ctx context.Context, tenantID, target string) error {
	return authorizePromotionScope(ctx, tenantID, FSOpRead, target)
}

func (PromotionRequestAuthorizer) AuthorizePromotionWrite(ctx context.Context, tenantID, target string) error {
	return authorizePromotionScope(ctx, tenantID, FSOpWrite, target)
}

func authorizePromotionScope(ctx context.Context, tenantID string, op FSOp, target string) error {
	scope := ScopeFromContext(ctx)
	if scope == nil || scope.TenantID == "" || tenantID == "" || scope.TenantID != tenantID {
		return ErrFSAccessDenied
	}
	return scope.AuthorizeFS(op, target)
}

func isScopedPromotionRouteAllowed(method, path string, query url.Values) bool {
	if len(query) != 0 {
		return false
	}
	switch path {
	case "/v1/promotions/imports", "/v1/promotions/imports:plan", "/v1/promotions/imports:allocate":
		return method == http.MethodPost
	}
	_, action, ok := parsePromotionItemPath(path)
	if !ok {
		return false
	}
	if action == "put-inline" {
		return method == http.MethodPut
	}
	switch action {
	case "get", "retire", "verify", "commit", "abort", "renew", "take-over", "ack-result":
		return method == http.MethodPost
	default:
		return false
	}
}

func (s *Server) handlePromotion(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) != 0 || !isScopedPromotionRouteAllowed(r.Method, r.URL.Path, r.URL.Query()) {
		errJSONCode(w, http.StatusMethodNotAllowed, "method not allowed", "method_not_allowed")
		return
	}
	scope := ScopeFromContext(r.Context())
	b := backendFromRequest(r)
	if scope == nil || scope.TenantID == "" || b == nil {
		errJSONCode(w, http.StatusUnauthorized, "authentication required", "unauthorized")
		return
	}
	if s.promotionRuntime == nil {
		errJSONCode(w, http.StatusUnprocessableEntity, "promotion is not enabled", "promotion_not_enabled")
		return
	}
	store, err := s.promotionRuntime.PromotionStore(r.Context(), scope.TenantID, b.Store())
	if err != nil || store == nil {
		if err == nil {
			err = datastore.ErrPromotionDisabled
		}
		writePromotionError(w, r, err)
		return
	}

	switch r.URL.Path {
	case "/v1/promotions/imports:plan":
		s.handlePromotionPlan(w, r, scope, store)
	case "/v1/promotions/imports:allocate":
		s.handlePromotionAllocate(w, r, scope, store)
	case "/v1/promotions/imports":
		s.handlePromotionCreate(w, r, scope, store)
	default:
		migrationID, action, ok := parsePromotionItemPath(r.URL.Path)
		if !ok {
			errJSONCode(w, http.StatusNotFound, "not found", "not_found")
			return
		}
		s.handlePromotionAction(w, r, scope, store, migrationID, action)
	}
}

func parsePromotionItemPath(path string) (string, string, bool) {
	const prefix = "/v1/promotions/imports/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(path, prefix)
	if rest == "" || strings.Contains(rest, "/") {
		return "", "", false
	}
	migrationID, action, ok := strings.Cut(rest, ":")
	if !ok || migrationID == "" || action == "" || strings.Contains(action, ":") {
		return "", "", false
	}
	if _, err := promotion.DecodeMigrationID(migrationID); err != nil {
		return "", "", false
	}
	return migrationID, action, true
}

func (s *Server) handlePromotionPlan(w http.ResponseWriter, r *http.Request, scope *TenantScope, store *datastore.PromotionStore) {
	var req promotion.PlanImportRequest
	if !decodePromotionJSON(w, r, &req) {
		return
	}
	req.TenantID = scope.TenantID
	result, err := store.PlanImport(r.Context(), req)
	if err != nil {
		writePromotionError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handlePromotionAllocate(w http.ResponseWriter, r *http.Request, scope *TenantScope, store *datastore.PromotionStore) {
	var req promotion.AllocateImportRequest
	if !decodePromotionJSON(w, r, &req) {
		return
	}
	lease, err := s.promotionRuntime.AllocationLease(r.Context(), scope.TenantID)
	if err != nil {
		writePromotionError(w, r, err)
		return
	}
	result, err := store.AllocateImportID(r.Context(), datastore.PromotionAllocationRequest{
		TenantID: scope.TenantID, Target: req.Target, ExpectedTargetAbsent: req.ExpectedTargetAbsent,
		IdempotencyKey: req.IdempotencyKey, AllocationLease: lease,
	})
	if err != nil {
		writePromotionError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, promotion.Allocation{
		MigrationID: result.MigrationID, AllocationEpoch: result.AllocationEpoch,
		AllocationSequence: result.AllocationSequence, AllocationProof: result.AllocationProof,
		CreateBefore: result.CreateBefore,
	})
}

func (s *Server) handlePromotionCreate(w http.ResponseWriter, r *http.Request, scope *TenantScope, store *datastore.PromotionStore) {
	var req promotion.CreateImportRequest
	if !decodePromotionJSON(w, r, &req) {
		return
	}
	lease, ok := s.promotionWriterLease(w, r, scope.TenantID)
	if !ok {
		return
	}
	result, err := store.CreateImport(r.Context(), datastore.PromotionCreateRequest{
		TenantID: scope.TenantID, MigrationID: req.MigrationID, AllocationProof: req.AllocationProof,
		Target: req.Target, ExpectedTargetAbsent: req.ExpectedTargetAbsent, Manifest: req.Manifest,
		Plan: req.Plan, OwnerToken: req.OwnerToken, RecoveryToken: req.RecoveryToken, WriterLease: lease,
	})
	if err != nil {
		writePromotionError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, promotionImportStatus(result))
}

func (s *Server) handlePromotionAction(w http.ResponseWriter, r *http.Request, scope *TenantScope, store *datastore.PromotionStore, migrationID, action string) {
	if action == "get" {
		var req promotion.GetImportRequest
		if !decodePromotionJSON(w, r, &req) {
			return
		}
		result, err := store.GetImport(r.Context(), datastore.PromotionGetRequest{
			TenantID: scope.TenantID, MigrationID: migrationID,
			RecoveryToken: req.RecoveryToken, AllocationProof: req.AllocationProof,
		})
		if err != nil {
			writePromotionError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, promotionImportStatus(result))
		return
	}

	lease, ok := s.promotionWriterLease(w, r, scope.TenantID)
	if !ok {
		return
	}
	switch action {
	case "put-inline":
		s.handlePromotionPutInline(w, r, scope, store, migrationID, lease)
	case "verify":
		var req promotion.VerifyImportRequest
		if !decodePromotionJSON(w, r, &req) {
			return
		}
		result, err := store.VerifyImport(r.Context(), datastore.PromotionVerifyRequest{
			TenantID: scope.TenantID, MigrationID: migrationID, OwnerEpoch: req.OwnerEpoch,
			OwnerToken: req.OwnerToken, ManifestHash: req.ManifestHash, WriterLease: lease,
		})
		s.writePromotionImportResult(w, r, result, err)
	case "commit", "abort", "renew":
		var req promotion.OwnerRequest
		if !decodePromotionJSON(w, r, &req) {
			return
		}
		var result *datastore.PromotionImport
		var err error
		switch action {
		case "commit":
			result, err = store.CommitImport(r.Context(), datastore.PromotionCommitRequest{TenantID: scope.TenantID, MigrationID: migrationID, OwnerEpoch: req.OwnerEpoch, OwnerToken: req.OwnerToken, WriterLease: lease})
		case "abort":
			result, err = store.AbortImport(r.Context(), datastore.PromotionAbortRequest{TenantID: scope.TenantID, MigrationID: migrationID, OwnerEpoch: req.OwnerEpoch, OwnerToken: req.OwnerToken, WriterLease: lease})
		case "renew":
			result, err = store.RenewImportLease(r.Context(), datastore.PromotionRenewRequest{TenantID: scope.TenantID, MigrationID: migrationID, OwnerEpoch: req.OwnerEpoch, OwnerToken: req.OwnerToken, WriterLease: lease})
		}
		s.writePromotionImportResult(w, r, result, err)
	case "take-over":
		var req promotion.TakeOverImportRequest
		if !decodePromotionJSON(w, r, &req) {
			return
		}
		result, err := store.TakeOverImport(r.Context(), datastore.PromotionTakeOverRequest{
			TenantID: scope.TenantID, MigrationID: migrationID, ExpectedOwnerEpoch: req.ExpectedOwnerEpoch,
			RecoveryToken: req.RecoveryToken, NewOwnerToken: req.NewOwnerToken, WriterLease: lease,
		})
		s.writePromotionImportResult(w, r, result, err)
	case "retire":
		var req promotion.RetireImportRequest
		if !decodePromotionJSON(w, r, &req) {
			return
		}
		result, err := store.RetireImportAllocation(r.Context(), datastore.PromotionRetireAllocationRequest{TenantID: scope.TenantID, MigrationID: migrationID, AllocationProof: req.AllocationProof, WriterLease: lease})
		if err != nil {
			writePromotionError(w, r, err)
			return
		}
		response := promotion.RetireImportResult{Retired: result.Retired}
		if result.Import != nil {
			status := promotionImportStatus(result.Import)
			response.Import = &status
		}
		writeJSON(w, http.StatusOK, response)
	case "ack-result":
		var req promotion.AcknowledgeImportResultRequest
		if !decodePromotionJSON(w, r, &req) {
			return
		}
		result, err := store.AcknowledgeImportResult(r.Context(), datastore.PromotionAcknowledgeResultRequest{
			TenantID: scope.TenantID, MigrationID: migrationID, RecoveryToken: req.RecoveryToken,
			TerminalState: req.TerminalState, TerminalResultDigest: req.TerminalResultDigest, WriterLease: lease,
		})
		s.writePromotionImportResult(w, r, result, err)
	default:
		errJSONCode(w, http.StatusNotFound, "not found", "not_found")
	}
}

func (s *Server) handlePromotionPutInline(w http.ResponseWriter, r *http.Request, scope *TenantScope, store *datastore.PromotionStore, migrationID, lease string) {
	meta, err := promotionInlineMetadataFromHeaders(r.Header)
	if err != nil {
		errJSONCode(w, http.StatusBadRequest, "invalid promotion content metadata", "invalid_request")
		return
	}
	if meta.SizeBytes > uint64(maxPromotionInlineBodyBytes) {
		errJSONCode(w, http.StatusRequestEntityTooLarge, "promotion content exceeds transport limit", "promotion_limit_exceeded")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(meta.SizeBytes)+1)
	result, err := store.PutInlineImportContent(r.Context(), datastore.PromotionInlineContentRequest{
		TenantID: scope.TenantID, MigrationID: migrationID, OwnerEpoch: meta.OwnerEpoch,
		OwnerToken: meta.OwnerToken, WriterLease: lease, RelativePath: meta.RelativePath,
		EntryHash: meta.EntryHash, SizeBytes: meta.SizeBytes, ChecksumSHA256: meta.ChecksumSHA256,
		IdempotencyKey: meta.IdempotencyKey, Body: r.Body,
	})
	if err != nil {
		writePromotionError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, promotion.InlineContent{
		ContentID: result.ContentID, RelativePath: result.RelativePath, SizeBytes: result.SizeBytes,
		ChecksumSHA256: result.ChecksumSHA256, State: result.State, StateVersion: result.StateVersion,
	})
}

func promotionInlineMetadataFromHeaders(header http.Header) (promotion.InlineContentMetadata, error) {
	ownerEpoch, err := strconv.ParseUint(header.Get("X-Drive9-Promotion-Owner-Epoch"), 10, 64)
	if err != nil || ownerEpoch == 0 {
		return promotion.InlineContentMetadata{}, errors.New("invalid owner epoch")
	}
	size, err := strconv.ParseUint(header.Get("X-Drive9-Promotion-Size"), 10, 64)
	if err != nil {
		return promotion.InlineContentMetadata{}, errors.New("invalid size")
	}
	meta := promotion.InlineContentMetadata{
		OwnerEpoch: ownerEpoch, OwnerToken: header.Get("X-Drive9-Promotion-Owner-Token"),
		RelativePath: header.Get("X-Drive9-Promotion-Relative-Path"), EntryHash: header.Get("X-Drive9-Promotion-Entry-Hash"),
		SizeBytes: size, ChecksumSHA256: header.Get("X-Drive9-Promotion-Checksum-SHA256"),
		IdempotencyKey: header.Get("X-Drive9-Promotion-Idempotency-Key"),
	}
	if meta.OwnerToken == "" || meta.RelativePath == "" || meta.EntryHash == "" || meta.ChecksumSHA256 == "" || meta.IdempotencyKey == "" {
		return promotion.InlineContentMetadata{}, errors.New("missing metadata")
	}
	return meta, nil
}

func (s *Server) promotionWriterLease(w http.ResponseWriter, r *http.Request, tenantID string) (string, bool) {
	lease, err := s.promotionRuntime.WriterLease(r.Context(), tenantID)
	if err != nil {
		writePromotionError(w, r, err)
		return "", false
	}
	if lease == "" {
		writePromotionError(w, r, datastore.ErrPromotionRestoreFenced)
		return "", false
	}
	return lease, true
}

func (s *Server) writePromotionImportResult(w http.ResponseWriter, r *http.Request, result *datastore.PromotionImport, err error) {
	if err != nil {
		writePromotionError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, promotionImportStatus(result))
}

func promotionImportStatus(value *datastore.PromotionImport) promotion.ImportStatus {
	if value == nil {
		return promotion.ImportStatus{}
	}
	return promotion.ImportStatus{
		MigrationID: value.MigrationID, AllocationEpoch: value.AllocationEpoch, AllocationSequence: value.AllocationSequence,
		Target: value.Target, ManifestHash: value.ManifestHash, QuotaReservationID: value.QuotaReservationID,
		State: value.State, StateVersion: value.StateVersion, OwnerEpoch: value.OwnerEpoch,
		ActivityDeadline: value.ActivityDeadline, LeaseExpiresAt: value.LeaseExpiresAt,
		RestoreGeneration: value.RestoreGeneration, DatabaseIncarnation: value.DatabaseIncarnation,
		WriterGeneration: value.WriterGeneration, TerminalResultBlob: value.TerminalResultBlob,
		TerminalResultDigest: value.TerminalResultDigest,
	}
}

func decodePromotionJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxPromotionJSONBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			errJSONCode(w, http.StatusRequestEntityTooLarge, "promotion request body too large", "promotion_limit_exceeded")
		} else {
			errJSONCode(w, http.StatusBadRequest, "invalid promotion request body", "invalid_request")
		}
		return false
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		errJSONCode(w, http.StatusBadRequest, "invalid promotion request body", "invalid_request")
		return false
	}
	return true
}

func writePromotionError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message, retry := promotionHTTPError(err)
	if retry {
		w.Header().Set("Retry-After", "1")
	}
	if status >= http.StatusInternalServerError && code == "promotion_internal_error" {
		logger.Error(r.Context(), "promotion_api_failed", eventFields(r.Context(), "promotion_api_failed", "error", err)...)
	}
	errJSONCode(w, status, message, code)
}

func promotionHTTPError(err error) (int, string, string, bool) {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.Is(err, datastore.ErrNotFound):
		return http.StatusNotFound, "promotion_import_not_found", "promotion import not found", false
	case errors.Is(err, ErrFSAccessDenied):
		return http.StatusForbidden, "permission_denied", "fs access denied", false
	case errors.Is(err, ErrFSInvalidPath), errors.Is(err, promotion.ErrInvalidManifest), errors.Is(err, promotion.ErrInvalidPlan), errors.Is(err, promotion.ErrInvalidAllocationProof), errors.Is(err, promotion.ErrInvalidMigrationID):
		return http.StatusBadRequest, "invalid_request", "invalid promotion request", false
	case errors.Is(err, promotion.ErrManifestLimitExceeded), errors.As(err, &tooLarge):
		return http.StatusRequestEntityTooLarge, "promotion_limit_exceeded", "promotion limit exceeded", false
	case errors.Is(err, promotion.ErrStorageBackendUnsupported):
		return http.StatusUnprocessableEntity, "promotion_storage_backend_unsupported", "promotion storage backend unsupported", false
	case errors.Is(err, datastore.ErrPromotionDisabled):
		return http.StatusUnprocessableEntity, "promotion_not_enabled", "promotion is not enabled", false
	case errors.Is(err, datastore.ErrPromotionTargetChanged):
		return http.StatusConflict, "target_precondition_changed", "promotion target precondition changed", false
	case errors.Is(err, datastore.ErrPromotionConflict), errors.Is(err, promotion.ErrPlanExpired), errors.Is(err, promotion.ErrPlanStale):
		return http.StatusConflict, "promotion_conflict", "promotion request conflict", false
	case errors.Is(err, datastore.ErrPromotionQuotaExceeded):
		return http.StatusTooManyRequests, "quota_exceeded", "promotion quota exceeded", false
	case errors.Is(err, datastore.ErrPromotionIdentityBudgetExceeded):
		return http.StatusTooManyRequests, "promotion_identity_budget_exceeded", "promotion identity budget exceeded", false
	case errors.Is(err, datastore.ErrPromotionIdentityCapacityUnavailable):
		return http.StatusServiceUnavailable, "promotion_identity_capacity_unavailable", "promotion identity capacity unavailable", true
	case errors.Is(err, datastore.ErrPromotionIdentityEpochUnavailable):
		return http.StatusServiceUnavailable, "promotion_identity_epoch_unavailable", "promotion identity epoch unavailable", true
	case errors.Is(err, datastore.ErrPromotionRestoreFenced):
		return http.StatusServiceUnavailable, "promotion_restore_fenced", "promotion is restore-fenced", true
	case errors.Is(err, datastore.ErrPromotionDeadlineExceeded):
		return http.StatusRequestTimeout, "promotion_deadline_exceeded", "promotion activity deadline exceeded", false
	case errors.Is(err, datastore.ErrPromotionIDRetired):
		return http.StatusGone, "promotion_id_retired", "promotion migration id is retired", false
	case errors.Is(err, datastore.ErrPromotionRecoveryRequired):
		return http.StatusInternalServerError, "promotion_recovery_required", "promotion recovery is required", false
	default:
		return http.StatusInternalServerError, "promotion_internal_error", "promotion operation failed", false
	}
}

func errJSONCode(w http.ResponseWriter, status int, message, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message, "code": code})
}

var _ datastore.PromotionAuthorizer = PromotionRequestAuthorizer{}
