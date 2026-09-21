package server

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

func TestScopedPromotionRouteAllowlistMirrorsDispatcher(t *testing.T) {
	migrationID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{AllocationEpoch: 2, AllocationSequence: 3})
	if err != nil {
		t.Fatal(err)
	}
	allowed := []struct{ method, path string }{
		{http.MethodPost, "/v1/promotions/imports:plan"},
		{http.MethodPost, "/v1/promotions/imports:allocate"},
		{http.MethodPost, "/v1/promotions/imports"},
		{http.MethodPut, "/v1/promotions/imports/" + migrationID + ":put-inline"},
		{http.MethodPost, "/v1/promotions/imports/" + migrationID + ":get"},
		{http.MethodPost, "/v1/promotions/imports/" + migrationID + ":commit"},
		{http.MethodPost, "/v1/promotions/imports/" + migrationID + ":abort"},
	}
	for _, tc := range allowed {
		if !isScopedPromotionRouteAllowed(tc.method, tc.path, nil) {
			t.Errorf("route %s %s denied", tc.method, tc.path)
		}
	}
	denied := []struct {
		method string
		path   string
		query  url.Values
	}{
		{http.MethodGet, "/v1/promotions/imports", nil},
		{http.MethodPost, "/v1/promotions/imports/not-an-id:get", nil},
		{http.MethodPost, "/v1/promotions/imports/" + migrationID + ":future", nil},
		{http.MethodPost, "/v1/promotions/imports/" + migrationID + ":get", url.Values{"leak": {"1"}}},
	}
	for _, tc := range denied {
		if isScopedPromotionRouteAllowed(tc.method, tc.path, tc.query) {
			t.Errorf("route %s %s unexpectedly allowed", tc.method, tc.path)
		}
	}
}

func TestPromotionHTTPErrorContract(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
		retry  bool
	}{
		{datastore.ErrNotFound, http.StatusNotFound, "promotion_import_not_found", false},
		{datastore.ErrPromotionRestoreFenced, http.StatusServiceUnavailable, "promotion_restore_fenced", true},
		{datastore.ErrPromotionIdentityBudgetExceeded, http.StatusTooManyRequests, "promotion_identity_budget_exceeded", false},
		{datastore.ErrPromotionDeadlineExceeded, http.StatusRequestTimeout, "promotion_deadline_exceeded", false},
		{datastore.ErrPromotionTargetChanged, http.StatusConflict, "target_precondition_changed", false},
		{datastore.ErrPromotionRecoveryRequired, http.StatusInternalServerError, "promotion_recovery_required", false},
		{promotion.ErrStorageBackendUnsupported, http.StatusUnprocessableEntity, "promotion_storage_backend_unsupported", false},
	}
	for _, tc := range tests {
		status, code, _, retry := promotionHTTPError(tc.err)
		if status != tc.status || code != tc.code || retry != tc.retry {
			t.Errorf("promotionHTTPError(%v) = (%d,%q,%v), want (%d,%q,%v)", tc.err, status, code, retry, tc.status, tc.code, tc.retry)
		}
	}
	if status, code, _, _ := promotionHTTPError(errors.New("database down")); status != http.StatusInternalServerError || code != "promotion_internal_error" {
		t.Fatalf("unexpected internal mapping: %d %q", status, code)
	}
}

func TestPromotionRequestAuthorizerUsesCurrentScope(t *testing.T) {
	scope := &TenantScope{TenantID: "tenant-a", IsScoped: true, FSScopes: []FSScope{{Prefix: "/allowed", Ops: map[FSOp]bool{FSOpRead: true, FSOpWrite: true}}}}
	ctx := context.WithValue(context.Background(), tenantScopeKey, scope)
	authorizer := PromotionRequestAuthorizer{}
	if err := authorizer.AuthorizePromotionWrite(ctx, "tenant-a", "/allowed/tree"); err != nil {
		t.Fatalf("allowed write: %v", err)
	}
	if err := authorizer.AuthorizePromotionRead(ctx, "tenant-a", "/denied/tree"); !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("denied read = %v", err)
	}
	if err := authorizer.AuthorizePromotionRead(ctx, "tenant-b", "/allowed/tree"); !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("cross-tenant read = %v", err)
	}
}
