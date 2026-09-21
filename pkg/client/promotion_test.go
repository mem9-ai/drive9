package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/promotion"
)

func TestPromotionClientPlanAndInlineContentWireContract(t *testing.T) {
	migrationID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{AllocationEpoch: 1, AllocationSequence: 2})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/promotions/imports:plan", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("plan request = %s auth=%q", r.Method, r.Header.Get("Authorization"))
		}
		var req promotion.PlanImportRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.TenantID != "" || req.Target != "/target" || !req.ExpectedTargetAbsent {
			t.Fatalf("plan request = %+v", req)
		}
		_ = json.NewEncoder(w).Encode(promotion.ImportPlan{Target: req.Target, PlanExpiresAt: time.Unix(10, 0).UTC()})
	})
	mux.HandleFunc("/v1/promotions/imports/"+migrationID+":put-inline", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("inline method = %s", r.Method)
		}
		if got := r.Header.Get("X-Drive9-Promotion-Relative-Path"); got != "dir/file.txt" {
			t.Fatalf("relative path = %q", got)
		}
		if got := r.Header.Get("X-Drive9-Promotion-Owner-Token"); got != "owner" {
			t.Fatalf("owner token = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, []byte("data")) {
			t.Fatalf("body = %q", body)
		}
		_ = json.NewEncoder(w).Encode(promotion.InlineContent{ContentID: "content-1", RelativePath: "dir/file.txt"})
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	client := New(server.URL, "secret")
	plan, err := client.PlanPromotionImport(context.Background(), promotion.PlanImportRequest{TenantID: "must-not-cross-wire", Target: "/target", ExpectedTargetAbsent: true})
	if err != nil {
		t.Fatalf("PlanPromotionImport: %v", err)
	}
	if plan.Target != "/target" {
		t.Fatalf("plan target = %q", plan.Target)
	}
	content, err := client.PutPromotionInlineContent(context.Background(), migrationID, promotion.InlineContentMetadata{
		OwnerEpoch: 1, OwnerToken: "owner", RelativePath: "dir/file.txt",
		EntryHash: strings.Repeat("a", 64), SizeBytes: 4,
		ChecksumSHA256: strings.Repeat("b", 64), IdempotencyKey: "key",
	}, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("PutPromotionInlineContent: %v", err)
	}
	if content.ContentID != "content-1" {
		t.Fatalf("content id = %q", content.ContentID)
	}
}

func TestPromotionClientPreservesTypedErrorCode(t *testing.T) {
	migrationID, err := promotion.EncodeMigrationID(promotion.MigrationIdentity{AllocationEpoch: 4, AllocationSequence: 5})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"promotion is restore-fenced","code":"promotion_restore_fenced"}`)
	}))
	defer server.Close()
	client := New(server.URL, "secret")
	_, err = client.GetPromotionImport(context.Background(), migrationID, promotion.GetImportRequest{RecoveryToken: "token"})
	status, ok := err.(*StatusError)
	if !ok || status.StatusCode != http.StatusServiceUnavailable || status.Code != "promotion_restore_fenced" {
		t.Fatalf("error = %#v", err)
	}
}
