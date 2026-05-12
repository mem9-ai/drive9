package starter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/tenant"
)

func TestProvisionerInitSchemaValidatesSchema(t *testing.T) {
	p := &Provisioner{}
	err := p.InitSchema(context.Background(), "ignored-dsn")
	if err == nil {
		t.Fatal("expected starter schema validation to reject invalid dsn")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unknown network") && !strings.Contains(strings.ToLower(err.Error()), "missing the slash separating the database name") {
		t.Fatalf("unexpected starter schema validation error: %v", err)
	}
}

func TestProvisionBranchCreatesBranchFromSourceBranch(t *testing.T) {
	var gotParentID string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1beta1/clusters/c1/branches" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			DisplayName  string `json:"displayName"`
			ParentID     string `json:"parentId"`
			RootPassword string `json:"rootPassword"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body.DisplayName != "fork-tenant" {
			t.Fatalf("displayName = %q", body.DisplayName)
		}
		if body.RootPassword == "" {
			t.Fatal("rootPassword is empty")
		}
		gotParentID = body.ParentID
		_ = json.NewEncoder(w).Encode(map[string]any{
			"branchId":   "b2",
			"state":      "ACTIVE",
			"userPrefix": "u2",
			"endpoints": map[string]any{
				"public": map[string]any{"host": "db.example", "port": 4000},
			},
		})
	}))
	defer ts.Close()

	p := &Provisioner{apiURL: ts.URL, client: ts.Client()}
	out, err := p.ProvisionBranch(context.Background(), "fork-tenant", &tenant.ClusterInfo{
		ClusterID: "c1",
		BranchID:  "b1",
		DBName:    "test",
	})
	if err != nil {
		t.Fatalf("ProvisionBranch: %v", err)
	}
	if gotParentID != "b1" {
		t.Fatalf("parentId = %q, want b1", gotParentID)
	}
	if out.ClusterID != "c1" || out.BranchID != "b2" || out.Username != "u2.root" || out.Host != "db.example" || out.Port != 4000 {
		t.Fatalf("unexpected cluster info: %#v", out)
	}
}

func TestProvisionBranchReturnsBranchIDOnPostCreateValidationError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1beta1/clusters/c1/branches" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"branchId": "b-created",
			"state":    "ACTIVE",
		})
	}))
	defer ts.Close()

	p := &Provisioner{apiURL: ts.URL, client: ts.Client()}
	out, err := p.ProvisionBranch(context.Background(), "fork-tenant", &tenant.ClusterInfo{
		ClusterID: "c1",
		DBName:    "test",
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if out == nil {
		t.Fatal("expected partial cluster info")
	}
	if out.ClusterID != "c1" || out.BranchID != "b-created" {
		t.Fatalf("partial cluster info = %#v", out)
	}
}
