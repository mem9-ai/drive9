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

func TestNewProvisionerFromEnvParsesDefaultSpendingLimit(t *testing.T) {
	t.Setenv(envTiDBAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(envTiDBAPIKey, "key")
	t.Setenv(envTiDBAPISecret, "secret")
	t.Setenv(envTiDBPoolID, "pool")
	t.Setenv(envTiDBSpendLimit, "2500")

	p, err := NewProvisionerFromEnv()
	if err != nil {
		t.Fatalf("NewProvisionerFromEnv: %v", err)
	}
	if p.defaultSpendLimit == nil || *p.defaultSpendLimit != 2500 {
		t.Fatalf("defaultSpendLimit = %v, want 2500", p.defaultSpendLimit)
	}
}

func TestNewProvisionerFromEnvRejectsInvalidDefaultSpendingLimit(t *testing.T) {
	t.Setenv(envTiDBAPIURL, "https://serverless.tidbapi.com")
	t.Setenv(envTiDBAPIKey, "key")
	t.Setenv(envTiDBAPISecret, "secret")
	t.Setenv(envTiDBPoolID, "pool")
	t.Setenv(envTiDBSpendLimit, "-1")

	_, err := NewProvisionerFromEnv()
	if err == nil {
		t.Fatal("expected invalid default spending limit error")
	}
	if !strings.Contains(err.Error(), envTiDBSpendLimit) || !strings.Contains(err.Error(), "-1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

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

func TestProvisionAppliesDefaultSpendingLimitAfterTakeover(t *testing.T) {
	var order []string
	var gotMonthly int32
	var gotUpdateMask string
	limit := int32(2500)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1beta1/clusters:takeoverFromPool":
			order = append(order, "takeover")
			var body struct {
				PoolID       string `json:"pool_id"`
				RootPassword string `json:"root_password"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode takeover request: %v", err)
			}
			if body.PoolID != "pool-1" {
				t.Fatalf("pool_id = %q, want pool-1", body.PoolID)
			}
			if body.RootPassword == "" {
				t.Fatal("root_password is empty")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId":  "c1",
				"userPrefix": "u1",
				"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1beta1/clusters/c1":
			order = append(order, "spending-limit")
			var body struct {
				UpdateMask string `json:"updateMask"`
				Cluster    struct {
					ClusterID     string `json:"clusterId"`
					SpendingLimit struct {
						Monthly int32 `json:"monthly"`
					} `json:"spendingLimit"`
				} `json:"cluster"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode spending limit request: %v", err)
			}
			gotUpdateMask = body.UpdateMask
			gotMonthly = body.Cluster.SpendingLimit.Monthly
			if body.Cluster.ClusterID != "" {
				t.Fatalf("cluster.clusterId = %q, want empty body field", body.Cluster.ClusterID)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"clusterId": "c1"})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	p := &Provisioner{apiURL: ts.URL, poolID: "pool-1", defaultSpendLimit: &limit, client: ts.Client()}
	out, err := p.Provision(context.Background(), "tenant-1")
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if len(order) != 2 || order[0] != "takeover" || order[1] != "spending-limit" {
		t.Fatalf("order = %v", order)
	}
	if gotUpdateMask != "spendingLimit.monthly" {
		t.Fatalf("updateMask = %q, want spendingLimit.monthly", gotUpdateMask)
	}
	if gotMonthly != 2500 {
		t.Fatalf("monthly = %d, want 2500", gotMonthly)
	}
	if out.ClusterID != "c1" || out.Username != "u1.root" || out.Password == "" {
		t.Fatalf("unexpected cluster info: %#v", out)
	}
}

func TestProvisionSkipsSpendingLimitWhenUnset(t *testing.T) {
	var patchCalled bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1beta1/clusters:takeoverFromPool":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId":  "c1",
				"userPrefix": "u1",
				"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
			})
		case r.Method == http.MethodPatch:
			patchCalled = true
			t.Fatalf("unexpected spending limit update %s", r.URL.Path)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	p := &Provisioner{apiURL: ts.URL, poolID: "pool-1", client: ts.Client()}
	if _, err := p.Provision(context.Background(), "tenant-1"); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if patchCalled {
		t.Fatal("spending limit update was called")
	}
}

func TestProvisionValidatesRequiredTakeoverMetadata(t *testing.T) {
	tests := []struct {
		name    string
		body    map[string]any
		wantErr string
	}{
		{
			name: "missing cluster id",
			body: map[string]any{
				"userPrefix": "u1",
				"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
			},
			wantErr: "starter response missing cluster id",
		},
		{
			name: "missing user prefix",
			body: map[string]any{
				"clusterId": "c1",
				"endpoints": map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
			},
			wantErr: "starter response missing user prefix",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1beta1/clusters:takeoverFromPool" {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				_ = json.NewEncoder(w).Encode(tt.body)
			}))
			defer ts.Close()

			p := &Provisioner{apiURL: ts.URL, poolID: "pool-1", client: ts.Client()}
			_, err := p.Provision(context.Background(), "tenant-1")
			if err == nil {
				t.Fatal("expected metadata validation error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestProvisionReturnsSpendingLimitUpdateError(t *testing.T) {
	limit := int32(2500)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1beta1/clusters:takeoverFromPool":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"clusterId":  "c1",
				"userPrefix": "u1",
				"endpoints":  map[string]any{"public": map[string]any{"host": "db.example", "port": 4000}},
			})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1beta1/clusters/c1":
			http.Error(w, "limit rejected", http.StatusBadRequest)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	p := &Provisioner{apiURL: ts.URL, poolID: "pool-1", defaultSpendLimit: &limit, client: ts.Client()}
	out, err := p.Provision(context.Background(), "tenant-1")
	if err == nil {
		t.Fatal("expected spending limit update error")
	}
	if out == nil || out.ClusterID != "c1" || out.Username != "u1.root" || out.Password == "" {
		t.Fatalf("partial cluster info = %#v, want created cluster", out)
	}
	if !strings.Contains(err.Error(), "update starter spending limit for cluster c1") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "starter spending limit update status 400") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeprovisionDeletesCluster(t *testing.T) {
	var deleteCalled bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", nonce="nonce-1", qop="auth"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodDelete || r.URL.Path != "/v1beta1/clusters/c1" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		deleteCalled = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	p := &Provisioner{apiURL: ts.URL, apiKey: "public-1", apiSecret: "private-1", client: ts.Client()}
	if err := p.Deprovision(context.Background(), &tenant.ClusterInfo{ClusterID: "c1"}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if !deleteCalled {
		t.Fatal("delete was not called")
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
		if body.RootPassword != "branch-pass" {
			t.Fatalf("rootPassword = %q, want branch-pass", body.RootPassword)
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
		Password:  "branch-pass",
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
	if out.Password != "branch-pass" {
		t.Fatalf("password = %q, want branch-pass", out.Password)
	}
}

func TestCreateBranchDoesNotWaitForActive(t *testing.T) {
	var gotGet bool
	var gotRootPassword string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1beta1/clusters/c1/branches":
			var body struct {
				RootPassword string `json:"rootPassword"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			gotRootPassword = body.RootPassword
			_ = json.NewEncoder(w).Encode(map[string]any{
				"branchId": "b-pending",
				"state":    "CREATING",
			})
		case r.Method == http.MethodGet:
			gotGet = true
			t.Fatalf("CreateBranch must not poll branch status")
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer ts.Close()

	p := &Provisioner{apiURL: ts.URL, client: ts.Client()}
	out, err := p.CreateBranch(context.Background(), "fork-tenant", &tenant.ClusterInfo{
		ClusterID: "c1",
		BranchID:  "b1",
		Password:  "branch-pass",
		DBName:    "test",
	})
	if err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}
	if gotGet {
		t.Fatal("CreateBranch polled branch status")
	}
	if gotRootPassword != "branch-pass" {
		t.Fatalf("rootPassword = %q, want branch-pass", gotRootPassword)
	}
	if out.ClusterID != "c1" || out.BranchID != "b-pending" || out.Host != "" || out.Username != "" || out.Password != "branch-pass" {
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
		return
	}
	if out.ClusterID != "c1" || out.BranchID != "b-created" {
		t.Fatalf("partial cluster info = %#v", out)
	}
}
