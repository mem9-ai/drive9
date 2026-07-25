package tidbcloudnative

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestLocalClustersAPIAuthorizeAndResolve(t *testing.T) {
	api := NewLocalClustersAPI(LocalClustersAPIConfig{
		Runtime: "false-runtime",
		Image:   "unused",
		Host:    "127.0.0.1",
	})
	_, err := api.ResolveAPIKey(context.Background(), "", "")
	if err == nil {
		t.Fatal("expected missing credentials error")
	}
	id, err := api.ResolveAPIKey(context.Background(), "pk", "sk")
	if err != nil {
		t.Fatalf("ResolveAPIKey: %v", err)
	}
	if id.OrganizationID != localOrgID || id.Role != tenant.TiDBCloudRoleOrgOwner {
		t.Fatalf("identity = %+v", id)
	}
}

func TestParseLocalCreateBodyRequiresPassword(t *testing.T) {
	_, err := parseLocalCreateBody([]byte(`{"displayName":"x","labels":{"a":"b"}}`))
	if err == nil || !strings.Contains(err.Error(), "rootPassword") {
		t.Fatalf("error = %v, want rootPassword required", err)
	}
	req, err := parseLocalCreateBody([]byte(`{
		"displayName":"x",
		"rootPassword":"secret",
		"labels":{"drive9.ai/managed":"true"},
		"spendingLimit":{"monthly":1000}
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if req.RootPassword != "secret" || req.Spending == nil || *req.Spending != 1000 {
		t.Fatalf("req = %+v", req)
	}
}

func TestIsPortConflictError(t *testing.T) {
	for _, tc := range []struct {
		msg  string
		want bool
	}{
		{"Bind for 0.0.0.0:40123 failed: port is already allocated", true},
		{"Error: listen tcp4 :40123: bind: address already in use", true},
		{"failed to bind host port for 0.0.0.0:1234", true},
		{"cannot pull image: network timeout", false},
		{"", false},
	} {
		if got := isPortConflictError(tc.msg); got != tc.want {
			t.Errorf("isPortConflictError(%q)=%v want %v", tc.msg, got, tc.want)
		}
	}
}

func TestLocalClustersAPIAllocateHostPortSkipsUsedAndReserved(t *testing.T) {
	api := NewLocalClustersAPI(LocalClustersAPIConfig{Host: "127.0.0.1"})
	// Reserve a wide range of recently-picked ports by marking them used via clusters.
	// allocateHostPort must still find a free OS port not in that set.
	api.mu.Lock()
	for p := 30000; p < 30100; p++ {
		api.reservedPorts[p] = struct{}{}
	}
	api.clusters["c-used"] = &localCluster{info: clusterInfo{}}
	api.clusters["c-used"].info.Endpoints.Public.Port = 30100
	api.mu.Unlock()

	port, err := api.allocateHostPort()
	if err != nil {
		t.Fatalf("allocateHostPort: %v", err)
	}
	if port >= 30000 && port <= 30100 {
		t.Fatalf("allocated used/reserved port %d", port)
	}
	if port <= 0 {
		t.Fatalf("invalid port %d", port)
	}
}

func TestLocalClustersAPIPatchAndListInMemory(t *testing.T) {
	api := NewLocalClustersAPI(LocalClustersAPIConfig{Host: "127.0.0.1"})
	// Inject a fake cluster without docker.
	api.mu.Lock()
	api.clusters["c1"] = &localCluster{info: clusterInfo{
		ClusterID:  "c1",
		State:      stateActive,
		Labels:     map[string]string{Drive9ManagedLabel: "true"},
		UserPrefix: localUserPrefix,
	}}
	api.clusters["c1"].info.Endpoints.Public.Host = "127.0.0.1"
	api.clusters["c1"].info.Endpoints.Public.Port = 4000
	api.mu.Unlock()

	body, _ := json.Marshal(map[string]any{
		"updateMask": "spendingLimit.monthly",
		"cluster":    map[string]any{"spendingLimit": map[string]int32{"monthly": 42}},
	})
	if err := api.PatchCluster(context.Background(), "pk", "sk", "c1", body); err != nil {
		t.Fatalf("PatchCluster: %v", err)
	}
	got, err := api.GetCluster(context.Background(), "pk", "sk", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if got.SpendingLimit == nil || got.SpendingLimit.Monthly != 42 {
		t.Fatalf("spending = %#v", got.SpendingLimit)
	}
	list, _, err := api.ListClusters(context.Background(), "pk", "sk", nil)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListClusters = %d err=%v", len(list), err)
	}
}
