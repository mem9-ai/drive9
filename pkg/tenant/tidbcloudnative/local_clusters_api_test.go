package tidbcloudnative

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/tenant"
)

func TestLocalClustersAPIAuthorizeAndResolve(t *testing.T) {
	api, err := NewLocalClustersAPI(LocalClustersAPIConfig{
		Runtime: "false-runtime",
		Image:   "unused",
		Host:    "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.ResolveAPIKey(context.Background(), "", "")
	if err == nil {
		t.Fatal("expected missing credentials error")
	}
	pub, priv := api.Credentials()
	id, err := api.ResolveAPIKey(context.Background(), pub, priv)
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
	api := mustLocalClustersAPI(t, LocalClustersAPIConfig{Host: "127.0.0.1"})
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
	api := mustLocalClustersAPI(t, LocalClustersAPIConfig{Host: "127.0.0.1"})
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
	pub, priv := api.Credentials()
	if err := api.PatchCluster(context.Background(), pub, priv, "c1", body); err != nil {
		t.Fatalf("PatchCluster: %v", err)
	}
	got, err := api.GetCluster(context.Background(), pub, priv, "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if got.SpendingLimit == nil || got.SpendingLimit.Monthly != 42 {
		t.Fatalf("spending = %#v", got.SpendingLimit)
	}
	list, _, err := api.ListClusters(context.Background(), pub, priv, nil)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListClusters = %d err=%v", len(list), err)
	}
}

func mustLocalClustersAPI(t *testing.T, cfg LocalClustersAPIConfig) *LocalClustersAPI {
	t.Helper()
	api, err := NewLocalClustersAPI(cfg)
	if err != nil {
		t.Fatalf("NewLocalClustersAPI: %v", err)
	}
	return api
}

// The local control plane must accept exactly the key pair it owns. Before this
// was enforced, any non-empty caller-chosen pair resolved to org:owner on every
// cluster, admin and quota path.
func TestLocalClustersAPIRejectsCallerChosenCredentialPair(t *testing.T) {
	api := mustLocalClustersAPI(t, LocalClustersAPIConfig{
		Runtime:    "false-runtime",
		Image:      "unused",
		Host:       "127.0.0.1",
		PublicKey:  "pinned-public",
		PrivateKey: "pinned-private",
	})
	if _, err := api.ResolveAPIKey(context.Background(), api.credential.publicKey, api.credential.privateKey); err != nil {
		t.Fatalf("pinned pair rejected: %v", err)
	}
	for _, tc := range []struct{ pub, priv string }{
		{"attacker", "attacker"},
		{"pinned-public", "wrong"},
		{"wrong", "pinned-private"},
	} {
		if _, err := api.ResolveAPIKey(context.Background(), tc.pub, tc.priv); err == nil {
			t.Fatalf("ResolveAPIKey(%q, %q) succeeded, want 401", tc.pub, tc.priv)
		}
	}
}

func TestResolveLocalCredentialPinsOrGenerates(t *testing.T) {
	cred, err := resolveLocalCredential("pk-1", "sk-1")
	if err != nil {
		t.Fatal(err)
	}
	if cred.publicKey != "pk-1" || cred.privateKey != "sk-1" {
		t.Fatalf("pinned credential = %+v", cred)
	}

	generated, err := resolveLocalCredential("", "")
	if err != nil {
		t.Fatal(err)
	}
	if generated.publicKey == "" || generated.privateKey == "" {
		t.Fatalf("generated credential is incomplete: %+v", generated)
	}
	if generated.publicKey == generated.privateKey {
		t.Fatal("generated halves must differ")
	}
	if !strings.HasPrefix(generated.publicKey, localCredentialKeyPrefix) ||
		!strings.HasPrefix(generated.privateKey, localCredentialKeyPrefix) {
		t.Fatalf("generated credential lacks its prefix: %+v", generated)
	}
	again, err := resolveLocalCredential("", "")
	if err != nil {
		t.Fatal(err)
	}
	if again.publicKey == generated.publicKey {
		t.Fatal("per-process credential must not be a constant")
	}

	if _, err := resolveLocalCredential("only-public", ""); err == nil {
		t.Fatal("expected a half-pinned pair to be rejected")
	}
	if _, err := resolveLocalCredential("pk", "sk\x01"); err == nil {
		t.Fatal("expected a control character to be rejected")
	}
}
