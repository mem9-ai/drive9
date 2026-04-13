package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/tidbcloud"
)

// newNativeProvisionServer returns a minimal *Server suitable for testing
// handleNativeProvision body-validation logic. The provisioner is wired to
// a real *tidbcloudnative.Provisioner so that authorizeNativeTarget succeeds.
func newNativeProvisionServer(t *testing.T) *Server {
	t.Helper()
	global := &stubGlobalClient{}
	account := &stubAccountClient{}
	prov := newTestNativeProvisioner(global, account)
	return &Server{
		provisioner: prov,
		metrics:     newServerMetrics(),
	}
}

func TestHandleNativeProvision_InvalidJSON(t *testing.T) {
	srv := newNativeProvisionServer(t)
	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-1",
		ClusterID:  "cluster-1",
	}

	body := strings.NewReader("{bad json")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/provision", body)
	srv.handleNativeProvision(w, r, target)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusBadRequest)
	}
	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(resp["error"], "invalid request body") {
		t.Fatalf("unexpected error: %s", resp["error"])
	}
}

func TestHandleNativeProvision_MissingUser(t *testing.T) {
	srv := newNativeProvisionServer(t)
	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-1",
		ClusterID:  "cluster-1",
	}

	body, _ := json.Marshal(nativeProvisionRequest{User: "", Password: "pass"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/provision", bytes.NewReader(body))
	srv.handleNativeProvision(w, r, target)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusBadRequest)
	}
	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(resp["error"], "user and password are required") {
		t.Fatalf("unexpected error: %s", resp["error"])
	}
}

func TestHandleNativeProvision_MissingPassword(t *testing.T) {
	srv := newNativeProvisionServer(t)
	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-1",
		ClusterID:  "cluster-1",
	}

	body, _ := json.Marshal(nativeProvisionRequest{User: "root", Password: ""})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/provision", bytes.NewReader(body))
	srv.handleNativeProvision(w, r, target)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusBadRequest)
	}
	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.Contains(resp["error"], "user and password are required") {
		t.Fatalf("unexpected error: %s", resp["error"])
	}
}

func TestHandleNativeProvision_EmptyBody(t *testing.T) {
	srv := newNativeProvisionServer(t)
	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-1",
		ClusterID:  "cluster-1",
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/provision", http.NoBody)
	srv.handleNativeProvision(w, r, target)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleNativeProvision_OversizedBody(t *testing.T) {
	srv := newNativeProvisionServer(t)
	target := &tidbcloud.ResolvedTarget{
		Type:       tidbcloud.TargetZeroInstance,
		InstanceID: "inst-1",
		ClusterID:  "cluster-1",
	}

	// 2 MiB body exceeds the 1 MiB MaxBytesReader limit.
	huge := strings.Repeat("x", 2<<20)
	payload, _ := json.Marshal(map[string]string{"user": huge, "password": "p"})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/provision", bytes.NewReader(payload))
	srv.handleNativeProvision(w, r, target)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d; body=%s", w.Code, http.StatusBadRequest, w.Body.String())
	}
}
