//go:build !windows

package cli

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	drive9fuse "github.com/mem9-ai/drive9/pkg/fuse"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/mountsupervisor"
)

func TestSupervisedMountViewIdentitySurvivesRestartArgs(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/status" {
			t.Errorf("unexpected request: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"tenant_id": "tenant-a"})
	}))
	defer server.Close()
	req := mountSuperviseStartRequest{Server: server.URL, APIKey: "test-key", RemoteRoot: "/", MountPoint: t.TempDir(), OriginalArgs: []string{"--foreground", ":/", "/mnt"}}
	prepared, err := prepareSupervisedMountView(req)
	if err != nil || prepared.MountID == "" || req.MountID != "" || calls.Load() != 1 {
		t.Fatalf("prepare: %+v %v", prepared, err)
	}
	if !reflect.DeepEqual(req.OriginalArgs, []string{"--foreground", ":/", "/mnt"}) {
		t.Fatal("mutated original caller arguments")
	}
	want := "--mount-view-id=" + prepared.MountID
	count := func(args []string) int {
		n := 0
		for _, a := range args {
			if a == want {
				n++
			}
		}
		return n
	}
	for _, args := range [][]string{
		workerArgsForSupervise(prepared.OriginalArgs),
		mountsupervisor.WorkerArgsFromSanitized(sanitizedMountArgs(prepared.OriginalArgs)),
	} {
		if count(args) != 1 {
			t.Fatalf("restart lost/duplicated incarnation: %v", args)
		}
	}
	args, err := superviseCommandArgs(prepared, "/tmp/drive9-test.log")
	if err != nil || count(args) != 1 {
		t.Fatalf("supervisor worker args: %v %v", args, err)
	}
	found := false
	for i, arg := range args {
		if arg == "--sanitized-args-json" && i+1 < len(args) {
			var saved []string
			if err := json.Unmarshal([]byte(args[i+1]), &saved); err != nil || count(saved) != 1 {
				t.Fatalf("adoption arguments: %v %v", saved, err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("missing saved adoption arguments")
	}
	if again, err := prepareSupervisedMountView(prepared); err != nil || !reflect.DeepEqual(again, prepared) || calls.Load() != 1 {
		t.Fatalf("recovery allocated a new incarnation: %+v %v", again, err)
	}
	// A recovery attempt with a missing file must reach the open-only path;
	// the supervisor preparation itself cannot recreate it.
	missing := prepared
	missing.MountID = "missing-journal"
	if got, err := prepareSupervisedMountView(missing); err != nil || got.MountID != missing.MountID || calls.Load() != 1 {
		t.Fatalf("recovery prepared a fresh journal: %+v %v", got, err)
	}
	_, err = drive9fuse.CreateMountViewJournal(context.Background(), drive9fuse.MountConfig{MountID: prepared.MountID})
	if err == nil {
		t.Fatal("explicit recovery ID accepted as new mount")
	}
}

func TestSupervisedMountViewBypassAndFailure(t *testing.T) {
	for _, req := range []mountSuperviseStartRequest{{MountKind: mountstate.MountKindObject}, {LayerRef: "legacy"}, {MountID: "existing"}} {
		got, err := prepareSupervisedMountView(req)
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Fatalf("changed non-new-main preparation: %+v %v", got, err)
		}
	}
	if got, err := prepareSupervisedMountView(mountSuperviseStartRequest{}); err == nil || got.MountID != "" || len(got.OriginalArgs) != 0 {
		t.Fatalf("failed preparation supplied worker identity: %+v %v", got, err)
	}
}

func TestSupervisedMountViewNonLayerCredentialsWithoutOwnerStatusDegradeCleanly(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		apiKey   string
		token    string
		wantFail bool
	}{
		{name: "fs_scoped_403", status: http.StatusForbidden, body: `{"error":"status is owner-only"}`, apiKey: "scoped-key"},
		{name: "capability_token_401", status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`, token: "cap-jwt"},
		{name: "standalone_404", status: http.StatusNotFound, body: "not found", apiKey: "owner-key"},
		{name: "standalone_501", status: http.StatusNotImplemented, body: "not implemented", apiKey: "owner-key"},
		{name: "missing_tenant_200", status: http.StatusOK, body: `{"status":"active"}`, apiKey: "owner-key"},
		{name: "invalid_api_key_401", status: http.StatusUnauthorized, body: `{"error":"invalid api key"}`, apiKey: "bad-key", wantFail: true},
		{name: "transient_503", status: http.StatusServiceUnavailable, body: `{"error":"unavailable"}`, apiKey: "owner-key", wantFail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/status" {
					t.Errorf("unexpected path: %s", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			origArgs := []string{"--foreground", ":/", "/mnt"}
			req := mountSuperviseStartRequest{
				Server:       srv.URL,
				APIKey:       tc.apiKey,
				Token:        tc.token,
				RemoteRoot:   "/",
				MountPoint:   t.TempDir(),
				OriginalArgs: append([]string(nil), origArgs...),
			}
			got, err := prepareSupervisedMountView(req)
			if tc.wantFail {
				if err == nil || got.MountID != "" {
					t.Fatalf("expected hard failure, got=%+v err=%v", got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", tc.name, err)
			}
			if got.MountID != "" || !reflect.DeepEqual(got.OriginalArgs, origArgs) {
				t.Fatalf("unexpected mount-view-id added on degraded main mount: %+v", got)
			}
		})
	}
}

func TestMountViewIdentityFlagIsExplicitAndUnique(t *testing.T) {
	for _, args := range [][]string{{"--mount-view-id="}, {"--mount-view-id=../bad"}, {"--mount-view-id=a", "--mount-view-id=b"}, {"--mount-view-id=a", "--mount-view-id=a"}} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		var id mountViewIDFlag
		fs.Var(&id, "mount-view-id", "")
		if err := fs.Parse(args); err == nil {
			t.Fatalf("accepted ambiguous identity: %v", args)
		}
	}
	for _, args := range [][]string{{"--mount-view-id=original"}, {"--mount-view-id", "original"}} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		var id mountViewIDFlag
		fs.Var(&id, "mount-view-id", "")
		if err := fs.Parse(args); err != nil || strings.TrimSpace(id.String()) != "original" {
			t.Fatalf("identity parse: %q %v", id, err)
		}
	}
}
