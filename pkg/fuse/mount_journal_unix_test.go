//go:build !windows

package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountJournalAuthenticatedRecovery(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")
	var tenant atomic.Value
	tenant.Store("tenant-a")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected authentication request: %s", r.URL.Path)
			http.Error(w, "wrong request", 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": tenant.Load()})
	}))
	defer server.Close()
	cfg := MountConfig{Server: server.URL, APIKey: "test-key", RemoteRoot: "/source", MountPoint: t.TempDir()}
	id, err := CreateMountViewJournal(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MountID = id
	if got := mountConfigFromOptions(&MountOptions{MountID: id}); got.MountID != id {
		t.Fatal("public options lost incarnation")
	}
	open := func(cfg MountConfig) (*mountstate.SwitchJournal, error) {
		cl, err := mountClient(cfg)
		if err != nil {
			return nil, err
		}
		return openMountViewJournal(context.Background(), cfg, cl)
	}
	j, err := open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	target := viewprotocol.Binding{LayerID: "a", WorkID: "work", Fence: 1}
	decision, err := j.Decide("switch-a", 1, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	path, err := mountstate.SwitchJournalPath(id)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tenant.Store("tenant-b")
	if other, err := open(cfg); other != nil || !errors.Is(err, mountstate.ErrSwitchJournalInvalid) {
		t.Fatalf("different authenticated filesystem accepted: %v %v", other, err)
	}
	tenant.Store("tenant-a")
	changed := cfg
	changed.RemoteRoot = "/elsewhere"
	if other, err := open(changed); other != nil || !errors.Is(err, mountstate.ErrSwitchJournalInvalid) {
		t.Fatalf("different root accepted: %v %v", other, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("wrong recovery altered journal: %v", err)
	}
	// A symlink to the original mountpoint names the same resolved scope.
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(cfg.MountPoint, alias); err != nil {
		t.Fatal(err)
	}
	changed = cfg
	changed.MountPoint = alias
	j, err = open(changed)
	if err != nil {
		t.Fatal(err)
	}
	s, err := j.Snapshot()
	if err != nil || s.Pending == nil || *s.Pending != decision || s.Binding != target {
		t.Fatalf("recovery lost target: %+v %v", s, err)
	}
	if _, err := j.Complete("switch-a"); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err = j.Snapshot()
	if err != nil || s.Pending != nil || s.Binding != target || s.Generation != 2 {
		t.Fatalf("completed recovery lost target: %+v %v", s, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if other, err := open(cfg); other != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing journal recreated: %v %v", other, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("missing journal appeared: %v", err)
	}
}

func TestMountJournalPreparationFailures(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"tenant_id":""}`))
	}))
	defer server.Close()
	cfg := MountConfig{Server: server.URL, Token: "test-token", MountPoint: t.TempDir()}
	for _, mutate := range []func(*MountConfig){
		func(c *MountConfig) { c.Token = "" },
		func(c *MountConfig) { c.APIKey = "test-key" },
		func(c *MountConfig) { c.MountID = "old-incarnation" },
		func(c *MountConfig) { c.LayerRef = "legacy" },
		func(c *MountConfig) { c.CheckpointRef = "legacy-checkpoint" },
		func(c *MountConfig) { c.MountPoint = "" },
	} {
		bad := cfg
		mutate(&bad)
		if id, err := CreateMountViewJournal(context.Background(), bad); id != "" || err == nil {
			t.Fatalf("invalid preparation: %q %v", id, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CreateMountViewJournal(ctx, cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid preparation made network calls")
	}
	if id, err := CreateMountViewJournal(context.Background(), cfg); id != "" || err == nil {
		t.Fatalf("missing tenant identity accepted: %q %v", id, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("status calls: %d", calls.Load())
	}
	files, err := filepath.Glob(filepath.Join(os.TempDir(), "*", "drive9-view-*.log"))
	if err != nil || len(files) != 0 {
		t.Fatalf("failed preparation wrote journal: %v %v", files, err)
	}
}
