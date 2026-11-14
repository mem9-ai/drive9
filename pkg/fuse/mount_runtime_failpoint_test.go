//go:build failpoint

package fuse

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestMountRuntimeClosesStagingAfterConstructionFailure(t *testing.T) {
	for _, phase := range []string{"recover", "sync-mode"} {
		t.Run(phase, func(t *testing.T) {
			cfg := MountConfig{Server: "http://example.invalid", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core", "writeback"}, WritePolicy: "writeback", SyncMode: "unknown"}
			var ledger *staging.Staging
			var generation uint64
			const point = "github.com/mem9-ai/drive9/pkg/fuse/mountRuntimeStagingReady"
			if err := failpoint.EnableCall(point, func(s *staging.Staging) {
				ledger = s
				if err := s.Shadow.WriteFull("/retained", []byte("recoverable"), 1); err != nil {
					t.Fatal(err)
				}
				generation = s.Shadow.Pin("/retained")
				if generation == 0 {
					t.Fatal("no shadow generation")
				}
				if phase == "recover" {
					// Make the real Recover call fail after all resources exist. Moving
					// rather than closing the journal leaves its descriptor open, so only
					// the constructor's error cleanup can close it.
					path := cfg.CacheDir + "/" + MountHash(cfg.Server, cfg.MountPoint, cfg.RemoteRoot) + "/journal.wal"
					if err := os.Rename(path, path+".moved"); err != nil {
						t.Fatal(err)
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := failpoint.Disable(point); err != nil {
					t.Error(err)
				}
				if ledger != nil {
					ledger.Close()
				}
			})
			runtime, err := buildMountRuntime(cfg, client.New(cfg.Server, ""), "/", "actor")
			if err == nil || runtime != nil || ledger == nil {
				t.Fatalf("runtime=%v err=%v ledger=%v", runtime, err, ledger != nil)
			}
			want := "sync mode"
			if phase == "recover" {
				want = "staging recovery"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("unexpected failure: %v", err)
			}
			if err := ledger.Journal.Append(staging.JournalEntry{Op: staging.JournalCommit, Path: "/after"}); err == nil {
				t.Fatal("failed build left journal writable")
			}
			if _, err := ledger.Shadow.ReadAtGen(generation, 0, make([]byte, 32)); err == nil {
				t.Fatal("failed build retained a live shadow generation")
			}
		})
	}
}

func TestMountRuntimeFailureStopsConstructedWorkersBeforeClosingStaging(t *testing.T) {
	cfg := MountConfig{Server: "http://example.invalid", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core", "sse", "writeback", "cache"}, WritePolicy: "writeback", SyncMode: "strict", WritebackLazyStaging: true, WritebackSyncWindow: time.Hour}
	var ledger *staging.Staging
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	cause := errors.New("facade assembly failed")
	points := []struct {
		name string
		fn   any
	}{
		{"mountRuntimeStagingReady", func(s *staging.Staging) { ledger = s }},
		{"mountRuntimeHandlersReady", func(built map[string]fs.Handler, err *error) {
			if _, ok := built["writeback"]; !ok {
				t.Error("no real writeback")
			}
			if _, ok := built["cache"]; !ok {
				t.Error("no real cache")
			}
			*err = cause
		}},
	}
	for _, point := range points {
		name := "github.com/mem9-ai/drive9/pkg/fuse/" + point.name
		if err := failpoint.EnableCall(name, point.fn); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = failpoint.Disable(name) })
	}
	exitPoint := "github.com/mem9-ai/drive9/pkg/extensions/writeback/writebackSyncLoopExiting"
	if err := failpoint.EnableCall(exitPoint, func() { close(entered); <-release }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(exitPoint) })
	done := make(chan error, 1)
	go func() {
		rt, err := buildMountRuntime(cfg, client.New(cfg.Server, ""), "/", "actor")
		if rt != nil {
			err = errors.New("returned failed runtime")
		}
		done <- err
	}()
	<-entered
	if ledger == nil {
		t.Fatal("no ledger")
	}
	entry := staging.JournalEntry{Op: staging.JournalCommit, Path: "/owned-until-stop"}
	if err := ledger.Journal.Append(entry); err != nil {
		t.Fatal("closed ledger before worker exited", err)
	}
	select {
	case err := <-done:
		t.Fatalf("constructor returned with worker active: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	if err := <-done; !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if err := ledger.Journal.Append(entry); err == nil {
		t.Fatal("failed construction retained open journal")
	}
}
