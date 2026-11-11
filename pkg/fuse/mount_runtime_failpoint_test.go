//go:build failpoint

package fuse

import (
	"os"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/client"
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
