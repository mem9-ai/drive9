//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// This subprocess exits without draining or closing the runtime. It leaves
// only fsync'ed bytes and WAL records, as a foreground process crash would.
func TestMountMainRecoveryWriterProcess(t *testing.T) {
	raw := os.Getenv("DRIVE9_TEST_MAIN_RECOVERY_CONFIG")
	if raw == "" {
		t.Skip("subprocess fixture")
	}
	var cfg MountConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	cl, err := mountClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var runtime *mountRuntime
	if os.Getenv("DRIVE9_TEST_MAIN_RECOVERY_LEGACY") == "1" {
		runtime, err = buildMountRuntime(cfg, cl, "/", "old-process")
	} else {
		var v *mountView
		v, err = prepareMountView(context.Background(), cfg, cl, "old-process")
		if err == nil {
			runtime = v.currentRuntime()
			s, snapshotErr := v.switcher.journal.Snapshot()
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			if err := os.WriteFile(filepath.Join(cfg.CacheDir, "previous-mount-id"), []byte(s.MountID), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.staging.Shadow.WriteFull("/pending", []byte("durable pending"), 7); err != nil {
		t.Fatal(err)
	}
	if err := runtime.staging.Shadow.Sync("/pending"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.staging.Journal.Append(staging.JournalEntry{Op: staging.JournalFsync, Path: "/pending", Length: 15, BaseRev: 7}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.staging.Journal.Fsync(); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

func TestMountMainStartupRecoversBeforeSwitching(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "foreground-restart", true: "upgrade-original-cache"}[legacy], func(t *testing.T) {
			cfg, cl, _, _ := mountViewFixture(t)
			cfg.Extensions = []string{"core", "writeback"}
			cfg.WritePolicy = "writeback"
			cfg.WriteCacheFreeRatio = -1 // Exercise recovery independently of host disk usage.
			raw, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestMountMainRecoveryWriterProcess$")
			cmd.Env = append(os.Environ(), "DRIVE9_TEST_MAIN_RECOVERY_CONFIG="+string(raw), "DRIVE9_TEST_MAIN_RECOVERY_LEGACY="+map[bool]string{false: "0", true: "1"}[legacy])
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("writer subprocess: %v\n%s", err, output)
			}
			// The user repeats the original command: no recovery ID is supplied.
			v, err := prepareMountView(t.Context(), cfg, cl, "new-process")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = v.close(context.Background()) })
			snapshot, err := v.switcher.journal.Snapshot()
			if err != nil || snapshot.Binding.LayerID != "" || snapshot.Generation != 1 {
				t.Fatalf("initial main: %+v %v", snapshot, err)
			}
			if !legacy {
				previous, err := os.ReadFile(filepath.Join(cfg.CacheDir, "previous-mount-id"))
				if err != nil || string(previous) == snapshot.MountID {
					t.Fatalf("test did not create a new incarnation: %v", err)
				}
			}
			runtime := v.currentRuntime()
			meta, exists := runtime.staging.Pending.GetMeta("/pending")
			if !exists || meta.BaseRev != 7 || meta.Size != 15 {
				t.Fatalf("durable pending write was abandoned: %+v exists=%v", meta, exists)
			}
			data := make([]byte, 15)
			if n, err := runtime.staging.Shadow.ReadAt("/pending", 0, data); err != nil || n != 15 || string(data) != "durable pending" {
				t.Fatalf("lost pending bytes: %q n=%d err=%v", data, n, err)
			}
		})
	}
}
