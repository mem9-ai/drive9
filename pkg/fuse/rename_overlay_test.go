package fuse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newRenameOverlayTestFS(t *testing.T, targetExists bool, onRename func()) (*Dat9FS, string, *atomic.Int32) {
	t.Helper()
	var renameCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if !targetExists && r.URL.Path == "/v1/fs/final" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("X-Dat9-IsDir", "true")
			w.Header().Set("Content-Length", "0")
		case http.MethodGet:
			_, _ = io.WriteString(w, `{"files":[]}`)
		case http.MethodPost:
			if r.URL.RawQuery == "rename" {
				renameCalls.Add(1)
				if onRename != nil {
					onRename()
				}
			}
		}
	}))
	t.Cleanup(ts.Close)
	root := t.TempDir()
	opts := &MountOptions{Profile: MountProfileCodingAgent, LocalRoot: root}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	fs.inodes.Lookup("/staging", true, 0, time.Now())
	return fs, root, &renameCalls
}

func TestRenameDirectoryRejectsNonEmptyLocalOverlayTarget(t *testing.T) {
	for _, targetExists := range []bool{false, true} {
		name := "missing_remote_target"
		if targetExists {
			name = "empty_remote_target"
		}
		t.Run(name, func(t *testing.T) {
			fs, root, calls := newRenameOverlayTestFS(t, targetExists, nil)
			// Populate only the backing overlay, as after remount: no cached
			// child inode may make the destination appear non-empty for us.
			files := map[string]string{
				"/staging/dist/source.js":       "source",
				"/final/node_modules/target.js": "target",
			}
			for p, content := range files {
				f, err := fs.localOverlay.OpenFile(p, syscall.O_CREAT|syscall.O_WRONLY, 0o644)
				if err != nil {
					t.Fatal(err)
				}
				_, writeErr := f.WriteString(content)
				closeErr := f.Close()
				if writeErr != nil || closeErr != nil {
					t.Fatalf("write %s: %v, close: %v", p, writeErr, closeErr)
				}
			}
			st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "staging", "final")
			if st != gofuse.Status(syscall.ENOTEMPTY) {
				t.Errorf("Rename = %v, want ENOTEMPTY", st)
			}
			if calls.Load() != 0 {
				t.Errorf("issued %d remote renames for a non-empty target", calls.Load())
			}
			for p, want := range files {
				data, err := os.ReadFile(root + "/overlay" + p)
				if err != nil || string(data) != want {
					t.Errorf("preserved %s = %q, %v; want %q", p, data, err, want)
				}
			}
		})
	}
}

func TestRenameLocalOverlaySubtreeEmptyDirectories(t *testing.T) {
	for _, tc := range []struct {
		name    string
		source  bool
		content bool
		target  bool
	}{
		{name: "missing_source"},
		{name: "empty_source", source: true},
		{name: "missing_target", source: true, content: true},
		{name: "empty_target", source: true, content: true, target: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, root, calls := newRenameOverlayTestFS(t, true, nil)
			if tc.source {
				if err := os.MkdirAll(root+"/overlay/staging", 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if tc.content {
				if err := os.MkdirAll(root+"/overlay/staging/dist", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(root+"/overlay/staging/dist/file", []byte("payload"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if tc.target {
				if err := os.MkdirAll(root+"/overlay/final", 0o755); err != nil {
					t.Fatal(err)
				}
			}
			st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "staging", "final")
			if st != gofuse.OK || calls.Load() != 1 {
				t.Fatalf("Rename = %v, remote calls = %d", st, calls.Load())
			}
			if tc.content {
				data, err := os.ReadFile(root + "/overlay/final/dist/file")
				if err != nil || string(data) != "payload" {
					t.Fatalf("moved content = %q, %v", data, err)
				}
			}
		})
	}
}

func TestRenameLocalOverlaySubtreePreservesRacingTarget(t *testing.T) {
	fs, root, _ := newRenameOverlayTestFS(t, true, nil)
	for _, dir := range []string{"staging", "final"} {
		if err := os.MkdirAll(root+"/overlay/"+dir+"/dist", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(root+"/overlay/"+dir+"/dist/file", []byte(dir), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Call the post-commit move directly: a destination child may have been
	// created after preflight. The move itself must never delete that child.
	if err := fs.renameLocalOverlaySubtree("/staging", "/final"); err == nil {
		t.Fatal("overlay move replaced a non-empty destination")
	}
	for _, dir := range []string{"staging", "final"} {
		data, err := os.ReadFile(root + "/overlay/" + dir + "/dist/file")
		if err != nil || string(data) != dir {
			t.Errorf("preserved %s = %q, %v", dir, data, err)
		}
	}
}

func TestRenameDirectoryPrunesNestedEmptyOverlayParents(t *testing.T) {
	for _, remoteTarget := range []bool{false, true} {
		name := "missing_remote_target"
		if remoteTarget {
			name = "empty_remote_target"
		}
		t.Run(name, func(t *testing.T) {
			fs, root, calls := newRenameOverlayTestFS(t, remoteTarget, nil)
			for _, p := range []string{"/staging/dist", "/final/a/b/c", "/final/d/e"} {
				if err := os.MkdirAll(root+"/overlay"+p, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(root+"/overlay/staging/dist/file", []byte("source"), 0o644); err != nil {
				t.Fatal(err)
			}
			st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "staging", "final")
			if st != gofuse.OK || calls.Load() != 1 {
				t.Fatalf("Rename = %v, remote calls = %d", st, calls.Load())
			}
			entries, err := os.ReadDir(root + "/overlay/final")
			if err != nil || len(entries) != 1 || entries[0].Name() != "dist" {
				t.Fatalf("final overlay = %v, %v; want source dist only", entries, err)
			}
			data, err := os.ReadFile(root + "/overlay/final/dist/file")
			if err != nil || string(data) != "source" {
				t.Fatalf("source content = %q, %v", data, err)
			}
		})
	}
}

func TestRenameDirectoryPreservesRealOverlayEntries(t *testing.T) {
	for _, kind := range []string{"empty_local_dir", "nested_local_dir", "hidden_file", "child_symlink", "target_symlink", "parent_symlink"} {
		t.Run(kind, func(t *testing.T) {
			fs, root, calls := newRenameOverlayTestFS(t, true, nil)
			if err := os.MkdirAll(root+"/overlay/staging/dist", 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(root+"/overlay/staging/dist/file", []byte("source"), 0o644); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			if err := os.MkdirAll(outside+"/a/b", 0o755); err != nil {
				t.Fatal(err)
			}
			final := root + "/overlay/final"
			protected := final + "/nested"
			if kind != "target_symlink" && kind != "parent_symlink" {
				if err := os.MkdirAll(final+"/nested", 0o755); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch kind {
			case "empty_local_dir":
				protected = final + "/dist"
				err = os.Mkdir(protected, 0o755)
			case "nested_local_dir":
				protected = final + "/nested/dist"
				err = os.Mkdir(protected, 0o755)
			case "hidden_file":
				protected = final + "/nested/file"
				err = os.WriteFile(protected, []byte("keep"), 0o644)
			case "child_symlink":
				protected = final + "/nested/link"
				err = os.Symlink(outside, protected)
			case "target_symlink", "parent_symlink":
				protected = final
				err = os.Symlink(outside, protected)
			}
			if err != nil {
				t.Fatal(err)
			}
			input := &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}
			newName := "final"
			if kind == "parent_symlink" {
				input.Newdir = fs.inodes.Lookup("/final", true, 0, time.Now())
				newName = "a"
			}
			st := fs.Rename(nil, input, "staging", newName)
			if st == gofuse.OK || calls.Load() != 0 {
				t.Errorf("Rename = %v, remote calls = %d; expected preflight rejection", st, calls.Load())
			}
			if _, err := os.Lstat(protected); err != nil {
				t.Errorf("protected target removed: %v", err)
			}
			if kind == "hidden_file" {
				if data, err := os.ReadFile(protected); err != nil || string(data) != "keep" {
					t.Errorf("protected content = %q, %v", data, err)
				}
			}
			if _, err := os.Stat(outside + "/a/b"); err != nil {
				t.Errorf("followed symlink and removed external empty directories: %v", err)
			}
			if data, err := os.ReadFile(root + "/overlay/staging/dist/file"); err != nil || string(data) != "source" {
				t.Errorf("source content = %q, %v", data, err)
			}
		})
	}
}

func TestRenameDirectoryOverlayFailureReconcilesCommittedState(t *testing.T) {
	for _, recoverOverlay := range []bool{false, true} {
		name := "persistent_failure"
		if recoverOverlay {
			name = "retry_after_reconciliation"
		}
		t.Run(name, func(t *testing.T) {
			var obstacle string
			fs, root, calls := newRenameOverlayTestFS(t, true, func() {
				// After preflight and remote commit, make an overlay parent a
				// file. This fails the local move even when tests run as root.
				if err := os.WriteFile(obstacle, []byte("obstacle"), 0o644); err != nil {
					t.Error(err)
				}
			})
			obstacle = root + "/overlay/parent"
			if err := os.MkdirAll(root+"/overlay/staging/dist", 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(root+"/overlay/staging/dist/file", []byte("local"), 0o644); err != nil {
				t.Fatal(err)
			}
			var err error
			fs.writeBack, err = NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.pendingIndex, err = NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.shadowStore, err = NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(fs.shadowStore.Close)
			const oldPath = "/staging/pending.txt"
			const newPath = "/parent/final/pending.txt"
			if err := fs.writeBack.Put(oldPath, []byte("pending"), 7, PendingNew); err != nil {
				t.Fatal(err)
			}
			if _, err := fs.pendingIndex.Put(oldPath, 7, PendingNew); err != nil {
				t.Fatal(err)
			}
			if err := fs.shadowStore.WriteFull(oldPath, []byte("pending"), 0); err != nil {
				t.Fatal(err)
			}
			ino := fs.inodes.Lookup(oldPath, false, 7, time.Now())
			fh := &FileHandle{Path: oldPath, Ino: ino}
			fs.openHandles.Add(fh)
			parent := fs.inodes.Lookup("/parent", true, 0, time.Now())
			input := &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: parent}
			var st gofuse.Status
			if recoverOverlay {
				fh.Lock()
				done := make(chan gofuse.Status, 1)
				go func() { done <- fs.Rename(nil, input, "staging", "final") }()
				// The index moves immediately before retarget waits for fh.mu.
				// At this point the first overlay attempt has already failed.
				deadline := time.Now().Add(5 * time.Second)
				for {
					fs.openHandles.mu.RLock()
					retargeted := fs.openHandles.pathByHandle[fh] == newPath
					fs.openHandles.mu.RUnlock()
					if retargeted {
						break
					}
					select {
					case got := <-done:
						fh.Unlock()
						t.Fatalf("Rename returned %v before reconciliation", got)
					default:
					}
					if time.Now().After(deadline) {
						fh.Unlock()
						t.Fatal("Rename did not reach handle reconciliation")
					}
					runtime.Gosched()
				}
				err := os.Remove(obstacle)
				fh.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				select {
				case st = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("Rename did not complete")
				}
			} else {
				st = fs.Rename(nil, input, "staging", "final")
			}
			wantStatus := gofuse.Status(syscall.ENOTDIR)
			if recoverOverlay {
				wantStatus = gofuse.OK
			}
			if st != wantStatus || calls.Load() != 1 {
				t.Errorf("Rename = %v, want %v; remote calls = %d", st, wantStatus, calls.Load())
			}
			if p, _ := fs.inodes.GetPath(ino); p != newPath || fh.Path != newPath {
				t.Errorf("committed inode path = %q, handle path = %q; want %q", p, fh.Path, newPath)
			}
			if data, ok := fs.writeBack.Get(newPath); !ok || string(data) != "pending" {
				t.Errorf("new writeback = %q, present = %t", data, ok)
			}
			if _, ok := fs.pendingIndex.GetMeta(newPath); !ok {
				t.Error("pending metadata did not follow committed rename")
			}
			if data, err := fs.shadowStore.ReadAll(newPath); err != nil || string(data) != "pending" {
				t.Errorf("new shadow = %q, %v", data, err)
			}
			if _, ok := fs.writeBack.GetMeta(oldPath); ok {
				t.Error("old writeback remains after migration")
			}
			if _, ok := fs.pendingIndex.GetMeta(oldPath); ok || fs.shadowStore.Has(oldPath) {
				t.Error("old pending metadata or shadow remains after migration")
			}
			overlayPath := root + "/overlay/staging/dist/file"
			if recoverOverlay {
				overlayPath = root + "/overlay/parent/final/dist/file"
			}
			if data, err := os.ReadFile(overlayPath); err != nil || string(data) != "local" {
				t.Errorf("preserved overlay = %q, %v", data, err)
			}
		})
	}
}

func TestRenameDirectoryPrunesEmptyOverlayParentsAfterRmdir(t *testing.T) {
	fs, root, calls := newRenameOverlayTestFS(t, true, nil)
	// These backing parents are automatically created by local-only mkdir.
	if err := os.MkdirAll(root+"/overlay/final/nested/dist", 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root+"/overlay/staging/dist", 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root+"/overlay/staging/dist/file", []byte("source"), 0644); err != nil {
		t.Fatal(err)
	}
	finalIno := fs.inodes.Lookup("/final", true, 0, time.Now())
	nestedIno := fs.inodes.Lookup("/final/nested", true, 0, time.Now())
	fs.inodes.Lookup("/final/nested/dist", true, 0, time.Now())
	if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: nestedIno}, "dist"); st != gofuse.OK {
		t.Fatalf("rmdir local dist: %v", st)
	}
	if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: finalIno}, "nested"); st != gofuse.OK {
		t.Fatalf("rmdir remote nested: %v", st)
	}
	visible, err := fs.listDir(context.Background(), "/final")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := fs.localOverlay.ReadDir("/final")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after successful rmdirs: visible_entries=%d raw_overlay_entries=%d", len(visible), len(raw))
	if len(visible) != 0 {
		t.Fatalf("destination not logically empty: %v", visible)
	}
	st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "staging", "final")
	t.Logf("rename_status=%v remote_rename_calls=%d", st, calls.Load())
	if st != gofuse.OK {
		t.Fatalf("empty destination rename rejected because of hidden backing scaffolding: %v", st)
	}
	if calls.Load() != 1 {
		t.Fatalf("remote rename calls = %d, want 1", calls.Load())
	}
	if data, err := os.ReadFile(root + "/overlay/final/dist/file"); err != nil || string(data) != "source" {
		t.Fatalf("renamed overlay content = %q, %v", data, err)
	}
	if _, err := os.Stat(root + "/overlay/final/nested"); !os.IsNotExist(err) {
		t.Fatalf("empty backing parent remains: %v", err)
	}
}
