//go:build !windows

package mountstate

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func switchTestJournal(t *testing.T) (*SwitchJournal, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "switch.log")
	j, err := CreateSwitchJournal(path, "mount-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j, path
}

func switchTarget(id string) viewprotocol.Binding {
	return viewprotocol.Binding{LayerID: id, WorkID: "work-1", Fence: 1}
}

func TestSwitchJournalRecoverAndReplay(t *testing.T) {
	j, path := switchTestJournal(t)
	a := switchTarget("a")
	r, err := j.Decide("op-a", 1, a)
	if err != nil || r.State != SwitchRecovering || r.Generation != 2 {
		t.Fatalf("decision: %+v %v", r, err)
	}
	if _, err := j.Decide("op-b", 2, switchTarget("b")); !errors.Is(err, ErrSwitchPending) {
		t.Fatalf("pending: %v", err)
	}
	if _, err := j.Decide("op-a", 1, switchTarget("b")); !errors.Is(err, ErrSwitchConflict) {
		t.Fatalf("digest: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	s, err := j.Snapshot()
	if err != nil || s.Binding != a || s.Generation != 2 || s.Pending == nil || *s.Pending != r {
		t.Fatalf("recovered: %+v %v", s, err)
	}
	s.Pending.Target = switchTarget("tampered")
	fresh, _ := j.Snapshot()
	if fresh.Pending.Target != a {
		t.Fatal("snapshot aliases journal")
	}
	done, err := j.Complete("op-a")
	if err != nil || done.State != SwitchDone {
		t.Fatalf("complete: %+v %v", done, err)
	}
	if _, err := j.Decide("op-b", 2, switchTarget("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Complete("op-b"); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	old, err := j.Decide("op-a", 1, a)
	if err != nil || old != done {
		t.Fatalf("old replay: %+v %v", old, err)
	}
	old, err = j.Complete("op-a")
	if err != nil || old != done {
		t.Fatalf("old completion: %+v %v", old, err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	s, _ = j.Snapshot()
	if before.Size() != after.Size() || s.Binding != switchTarget("b") || s.Generation != 3 || s.Pending != nil {
		t.Fatalf("replay changed target: %+v", s)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	s, _ = j.Snapshot()
	if s.Generation != 3 || s.Pending != nil || s.Binding != switchTarget("b") {
		t.Fatalf("done recovery: %+v", s)
	}
}

func TestSwitchJournalExclusiveOwnerAndIncarnation(t *testing.T) {
	j, path := switchTestJournal(t)
	if other, err := OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{}); err == nil {
		_ = other.Close()
		t.Fatal("two writers")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id      string
		initial viewprotocol.Binding
	}{
		{"mount-2", viewprotocol.Binding{}}, {"mount-1", switchTarget("a")},
	} {
		if other, err := OpenSwitchJournal(path, tc.id, tc.initial); !errors.Is(err, ErrSwitchJournalInvalid) {
			if other != nil {
				_ = other.Close()
			}
			t.Fatalf("accepted foreign incarnation: %v", err)
		}
	}
}

type switchFaultWriter struct {
	file     *os.File
	short    bool
	failSync bool
}

func (w switchFaultWriter) Write(p []byte) (int, error) {
	if w.short {
		return w.file.Write(p[:len(p)/2])
	}
	return w.file.Write(p)
}
func (w switchFaultWriter) Sync() error {
	if w.failSync {
		return io.ErrClosedPipe
	}
	return w.file.Sync()
}

func TestSwitchJournalUnknownDecisionRemainsClosed(t *testing.T) {
	for _, phase := range []string{"decision", "completion"} {
		t.Run(phase, func(t *testing.T) {
			j, path := switchTestJournal(t)
			if phase == "completion" {
				if _, err := j.Decide("op-a", 1, switchTarget("a")); err != nil {
					t.Fatal(err)
				}
			}
			j.writer = switchFaultWriter{file: j.file, failSync: true}
			var err error
			if phase == "decision" {
				_, err = j.Decide("op-a", 1, switchTarget("a"))
			} else {
				_, err = j.Complete("op-a")
			}
			if !errors.Is(err, ErrSwitchUncertain) {
				t.Fatalf("unknown: %v", err)
			}
			if _, err := j.Snapshot(); !errors.Is(err, ErrSwitchUncertain) {
				t.Fatalf("snapshot assumed outcome: %v", err)
			}
			if _, _, err := j.Lookup("op-a"); !errors.Is(err, ErrSwitchUncertain) {
				t.Fatalf("lookup assumed outcome: %v", err)
			}
			if _, err := j.Decide("retry", 1, switchTarget("b")); !errors.Is(err, ErrSwitchUncertain) {
				t.Fatalf("accepted retry: %v", err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			j, err = OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = j.Close() }()
			s, _ := j.Snapshot()
			if s.Binding != switchTarget("a") || s.Generation != 2 {
				t.Fatalf("lost target: %+v", s)
			}
			if (s.Pending != nil) != (phase == "decision") {
				t.Fatalf("recovery phase: %+v", s)
			}
		})
	}
}

func TestSwitchJournalTornOrCorruptTailDoesNotFallback(t *testing.T) {
	for _, fault := range []string{"short", "checksum", "empty", "unknown_version"} {
		t.Run(fault, func(t *testing.T) {
			j, path := switchTestJournal(t)
			if fault == "short" {
				j.writer = switchFaultWriter{file: j.file, short: true}
			}
			_, err := j.Decide("op-a", 1, switchTarget("a"))
			if fault == "short" && !errors.Is(err, ErrSwitchUncertain) {
				t.Fatal(err)
			}
			if fault != "short" && err != nil {
				t.Fatal(err)
			}
			if fault == "unknown_version" {
				if err := j.append(switchRecord{Version: 99, Kind: "complete"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if fault == "checksum" || fault == "empty" {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if fault == "checksum" {
					data[len(data)-1] ^= 1
				} else {
					data = nil
				}
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			other, err := OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{})
			if other != nil {
				_ = other.Close()
			}
			if !errors.Is(err, ErrSwitchJournalInvalid) {
				t.Fatalf("accepted corrupt log: %v", err)
			}
		})
	}
}

func TestSwitchJournalDirectorySyncAndSafePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal")
	_, err := openSwitchJournal(path, "mount-1", viewprotocol.Binding{}, true, func(string) error { return io.ErrClosedPipe })
	if !errors.Is(err, ErrSwitchUncertain) {
		t.Fatalf("directory durability: %v", err)
	}
	j, err := OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if j, err := OpenSwitchJournal(link, "mount-1", viewprotocol.Binding{}); err == nil {
		_ = j.Close()
		t.Fatal("followed symlink")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if j, err := OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{}); err == nil {
		_ = j.Close()
		t.Fatal("accepted public journal")
	}
}

func TestSwitchJournalConcurrentDecisionsAndBounds(t *testing.T) {
	j, path := switchTestJournal(t)
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := j.Decide("same", 1, switchTarget("a")); results <- err }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := j.Complete("same"); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	for _, tc := range []struct {
		id     string
		gen    uint64
		target viewprotocol.Binding
	}{
		{"bad/id", 2, switchTarget("a")}, {"zero", 0, switchTarget("a")}, {"stale", 1, switchTarget("a")},
		{"invalid", 2, viewprotocol.Binding{LayerID: "a"}},
	} {
		if _, err := j.Decide(tc.id, tc.gen, tc.target); err == nil {
			t.Fatalf("accepted %+v", tc)
		}
	}
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Fatal("rejected request appended")
	}
	j.bytes = maxSwitchJournalBytes - 2*(maxSwitchRecordBytes+4+32)
	if _, err := j.Decide("last", 2, viewprotocol.Binding{}); err != nil {
		t.Fatalf("allowed boundary: %v", err)
	}
	if _, err := j.Complete("last"); err != nil {
		t.Fatalf("reserved completion: %v", err)
	}
	j.bytes = maxSwitchJournalBytes - 2*(maxSwitchRecordBytes+4+32) + 1
	if _, err := j.Decide("over", 3, switchTarget("b")); !errors.Is(err, ErrSwitchJournalLimit) {
		t.Fatalf("limit: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Snapshot(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestSwitchJournalCreateAndRecoveryNeverReplaceMissingFacts(t *testing.T) {
	j, path := switchTestJournal(t)
	if _, err := j.Decide("op-a", 1, switchTarget("a")); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := CreateSwitchJournal(path, "mount-2", viewprotocol.Binding{}); !os.IsExist(err) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("overwrote old journal: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(original) {
		t.Fatalf("old journal changed: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenSwitchJournal(path, "mount-1", viewprotocol.Binding{}); !os.IsNotExist(err) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("missing recovery initialized main: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("recovery created file: %v", err)
	}
}
