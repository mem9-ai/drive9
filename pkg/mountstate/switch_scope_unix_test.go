//go:build !windows

package mountstate

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestSwitchScopeRecovery(t *testing.T) {
	args := [4]string{"https://server.invalid", "tenant-a", "/source/", "/mount/point"}
	scope, err := SwitchScope(args[0], args[1], args[2], args[3])
	if err != nil || len(scope) != 64 {
		t.Fatalf("scope %q: %v", scope, err)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "incarnation.log")
	j, err := CreateSwitchJournal(path, "incarnation", scope, viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := j.Decide("op-a", 1, switchTarget("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range []string{"https://other.invalid", "tenant-b", "/other/", "/other/mount"} {
		t.Run(value, func(t *testing.T) {
			changed := args
			changed[i] = value
			otherScope, err := SwitchScope(changed[0], changed[1], changed[2], changed[3])
			if err != nil || otherScope == scope {
				t.Fatalf("changed scope: %q %v", otherScope, err)
			}
			other, err := OpenSwitchJournal(path, "incarnation", otherScope, viewprotocol.Binding{})
			if other != nil || !errors.Is(err, ErrSwitchJournalInvalid) {
				t.Fatalf("wrong scope opened journal: %v %v", other, err)
			}
		})
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("rejected recovery changed journal: %v", err)
	}
	j, err = OpenSwitchJournal(path, "incarnation", scope, viewprotocol.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	s, err := j.Snapshot()
	if err != nil || s.Pending == nil || *s.Pending != r || s.Binding != r.Target {
		t.Fatalf("lost decided target: %+v %v", s, err)
	}
}

func TestSwitchScopeInputsAndPath(t *testing.T) {
	scope, err := SwitchScope("https://server.invalid/", "tenant", "/source", "/mount/point/")
	canonical, canonicalErr := SwitchScope("https://server.invalid", "tenant", "/source/", "/mount/point")
	if err != nil || canonicalErr != nil || scope != canonical {
		t.Fatalf("equivalent scope mismatch: %v %v", err, canonicalErr)
	}
	for _, args := range [][4]string{
		{"", "tenant", "/", "/mount"},
		{"https://server.invalid", "", "/", "/mount"},
		{"https://server.invalid", " tenant", "/", "/mount"},
		{"https://server.invalid", "tenant", "/../outside", "/mount"},
		{"https://server.invalid", "tenant", "/", "relative"},
	} {
		if got, err := SwitchScope(args[0], args[1], args[2], args[3]); err == nil || got != "" {
			t.Fatalf("accepted invalid scope: %q %v", got, err)
		}
	}
	for _, id := range []string{"", "../escape", "/absolute"} {
		if _, err := SwitchJournalPath(id); err == nil {
			t.Fatalf("accepted invalid incarnation %q", id)
		}
	}
	p, err := SwitchJournalPath("incarnation-a")
	q, otherErr := SwitchJournalPath("incarnation-b")
	if err != nil || otherErr != nil || p == q || filepath.Dir(p) != stateDir() {
		t.Fatalf("journal paths: %q %q %v %v", p, q, err, otherErr)
	}
	// No directory or journal is created by path derivation.
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("path derivation created a journal: %v", err)
	}
}
