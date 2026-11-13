package localize

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestViewSwitchAllowsPersistentFilesButKeepsUnsafeState(t *testing.T) {
	for _, mode := range []string{"persistent", "open", "special", "transient", "unreadable"} {
		t.Run(mode, func(t *testing.T) {
			f, err := New(Options{LocalRoot: t.TempDir(), TransientRoot: t.TempDir(), LocalOnlyPatterns: []string{"**/node_modules/**"}})
			if err != nil {
				t.Fatal(err)
			}
			h, err := f.overlay.OpenFile("/node_modules/keep", syscall.O_CREAT|syscall.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "open":
				f.noteDBOpen("/node_modules/keep")
			case "special":
				f.createLocalSpecial("/node_modules/fifo", fs.Attr{Kind: fs.KindFile, Mode: syscall.S_IFIFO | 0600})
			case "transient":
				if err := os.WriteFile(filepath.Join(f.transient.Root(), "a.db-shm"), []byte("shared"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unreadable":
				if err := os.Remove(f.transient.Root()); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.transient.Root(), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			reason, n := f.ViewSwitchRefusal()
			if mode == "persistent" {
				if n != 0 || reason != "" {
					t.Fatalf("persistent data blocked: %s %d", reason, n)
				}
			} else if n == 0 || reason == "" {
				t.Fatalf("unsafe state accepted: %s %d", reason, n)
			}
			if _, n := f.ReexecRefusal(); n == 0 {
				t.Fatal("process re-exec restriction was relaxed")
			}
		})
	}
}
