package mountstate

import (
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestViewLocalRootPersistsByLayer(t *testing.T) {
	base := t.TempDir()
	a := viewprotocol.Binding{LayerID: "layer-a", WorkID: "work-a", Fence: 1}
	root, err := ViewLocalRoot(base, a)
	if err != nil || root == base || filepath.Dir(filepath.Dir(root)) != base {
		t.Fatalf("root=%q err=%v", root, err)
	}
	a.WorkID, a.Fence, a.Validation = "work-b", 2, true
	if got, err := ViewLocalRoot(base, a); err != nil || got != root {
		t.Fatalf("new work lost local data: %q %v", got, err)
	}
	a.LayerID = "layer-b"
	if got, err := ViewLocalRoot(base, a); err != nil || got == root {
		t.Fatalf("distinct layer shares directory: %q %v", got, err)
	}
	if got, err := ViewLocalRoot(base, viewprotocol.Binding{}); err != nil || got != base {
		t.Fatalf("main directory changed: %q %v", got, err)
	}
	if got, err := ViewLocalRoot("", a); err != nil || got != "" {
		t.Fatalf("disabled local storage enabled: %q %v", got, err)
	}
	a.LayerID = "../invalid"
	if got, err := ViewLocalRoot(base, a); err == nil || got != "" {
		t.Fatalf("invalid identity selected a directory: %q %v", got, err)
	}
}
