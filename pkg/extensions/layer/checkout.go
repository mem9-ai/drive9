// Hot checkout: switch the mounted shadow (layer, checkpoint, or live)
// without umount. Open dirty handles keep flushing to the tree paths they
// were opened on — writes after the switch land in the new view.
package layer

import (
	"context"
	"fmt"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// CheckoutTarget selects the next shadow.
type CheckoutTarget struct {
	// LayerRef resolves a layer (id, name, or tag ref); "" with Live or
	// CheckpointRef unset is invalid.
	LayerRef string
	// CheckpointRef pins a checkpoint of LayerRef (read-only view).
	CheckpointRef string
	// Live returns to the classic view.
	Live bool
}

// Checkout hot-switches the mount's shadow view. It resolves the refs
// through the metadata API, walks the new view's marker roots into a fresh
// index, swaps the view, and flushes the kernel caches.
func (l *Layer) Checkout(ctx context.Context, target CheckoutTarget) error {
	if target.Live {
		l.swapView(newLiveView())
		return nil
	}
	if target.LayerRef == "" {
		return fmt.Errorf("layer checkout: empty layer ref")
	}
	layer, err := l.reg.GetLayer(ctx, target.LayerRef)
	if err != nil {
		return fmt.Errorf("resolve layer %s: %w", target.LayerRef, err)
	}
	// Refuse to shadow terminal layers: their trees are settled.
	switch layer.State {
	case "", "active", "sealed", "committing":
	default:
		return fmt.Errorf("layer %s is %s; fork or create a new layer instead", layer.LayerID, layer.State)
	}
	nv := &view{mode: viewLayer, layerID: layer.LayerID}
	nv.applyRoots(layerRootPath(layer.LayerID))
	if target.CheckpointRef != "" {
		cp, cerr := l.reg.GetCheckpoint(ctx, target.CheckpointRef)
		if cerr != nil {
			return fmt.Errorf("resolve checkpoint %s: %w", target.CheckpointRef, cerr)
		}
		if cp.LayerID != layer.LayerID {
			return fmt.Errorf("checkpoint %s belongs to layer %s, not %s", cp.CheckpointID, cp.LayerID, layer.LayerID)
		}
		nv.mode = viewCheckpoint
		nv.checkpoint = cp.CheckpointID
		nv.readOnly = true
		nv.applyRoots(layerRootPath(layer.LayerID) + "/checkpoints/" + cp.CheckpointID)
	}
	idx, err := buildViewIndex(ctx, l.io, nv, pathMapper{
		serverToView: l.viewPathOf,
		viewToServer: l.serverAbs,
	})
	if err != nil {
		return fmt.Errorf("build layer view: %w", err)
	}
	nv.idx = idx
	l.swapView(nv)
	return nil
}

// applyRoots derives the marker roots from a layer (or checkpoint) root.
func (v *view) applyRoots(base string) {
	v.treeRoot = base + "/" + treeSubdir
	v.blackRoot = base + "/" + blackoutSubdir
	v.blackDirRoot = base + "/" + blackoutDirSubdir
	v.casRoot = base + "/" + casSubdir
	v.attrsRoot = base + "/" + attrsSubdir
}

// swapView installs nv, fences stale lookups with a generation bump, and
// resets the kernel caches so every entry resolves through the new view.
func (l *Layer) swapView(nv *view) {
	l.mu.Lock()
	l.cur = nv
	l.mu.Unlock()
	l.gen.Add(1)
	if l.notify != nil {
		l.notify.InvalidateAll()
	}
	l.emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
}
