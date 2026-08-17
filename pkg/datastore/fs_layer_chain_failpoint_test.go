//go:build failpoint

package datastore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
)

const fsLayerDeleteAfterPinsCheckFailpoint = "github.com/mem9-ai/drive9/pkg/datastore/fsLayerDeleteAfterPinsCheck"

func TestDeleteFSLayerSerializesConcurrentForkAfterPinsCheck(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateFSLayer(ctx, &FSLayer{LayerID: "p-del-race", BaseRootPath: "/repo"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	var (
		mu      sync.Mutex
		forkErr error
	)
	forkDone := make(chan struct{})
	if err := failpoint.EnableCall(fsLayerDeleteAfterPinsCheckFailpoint, func(layerID string) {
		if layerID != "p-del-race" {
			return
		}
		go func() {
			defer close(forkDone)
			_, err := s.ForkFSLayer(ctx, FSLayerForkOptions{
				ChildLayerID:  "c-del-race",
				ParentLayerID: "p-del-race",
			})
			mu.Lock()
			forkErr = err
			mu.Unlock()
		}()
	}); err != nil {
		t.Fatalf("enable failpoint: %v", err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(fsLayerDeleteAfterPinsCheckFailpoint) })

	if err := s.DeleteFSLayer(ctx, "p-del-race", DeleteFSLayerOptions{}); err != nil {
		t.Fatalf("DeleteFSLayer: %v", err)
	}
	<-forkDone

	mu.Lock()
	gotForkErr := forkErr
	mu.Unlock()
	if !errors.Is(gotForkErr, ErrFSLayerStateConflict) {
		t.Fatalf("concurrent fork err=%v, want ErrFSLayerStateConflict", gotForkErr)
	}
	parent, err := s.GetFSLayer(ctx, "p-del-race")
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if parent.State != FSLayerStateAbandoned {
		t.Fatalf("parent state=%s, want abandoned", parent.State)
	}
	if _, err := s.GetFSLayer(ctx, "c-del-race"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("child get err=%v, want ErrNotFound (fork must not pin an abandoned parent)", err)
	}
}
