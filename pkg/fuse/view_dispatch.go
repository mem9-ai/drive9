package fuse

import (
	"context"
	"errors"
	"math"
	"reflect"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

var (
	errViewGenerationConflict = errors.New("mount view generation conflict")
	errViewUnmounted          = errors.New("mount connection has ended")
)

// viewRequestTarget is immutable once installed. The initial delegate remains
// immutable too, so non-data callbacks can safely retain their local snapshot.
type viewRequestTarget struct {
	fs         gofuse.RawFileSystem
	generation uint64
}

func (v *viewRequestFS) currentTarget() viewRequestTarget {
	if target := v.selected.Load(); target != nil {
		return *target
	}
	return viewRequestTarget{fs: v.inner, generation: 1}
}

// installTarget changes only callback dispatch, leaving ordinary admission
// CLOSED. The owner must already have durably decided this generation, stopped
// the old background publishers and either preserved or rejected every old
// inode/handle/mapping reference. This is not an inode or handle router.
// The caller must finish kernel fencing/cache invalidation and the durable
// completion record before resuming. Failure after this method cannot be
// described as keeping the old binding; it requires recovery of that decision.
func (v *viewRequestFS) installTarget(ctx context.Context, pause *viewPause, expected, generation uint64, target gofuse.RawFileSystem) error {
	if pause == nil || pause.gate != &v.admission {
		return errViewPauseLost
	}
	if expected == 0 || expected == math.MaxUint64 || generation != expected+1 || target == nil {
		return errViewGenerationConflict
	}
	value := reflect.ValueOf(target)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return errViewGenerationConflict
		}
	}
	return pause.atIdle(ctx, func() error {
		if v.unmounted.Load() {
			return errViewUnmounted
		}
		if current := v.currentTarget(); current.fs == nil || current.generation != expected {
			return errViewGenerationConflict
		}
		v.selected.Store(&viewRequestTarget{fs: target, generation: generation})
		return nil
	})
}
