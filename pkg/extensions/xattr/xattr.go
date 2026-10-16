// Package xattr provides session-scoped in-memory extended attributes for
// a mount: xattr reads and writes are answered locally and never consult
// the chain below (attributes are not persisted to the backend), renames
// and unlinks move or drop the stored entries with the surviving paths,
// and every other op passes through.
package xattr

import (
	"context"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Options configures the extension.
type Options struct {
	// Events, when set, drops a path's stored entries on delete events
	// (remote-origin deletions included).
	Events *fs.EventBus
}

// Xattr answers extended-attribute reads and writes from the session
// store. Renames and unlinks keep the store's entries attached to the
// surviving paths.
type Xattr struct {
	store *XAttrStore
}

// New creates the extension, subscribing to delete events when Events is
// set.
func New(opts Options) (*Xattr, error) {
	x := &Xattr{store: NewXAttrStore()}
	if opts.Events != nil {
		// Rename bookkeeping is event-driven: outer extensions (localize,
		// layer, Git, specials) complete renames without dispatching
		// through this plane, so the store moves on the same successful-
		// rename signal the rest of the mount consumes.
		opts.Events.On(fs.EventWrite, func(e fs.Event) {
			if e.RenamedTo != "" {
				x.store.Rename(e.Path, e.RenamedTo)
			}
		})
		opts.Events.On(fs.EventDelete, func(e fs.Event) { x.store.RemoveAll(e.Path) })
	}
	return x, nil
}

func (x *Xattr) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpGetXattr:
		// Session-local only: an unsupported namespace is EOPNOTSUPP and a
		// miss is ENOATTR — the backend is never consulted.
		if !NamespaceSupported(op.Name) {
			return fs.Result{}, fs.StatusErrorOf(gofuse.Status(syscall.EOPNOTSUPP))
		}
		if v, hit := x.store.Get(op.Path, op.Name); hit {
			return fs.Result{Data: v}, nil
		}
		return fs.Result{}, fs.StatusErrorOf(gofuse.ENOATTR)
	case fs.OpSetXattr:
		if !NamespaceSupported(op.Name) {
			return fs.Result{}, syscall.EOPNOTSUPP
		}
		if errno := x.store.SetWithFlags(op.Path, op.Name, op.Data, op.Flags); errno != 0 {
			return fs.Result{}, errno
		}
		return fs.Result{}, nil
	case fs.OpListXattr:
		names := x.store.List(op.Path)
		if len(names) == 0 {
			// An empty xattr set lists as success with zero bytes, not
			// ENOSYS.
			return fs.Result{Data: nil}, nil
		}
		buf := make([]byte, 0, len(names)*16)
		for _, n := range names {
			buf = append(buf, n...)
			buf = append(buf, 0)
		}
		return fs.Result{Data: buf}, nil
	case fs.OpRemoveXattr:
		if !NamespaceSupported(op.Name) {
			return fs.Result{}, fs.StatusErrorOf(gofuse.Status(syscall.EOPNOTSUPP))
		}
		if x.store.Remove(op.Path, op.Name) {
			return fs.Result{}, nil
		}
		return fs.Result{}, fs.StatusErrorOf(gofuse.ENOATTR)
	case fs.OpRename:
		// The migration is event-driven ONLY (one ordered path): this
		// plane's own dispatch AND renames claimed by outer extensions
		// (localize/layer/git/specials complete without next) move the
		// store on the facade's successful-rename emission. Moving here
		// as well re-renamed an already-moved source and deleted the
		// destination's fresh attributes.
	case fs.OpUnlink, fs.OpRmdir:
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		x.store.RemoveAll(op.Path)
		return res, nil
	}
	return next(ctx, op)
}

// ReexecRefusal reports stored xattrs (lost on re-exec).
func (x *Xattr) ReexecRefusal() (string, int) {
	if n := x.store.Count(); n > 0 {
		return "in-memory xattrs exist", n
	}
	return "", 0
}
