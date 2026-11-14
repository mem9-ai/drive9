package writeback

import (
	"context"
	"errors"
	"io"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// truncateCleanMainToZero commits a pathname truncate independently of clean
// single-layout handles awaiting RELEASE. A dirty handle or pending upload still owns its
// image and uses the existing local truncate path instead.
func (w *Writeback) truncateCleanMainToZero(ctx context.Context, op *fs.Op) (fs.Result, error, bool) {
	if w.opts.Policy != PolicyCloseSync || op.Size != 0 || op.Handle != nil || w.opts.Client.ViewBinding().LayerID != "" {
		return fs.Result{}, nil, false
	}
	handles := w.inodeHandles(op.Ino, op.Path)
	if len(handles) == 0 {
		return fs.Result{}, nil, false
	}
	locked, err := w.lockSiblingsOrdered(handles, nil)
	if err != nil {
		return fs.Result{}, err, true
	}
	defer func() {
		for i := len(locked) - 1; i >= 0; i-- {
			locked[i].mu.Unlock()
		}
	}()
	aliases := make(map[string]bool)
	for _, path := range w.inodeAliasPaths(op.Ino, op.Path) {
		aliases[path] = true
	}
	for _, h := range locked {
		if !aliases[h.path] || h.unlinked {
			return fs.Result{}, syscall.EAGAIN, true
		}
		if h.dirty || h.trunc && h.buf == nil || h.isNew && !h.landed || h.stagedToCache || w.hasPendingLocalState(h.path) {
			return fs.Result{}, nil, false
		}
	}
	// Use the client directly: terminal mutation events can call back into
	// handle decorators. Emit the ordinary invalidation after releasing locks.
	path := w.remotePath(op.Path)
	st, err := w.opts.Client.StatCtx(ctx, path)
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errors.Join(syscall.EIO, err)
		}
		return fs.Result{}, mapErr(err), true
	}
	// Ordinary PUT cannot replace append-log or extent content. Unknown
	// layouts also retain the existing path; only explicit single is eligible.
	if st.ContentLayout != client.ContentLayoutSingle {
		return fs.Result{}, nil, false
	}
	if st.Revision <= 0 {
		return fs.Result{}, syscall.EIO, true
	}
	rev := st.Revision
	if st.Size != 0 {
		rev, err = w.opts.Client.WriteCtxConditionalWithRevision(ctx, path, nil, st.Revision)
		if err != nil {
			// No handle was dirtied. A late RELEASE cannot retry this
			// pathname mutation and overwrite a later writer's contents.
			if errors.Is(err, io.EOF) {
				err = errors.Join(syscall.EIO, err) // EOF is only success for reads.
			}
			return fs.Result{}, mapErr(err), true
		}
		if rev <= 0 {
			return fs.Result{}, syscall.EIO, true
		}
	}
	for _, h := range locked {
		h.buf, h.stream, h.pendingImage = nil, nil, nil
		h.baseLoaded, h.trunc, h.visTrunc = false, false, false
		h.baseSize, h.baseRev = 0, rev
		w.landedAt(h.path, rev, 0, 0, []byte{})
	}
	return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: st.Mode, Rev: rev, ResourceID: st.ResourceID}}, nil, true
}
