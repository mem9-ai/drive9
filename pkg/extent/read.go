package extent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"syscall"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// OpenRead returns a reader for an extent inode. Bytes come from object
// storage (S3/MinIO/file mock), not from drive9-server HTTP.
//
// ctx is the caller's request context: cancellation and deadlines are honoured
// between reads, and the read stops with ctx.Err() instead of continuing to
// stream after the request is gone. JuiceFS's vfs.FileReader has no context
// parameter, so a read already in flight in the chunk store still finishes.
func (rt *Runtime) OpenRead(ctx context.Context, ino uint64, length, offset, limit uint64) (io.ReadCloser, error) {
	if rt == nil || rt.Reader == nil {
		return nil, fmt.Errorf("nil extent runtime")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	jctx := jfsmeta.NewContext(1, 0, []uint32{0})
	var attr jfsmeta.Attr
	if st := rt.Meta.GetAttr(jctx, jfsmeta.Ino(ino), &attr); st == 0 {
		if attr.Typ == jfsmeta.TypeSymlink {
			return rt.openSymlinkRead(jctx, ino, offset, limit)
		}
		if length == 0 {
			length = attr.Length
		}
	}
	if offset > length {
		offset = length
	}
	end := length
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return &extentReadCloser{
		ctx:    jctx,
		reqCtx: ctx,
		r:      rt.Reader.Open(jfsmeta.Ino(ino), length),
		off:    offset,
		end:    end,
	}, nil
}

func (rt *Runtime) openSymlinkRead(ctx jfsmeta.Context, ino uint64, offset, limit uint64) (io.ReadCloser, error) {
	target := []byte{}
	if rt.Transport != nil {
		var resp struct {
			Errno  int    `json:"errno"`
			Target []byte `json:"target"`
		}
		if st := rt.Transport.Call(ctx, "readlink", map[string]any{"inode": ino}, &resp); st == 0 && resp.Errno == 0 {
			target = resp.Target
		}
	}
	if len(target) == 0 {
		var raw []byte
		if st := rt.Meta.ReadLink(ctx, jfsmeta.Ino(ino), &raw); st == 0 {
			target = raw
		} else if st != syscall.ENOSYS && st != 0 {
			return nil, fmt.Errorf("extent readlink: %v", st)
		}
	}
	if offset > uint64(len(target)) {
		offset = uint64(len(target))
	}
	target = target[offset:]
	if limit > 0 && uint64(len(target)) > limit {
		target = target[:limit]
	}
	return io.NopCloser(bytes.NewReader(target)), nil
}

type extentReadCloser struct {
	ctx    jfsmeta.Context
	reqCtx context.Context
	r      vfs.FileReader
	off    uint64
	end    uint64
}

func (e *extentReadCloser) Read(p []byte) (int, error) {
	if e == nil || e.r == nil {
		return 0, io.EOF
	}
	if e.off >= e.end || len(p) == 0 {
		return 0, io.EOF
	}
	if e.reqCtx != nil {
		if err := e.reqCtx.Err(); err != nil {
			return 0, err
		}
	}
	want := uint64(len(p))
	if e.off+want > e.end {
		want = e.end - e.off
	}
	n, st := e.r.Read(e.ctx, e.off, p[:want])
	if st != 0 {
		return n, fmt.Errorf("extent read: %v", st)
	}
	if n == 0 {
		return 0, io.EOF
	}
	e.off += uint64(n)
	return n, nil
}

func (e *extentReadCloser) Close() error {
	if e != nil && e.r != nil {
		e.r.Close(e.ctx)
		e.r = nil
	}
	return nil
}
