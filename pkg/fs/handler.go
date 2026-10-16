package fs

import (
	"context"
)

// Next continues dispatch into the remainder of the chain.
type Next func(ctx context.Context, op *Op) (Result, error)

// Handler is one link of the namespace operation chain. A handler may
// claim an op (return a Result without calling next), rewrite it (mutate
// op, typically op.Path, then call next), or pass it through unchanged.
// Optional capabilities (Lifecycle, ForgetKeeper, ReexecGater) stay out of
// this interface so any struct can be a handler; assembly and the facade
// discover them by type assertion.
type Handler interface {
	Do(ctx context.Context, op *Op, next Next) (Result, error)
}

// HandlerFunc adapts a plain function into a Handler.
type HandlerFunc func(ctx context.Context, op *Op, next Next) (Result, error)

// Do implements Handler.
func (f HandlerFunc) Do(ctx context.Context, op *Op, next Next) (Result, error) {
	return f(ctx, op, next)
}

// Handle is the data-plane composition unit. Open/create returns the head
// of a decorator chain; data operations dispatch outermost-in. Read takes a
// caller-provided destination buffer so hot read paths stay zero-copy:
// decorators must pass dest through unchanged and only terminal handles
// fill it — any intermediate copy is a bug.
type Handle interface {
	// Read fills dest with content at off and returns the byte count.
	Read(ctx context.Context, off int64, dest []byte) (int, error)
	// Write stores data at off.
	Write(ctx context.Context, off int64, data []byte) error
	// Sync durably commits the handle's current content (fsync).
	Sync(ctx context.Context) error
	// Flush flushes one writer's pending state (dup'd descriptors each
	// flush once; close-time commit point).
	Flush(ctx context.Context) error
	// Release drops the handle's resources after the last reference.
	Release(ctx context.Context) error
}

// Optional handle capabilities. They stay out of the core Handle interface
// so every decorator remains implementable; type-assert at assembly or use
// sites.
type HandleFallocate interface {
	Fallocate(ctx context.Context, off, size int64, mode uint32) error
}

// HandleLockOps is the optional lock capability for data planes that own a
// foreign POSIX lock namespace instead of the mount-local lock table: when
// the open handle implements it, the facade defers lock ops to it.
type HandleLockOps interface {
	GetLk(ctx context.Context, lk FileLock) (FileLock, error)
	SetLk(ctx context.Context, lk FileLock, blocking bool) error
}

// HandleLseek is the optional seek capability for data planes that support
// sparse or hole layouts.
type HandleLseek interface {
	Lseek(ctx context.Context, off int64, whence uint32) (int64, error)
}

// ForgetKeeper is implemented by handlers whose pending state (staged
// buffers, queued commits) must keep a forgotten inode's path mapping
// alive.
type ForgetKeeper interface {
	KeepForgotten(path string) bool
}

// ReexecGater is implemented by handlers that hold in-memory state a
// process re-exec would lose (session-scoped xattrs, unwritten buffers).
// The mount's re-exec preflight asks every handler; an empty reason means
// nothing to refuse.
type ReexecGater interface {
	ReexecRefusal() (reason string, count int)
}
