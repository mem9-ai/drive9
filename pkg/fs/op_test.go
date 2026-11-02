package fs

import (
	"context"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestOpKindKernelABI(t *testing.T) {
	cases := []struct {
		kind OpKind
		val  uint32
		name string
	}{
		{OpLookup, 1, "lookup"},
		{OpGetAttr, 3, "getattr"},
		{OpReadlink, 5, "readlink"},
		{OpMknod, 8, "mknod"},
		{OpUnlink, 10, "unlink"},
		{OpRead, 15, "read"},
		{OpStatFs, 17, "statfs"},
		{OpSetXattr, 21, "setxattr"},
		{OpRemoveXattr, 24, "removexattr"},
		{OpGetLk, 31, "getlk"},
		{OpAccess, 34, "access"},
		{OpFallocate, 43, "fallocate"},
		{OpLseek, 46, "lseek"},
		{OpCopyFileRange, 47, "copy_file_range"},
		{OpList, 0x1000, "list"},
		{OpCreate, 0x1001, "create"},
		{OpRename, 0x1002, "rename"},
	}
	for _, c := range cases {
		if uint32(c.kind) != c.val {
			t.Fatalf("%s = %d, want %d", c.name, uint32(c.kind), c.val)
		}
		if c.kind.String() != c.name {
			t.Fatalf("String() = %q, want %q", c.kind.String(), c.name)
		}
	}
	for _, k := range []OpKind{OpList, OpCreate, OpRename} {
		if uint32(k) < 0x1000 {
			t.Fatalf("synthetic kind %s below private range: %d", k, uint32(k))
		}
	}
}

func TestExtendedOpKindDispatch(t *testing.T) {
	var setXattr *Op
	core := HandlerFunc(func(ctx context.Context, op *Op, next Next) (Result, error) {
		switch op.Kind {
		case OpSetXattr:
			setXattr = op
			return Result{}, nil
		case OpGetXattr:
			return Result{Data: []byte("v:" + op.Name)}, nil
		case OpStatFs:
			return Result{Stat: &FsStat{Blocks: 42, Free: 40}}, nil
		case OpGetLk:
			lk := op.Lock
			return Result{Lock: &lk}, nil
		case OpCopyFileRange:
			return Result{Written: uint32(len(op.Data))}, nil
		}
		return Result{}, nil
	})
	ch, err := NewChain(core)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Use(HandlerFunc(func(ctx context.Context, op *Op, next Next) (Result, error) {
		return next(ctx, op)
	})); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &Op{Kind: OpSetXattr, Path: "/a", Name: "user.k", Data: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if setXattr == nil || setXattr.Name != "user.k" || string(setXattr.Data) != "v" {
		t.Fatalf("setxattr op = %+v", setXattr)
	}
	res, err := ch.Dispatch(ctx, &Op{Kind: OpGetXattr, Path: "/a", Name: "user.k"})
	if err != nil || string(res.Data) != "v:user.k" {
		t.Fatalf("getxattr = %q err=%v", res.Data, err)
	}
	res, err = ch.Dispatch(ctx, &Op{Kind: OpStatFs})
	if err != nil || res.Stat == nil || res.Stat.Blocks != 42 {
		t.Fatalf("statfs = %+v err=%v", res, err)
	}
	res, err = ch.Dispatch(ctx, &Op{Kind: OpGetLk, Path: "/a", Lock: FileLock{Start: 0, End: 9, Type: syscall.F_WRLCK}})
	if err != nil || res.Lock == nil || res.Lock.End != 9 {
		t.Fatalf("getlk = %+v err=%v", res, err)
	}
	res, err = ch.Dispatch(ctx, &Op{Kind: OpCopyFileRange, Path: "/a", Path2: "/b", Offset: 10, Data: make([]byte, 32)})
	if err != nil || res.Written != 32 {
		t.Fatalf("copy_file_range = %+v err=%v", res, err)
	}
}

func TestErrnoContract(t *testing.T) {
	if !IsNotFound(ErrNotFound) {
		t.Fatal("sentinel not classified")
	}
	if got := StatusOf(ErrNotFound); got != 2 {
		t.Fatalf("StatusOf(ErrNotFound) = %v", got)
	}
	if got := StatusOf(ErrUnsupported); got != gofuse.Status(syscall.ENOSYS) {
		t.Fatalf("StatusOf(ErrUnsupported) = %v", got)
	}
}
