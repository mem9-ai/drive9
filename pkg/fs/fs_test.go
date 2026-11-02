package fs

import (
	"context"
	"testing"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

func TestWrapFSFromNext(t *testing.T) {
	attr := Attr{Size: 11}
	var gotInner Filesystem
	inner := HandlerFunc(func(ctx context.Context, op *Op, next Next) (Result, error) {
		return Result{Attr: &attr}, nil
	})
	outer := HandlerFunc(func(ctx context.Context, op *Op, next Next) (Result, error) {
		gotInner = HandlerFilesystem(next)
		return Result{}, nil
	})
	ch, err := NewChain(outer, inner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(context.Background(), &Op{Kind: OpGetAttr, Path: "/a"}); err != nil {
		t.Fatal(err)
	}
	if gotInner == nil {
		t.Fatal("WrapFS returned nil")
	}
	st, err := gotInner.GetAttr(context.Background(), "/b")
	if err != nil {
		t.Fatalf("wrapped next unusable: %v", err)
	}
	if st.Size != 11 {
		t.Fatalf("wrapped next routed wrong: %+v", st)
	}
}

func TestPathHelpers(t *testing.T) {
	if d := pathutil.DirOf("/a/b"); d != "/a" {
		t.Fatalf("DirOf = %q", d)
	}
	if j := pathutil.JoinPath("/", "x"); j != "/x" {
		t.Fatalf("JoinPath root = %q", j)
	}
	if b := pathutil.BaseName("/a/b"); b != "b" {
		t.Fatalf("BaseName = %q", b)
	}
	if p, b := pathutil.SplitParent("/a/b"); p != "/a" || b != "b" {
		t.Fatalf("SplitParent = %q %q", p, b)
	}
}
