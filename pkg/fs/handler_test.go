package fs

import (
	"context"
	"testing"
)

type memHandle struct {
	data   []byte
	closed bool
}

func (m *memHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, nil
	}
	return copy(dest, m.data[off:]), nil
}

func (m *memHandle) Write(ctx context.Context, off int64, data []byte) error {
	end := off + int64(len(data))
	if end > int64(len(m.data)) {
		grown := make([]byte, end)
		copy(grown, m.data)
		m.data = grown
	}
	copy(m.data[off:], data)
	return nil
}

func (m *memHandle) Sync(ctx context.Context) error    { return nil }
func (m *memHandle) Flush(ctx context.Context) error   { return nil }
func (m *memHandle) Release(ctx context.Context) error { m.closed = true; return nil }

type tapHandle struct {
	name    string
	inner   Handle
	calls   *[]string
	gotDest []byte
}

func (h *tapHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	*h.calls = append(*h.calls, h.name+".read")
	h.gotDest = dest
	return h.inner.Read(ctx, off, dest)
}

func (h *tapHandle) Write(ctx context.Context, off int64, data []byte) error {
	*h.calls = append(*h.calls, h.name+".write")
	return h.inner.Write(ctx, off, data)
}

func (h *tapHandle) Sync(ctx context.Context) error {
	*h.calls = append(*h.calls, h.name+".sync")
	return h.inner.Sync(ctx)
}

func (h *tapHandle) Flush(ctx context.Context) error {
	*h.calls = append(*h.calls, h.name+".flush")
	return h.inner.Flush(ctx)
}

func (h *tapHandle) Release(ctx context.Context) error {
	*h.calls = append(*h.calls, h.name+".release")
	return h.inner.Release(ctx)
}

func TestHandleDecoratorChain(t *testing.T) {
	var calls []string
	term := &memHandle{data: []byte("hello")}
	inner := &tapHandle{name: "inner", inner: term, calls: &calls}
	outer := &tapHandle{name: "outer", inner: inner, calls: &calls}

	dest := make([]byte, 4)
	n, err := outer.Read(context.Background(), 1, dest)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 || string(dest) != "ello" {
		t.Fatalf("read = %d %q, want 4 \"ello\"", n, dest)
	}
	// zero-copy contract: every decorator passes dest through unchanged.
	if &outer.gotDest[0] != &dest[0] || &inner.gotDest[0] != &dest[0] {
		t.Fatal("decorator did not pass dest buffer through")
	}

	if err := outer.Write(context.Background(), 5, []byte(" world")); err != nil {
		t.Fatal(err)
	}
	if string(term.data) != "hello world" {
		t.Fatalf("terminal data = %q", term.data)
	}
	if err := outer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := outer.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !term.closed {
		t.Fatal("release did not reach the terminal handle")
	}
	want := []string{
		"outer.read", "inner.read",
		"outer.write", "inner.write",
		"outer.flush", "inner.flush",
		"outer.release", "inner.release",
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	}
}
