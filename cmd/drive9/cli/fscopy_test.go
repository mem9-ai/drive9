package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

type memBackend struct {
	scheme Scheme
	id     string
	mu     sync.Mutex
	files  map[string][]byte
}

func newMem(scheme Scheme, id string) *memBackend {
	return &memBackend{scheme: scheme, id: id, files: map[string][]byte{}}
}

func (m *memBackend) Scheme() Scheme   { return m.scheme }
func (m *memBackend) Caps() Capability { return CapStat | CapRead | CapWrite }
func (m *memBackend) Close() error     { return nil }
func (m *memBackend) Identity() string { return m.id }
func (m *memBackend) Stat(context.Context, Location) (*FileInfo, error) {
	return nil, ErrNotSupported
}
func (m *memBackend) List(context.Context, Location, ListOpts) (ListPage, error) {
	return ListPage{}, ErrNotSupported
}
func (m *memBackend) OpenRead(_ context.Context, loc Location) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.files[loc.Path]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (m *memBackend) OpenReadRange(context.Context, Location, int64, int64) (io.ReadCloser, error) {
	return nil, ErrNotSupported
}
func (m *memBackend) OpenWrite(_ context.Context, loc Location, _ WriteOpts) (WriteHandle, error) {
	return &memWrite{m: m, path: loc.Path}, nil
}
func (m *memBackend) Remove(context.Context, Location, bool) error { return ErrNotSupported }
func (m *memBackend) Mkdir(context.Context, Location) error        { return ErrNotSupported }

type memWrite struct {
	m    *memBackend
	path string
	buf  bytes.Buffer
	done bool
}

func (w *memWrite) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *memWrite) Close() error {
	w.m.mu.Lock()
	defer w.m.mu.Unlock()
	w.m.files[w.path] = append([]byte(nil), w.buf.Bytes()...)
	w.done = true
	return nil
}
func (w *memWrite) Abort(context.Context) error {
	w.buf.Reset()
	return nil
}

type sameCopyBackend struct {
	*memBackend
	copies int
}

func (s *sameCopyBackend) Stat(_ context.Context, loc Location) (*FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.files[loc.Path]; !ok {
		return nil, ErrNotFound
	}
	return &FileInfo{Name: loc.Path, Path: loc.Path, Size: int64(len(s.files[loc.Path]))}, nil
}

func (s *sameCopyBackend) Copy(_ context.Context, src, dst Location) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.files[src.Path]
	if !ok {
		return ErrNotFound
	}
	s.files[dst.Path] = append([]byte(nil), b...)
	s.copies++
	return nil
}

func TestCopySameBackendRefusesOverwriteWithoutForce(t *testing.T) {
	b := &sameCopyBackend{memBackend: newMem(SchemeS3, "one")}
	b.files["a.txt"] = []byte("src")
	b.files["b.txt"] = []byte("existing")
	src := Location{Raw: "s3://bucket/a.txt", Bucket: "bucket", Path: "a.txt"}
	dst := Location{Raw: "s3://bucket/b.txt", Bucket: "bucket", Path: "b.txt"}
	err := Copy(context.Background(), b, b, src, dst, &FileInfo{Size: 3}, CopyOpts{})
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("err=%v, want --force", err)
	}
	if b.copies != 0 {
		t.Fatal("SameCopier must not run when overwrite is refused")
	}
	if got := string(b.files["b.txt"]); got != "existing" {
		t.Fatalf("dest mutated: %q", got)
	}
	if err := Copy(context.Background(), b, b, src, dst, &FileInfo{Size: 3}, CopyOpts{Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	if b.copies != 1 {
		t.Fatalf("copies=%d", b.copies)
	}
	if got := string(b.files["b.txt"]); got != "src" {
		t.Fatalf("got %q", got)
	}
}

func TestStreamCopyNonSeeker(t *testing.T) {
	src := newMem(SchemeDrive9, "a")
	dst := newMem(SchemeS3, "b")
	src.files["a.txt"] = []byte("hello-world")
	err := StreamCopy(context.Background(), src, dst,
		Location{Raw: "src", Path: "a.txt"},
		Location{Raw: "dst", Path: "b.txt"},
		&FileInfo{Size: 11},
		CopyOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(dst.files["b.txt"]); got != "hello-world" {
		t.Fatalf("got %q", got)
	}
}

func TestStreamCopyShortWrite(t *testing.T) {
	src := newMem(SchemeDrive9, "a")
	dst := &shortWriteBackend{memBackend: *newMem(SchemeS3, "b")}
	src.files["a.txt"] = []byte("hello-world")
	err := StreamCopy(context.Background(), src, dst,
		Location{Raw: "src", Path: "a.txt"},
		Location{Raw: "dst", Path: "b.txt"},
		&FileInfo{Size: 11},
		CopyOpts{})
	if err == nil || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("err=%v, want short write", err)
	}
}

type shortWriteBackend struct{ memBackend }

func (s *shortWriteBackend) OpenWrite(_ context.Context, loc Location, _ WriteOpts) (WriteHandle, error) {
	return &shortWrite{path: loc.Path}, nil
}

type shortWrite struct {
	path string
	buf  bytes.Buffer
}

func (w *shortWrite) Write(p []byte) (int, error) {
	if len(p) <= 1 {
		return w.buf.Write(p)
	}
	n := len(p) / 2
	_, err := w.buf.Write(p[:n])
	return n, err
}
func (w *shortWrite) Close() error                { return nil }
func (w *shortWrite) Abort(context.Context) error { return nil }

func TestStreamCopyShortRead(t *testing.T) {
	src := newMem(SchemeDrive9, "a")
	dst := newMem(SchemeS3, "b")
	src.files["a.txt"] = []byte("short")
	err := StreamCopy(context.Background(), src, dst,
		Location{Raw: "src", Path: "a.txt"},
		Location{Raw: "dst", Path: "b.txt"},
		&FileInfo{Size: 11},
		CopyOpts{})
	if err == nil || !strings.Contains(err.Error(), "short copy") {
		t.Fatalf("err=%v, want short copy", err)
	}
}

func TestStreamCopyAbortOnReadError(t *testing.T) {
	src := &errReadBackend{}
	dst := newMem(SchemeS3, "b")
	err := StreamCopy(context.Background(), src, dst,
		Location{Raw: "src", Path: "a"},
		Location{Raw: "dst", Path: "b"},
		nil, CopyOpts{})
	if err == nil {
		t.Fatal("expected error")
	}
}

type errReadBackend struct{ memBackend }

func (e *errReadBackend) Scheme() Scheme   { return SchemeDrive9 }
func (e *errReadBackend) Caps() Capability { return CapRead }
func (e *errReadBackend) Close() error     { return nil }
func (e *errReadBackend) Stat(context.Context, Location) (*FileInfo, error) {
	return nil, ErrNotSupported
}
func (e *errReadBackend) List(context.Context, Location, ListOpts) (ListPage, error) {
	return ListPage{}, ErrNotSupported
}
func (e *errReadBackend) OpenRead(context.Context, Location) (io.ReadCloser, error) {
	return io.NopCloser(&errReader{}), nil
}
func (e *errReadBackend) OpenReadRange(context.Context, Location, int64, int64) (io.ReadCloser, error) {
	return nil, ErrNotSupported
}
func (e *errReadBackend) OpenWrite(context.Context, Location, WriteOpts) (WriteHandle, error) {
	return nil, ErrNotSupported
}
func (e *errReadBackend) Remove(context.Context, Location, bool) error { return ErrNotSupported }
func (e *errReadBackend) Mkdir(context.Context, Location) error        { return ErrNotSupported }

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
