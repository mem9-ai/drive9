package extent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/s3client"
)

// recordingPutS3 records what s3Wrap.Put hands to the object store so a test
// can assert the payload was streamed, not buffered, and that its length is
// exact.
type recordingPutS3 struct {
	*s3client.LocalS3Client
	calls    int
	bodyType string
	size     int64
	body     []byte
	seekable bool
}

func (r *recordingPutS3) PutObject(ctx context.Context, key string, body io.Reader, size int64, encOpts s3client.EncryptionOpts) error {
	r.calls++
	r.bodyType = fmt.Sprintf("%T", body)
	r.size = size
	_, r.seekable = body.(io.Seeker)
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	r.body = data
	return r.LocalS3Client.PutObject(ctx, key, bytes.NewReader(data), size, encOpts)
}

func newPutRecorder(t *testing.T) *recordingPutS3 {
	t.Helper()
	local, err := s3client.NewLocal(t.TempDir(), "http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	return &recordingPutS3{LocalS3Client: local}
}

func TestS3WrapPutStreamsSeekableBodyWithoutBuffering(t *testing.T) {
	rec := newPutRecorder(t)
	st, err := WrapS3(rec, "t/tenant-a/")
	if err != nil {
		t.Fatal(err)
	}
	// JuiceFS's chunk store uploads a *bytes.Reader (pkg/chunk/cached_store.go
	// put); it must reach the object store unreplaced and correctly rewound.
	payload := bytes.Repeat([]byte("abcdefgh"), 4096)
	if err := st.Put(context.Background(), "chunks/c1", bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if rec.calls != 1 {
		t.Fatalf("PutObject calls = %d, want 1", rec.calls)
	}
	if rec.bodyType != "*bytes.Reader" {
		t.Fatalf("body type = %s, want the caller's *bytes.Reader (payload must not be buffered)", rec.bodyType)
	}
	if rec.size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", rec.size, len(payload))
	}
	if !bytes.Equal(rec.body, payload) {
		t.Fatalf("payload corrupted by measuring its length: %d bytes read, want %d", len(rec.body), len(payload))
	}
	if _, err := os.Stat(filepath.Join(rec.ObjectsDir(), "t/tenant-a/chunks/c1")); err != nil {
		t.Fatalf("object not stored under the tenant prefix: %v", err)
	}
}

func TestS3WrapPutMeasuresFromCurrentOffset(t *testing.T) {
	rec := newPutRecorder(t)
	st, err := WrapS3(rec, "")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("0123456789")
	r := bytes.NewReader(payload)
	if _, err := r.Seek(4, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), "chunks/c2", r); err != nil {
		t.Fatal(err)
	}
	if rec.size != int64(len(payload)-4) {
		t.Fatalf("size = %d, want remaining %d", rec.size, len(payload)-4)
	}
	if !bytes.Equal(rec.body, payload[4:]) {
		t.Fatalf("body = %q, want %q", rec.body, payload[4:])
	}
}

func TestS3WrapPutSpoolsNonSeekableBody(t *testing.T) {
	rec := newPutRecorder(t)
	st, err := WrapS3(rec, "t/tenant-a/")
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("xyz"), 1024)
	if err := st.Put(context.Background(), "chunks/c3", io.NopCloser(bytes.NewReader(payload))); err != nil {
		t.Fatal(err)
	}
	if rec.size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", rec.size, len(payload))
	}
	if !bytes.Equal(rec.body, payload) {
		t.Fatalf("spooled payload mismatch: %d bytes, want %d", len(rec.body), len(payload))
	}
	// The object client re-reads a plain-HTTP body to sign it, so the spooled
	// reader must be rewindable.
	if !rec.seekable {
		t.Fatalf("spooled body type %s is not seekable", rec.bodyType)
	}
}

func TestS3WrapPutRejectsNilBody(t *testing.T) {
	rec := newPutRecorder(t)
	st, err := WrapS3(rec, "t/tenant-a/")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), "chunks/c4", nil); err == nil {
		t.Fatal("nil body must fail instead of uploading an empty object")
	}
	if rec.calls != 0 {
		t.Fatalf("PutObject calls = %d, want 0", rec.calls)
	}
}
