package s3client

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
)

func newTestClient(t *testing.T) *LocalS3Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "dat9-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	c, err := NewLocal(dir, "http://localhost:9091/s3")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCalcParts(t *testing.T) {
	tests := []struct {
		total    int64
		partSize int64
		wantN    int
		lastSize int64
	}{
		{10, 3, 4, 1},
		{9, 3, 3, 3},
		{1, 8 << 20, 1, 1},
		{16 << 20, 8 << 20, 2, 8 << 20},
		{17 << 20, 8 << 20, 3, 1 << 20},
	}
	for _, tt := range tests {
		parts := CalcParts(tt.total, tt.partSize)
		if len(parts) != tt.wantN {
			t.Errorf("CalcParts(%d, %d): got %d parts, want %d", tt.total, tt.partSize, len(parts), tt.wantN)
			continue
		}
		if parts[len(parts)-1].Size != tt.lastSize {
			t.Errorf("CalcParts(%d, %d): last part size=%d, want %d", tt.total, tt.partSize, parts[len(parts)-1].Size, tt.lastSize)
		}
		// Verify part numbers are 1-based
		for i, p := range parts {
			if p.Number != i+1 {
				t.Errorf("part %d has Number=%d", i, p.Number)
			}
		}
	}
}

func TestPutAndGetObject(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	data := []byte("hello world")
	if err := c.PutObject(ctx, "blobs/test1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}

	rc, err := c.GetObject(ctx, "blobs/test1")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "hello world" {
		t.Errorf("got %q", got)
	}
}

func TestDeleteObject(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	c.PutObject(ctx, "blobs/del", bytes.NewReader([]byte("x")), 1)
	if err := c.DeleteObject(ctx, "blobs/del"); err != nil {
		t.Fatal(err)
	}
	_, err := c.GetObject(ctx, "blobs/del")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestMultipartUploadComplete(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Initiate
	upload, err := c.CreateMultipartUpload(ctx, "blobs/big1")
	if err != nil {
		t.Fatal(err)
	}
	if upload.UploadID == "" || upload.Key != "blobs/big1" {
		t.Fatalf("unexpected upload: %+v", upload)
	}

	// Upload 3 parts
	partData := []string{"AAAA", "BBBB", "CC"}
	var parts []Part
	for i, d := range partData {
		etag, err := c.UploadPart(ctx, upload.UploadID, i+1, bytes.NewReader([]byte(d)))
		if err != nil {
			t.Fatalf("upload part %d: %v", i+1, err)
		}
		parts = append(parts, Part{Number: i + 1, Size: int64(len(d)), ETag: etag})
	}

	// List parts
	listed, err := c.ListParts(ctx, "blobs/big1", upload.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(listed))
	}

	// Complete
	if err := c.CompleteMultipartUpload(ctx, "blobs/big1", upload.UploadID, parts); err != nil {
		t.Fatal(err)
	}

	// Read assembled object
	rc, err := c.GetObject(ctx, "blobs/big1")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if string(got) != "AAAABBBBCC" {
		t.Errorf("expected AAAABBBBCC, got %q", got)
	}
}

func TestMultipartUploadAbort(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	upload, err := c.CreateMultipartUpload(ctx, "blobs/aborted")
	if err != nil {
		t.Fatal(err)
	}
	c.UploadPart(ctx, upload.UploadID, 1, bytes.NewReader([]byte("data")))

	if err := c.AbortMultipartUpload(ctx, "blobs/aborted", upload.UploadID); err != nil {
		t.Fatal(err)
	}

	// ListParts should fail after abort
	_, err = c.ListParts(ctx, "blobs/aborted", upload.UploadID)
	if err == nil {
		t.Error("expected error after abort")
	}
}

func TestPresignURLsGenerated(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	upload, _ := c.CreateMultipartUpload(ctx, "blobs/presign-test")

	url, err := c.PresignUploadPart(ctx, "blobs/presign-test", upload.UploadID, 1, UploadTTL)
	if err != nil {
		t.Fatal(err)
	}
	if url.URL == "" || url.Number != 1 {
		t.Errorf("unexpected presigned URL: %+v", url)
	}

	getURL, err := c.PresignGetObject(ctx, "blobs/presign-test", DownloadTTL)
	if err != nil {
		t.Fatal(err)
	}
	if getURL == "" {
		t.Error("expected non-empty presigned GET URL")
	}
}

func TestPartialUploadAndListParts(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	upload, _ := c.CreateMultipartUpload(ctx, "blobs/partial")

	// Upload only parts 1 and 3 (skip 2 — simulates interrupted upload)
	c.UploadPart(ctx, upload.UploadID, 1, bytes.NewReader([]byte("PART1")))
	c.UploadPart(ctx, upload.UploadID, 3, bytes.NewReader([]byte("PART3")))

	listed, err := c.ListParts(ctx, "blobs/partial", upload.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 uploaded parts, got %d", len(listed))
	}
	if listed[0].Number != 1 || listed[1].Number != 3 {
		t.Errorf("unexpected part numbers: %v", listed)
	}
}
