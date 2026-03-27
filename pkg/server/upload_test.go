package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
)

func newTestServerWithS3(t *testing.T) (*Server, *s3client.LocalS3Client) {
	t.Helper()
	blobDir, err := os.MkdirTemp("", "dat9-srv-blobs-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(blobDir) })

	s3Dir, err := os.MkdirTemp("", "dat9-srv-s3-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initServerTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testmysql.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	s3c, err := s3client.NewLocal(s3Dir, "http://localhost:9091/s3")
	if err != nil {
		t.Fatal(err)
	}

	b, err := backend.NewWithS3(store, s3c)
	if err != nil {
		t.Fatal(err)
	}
	return New(b), s3c
}

func TestLargeFilePut202(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// PUT with Content-Length >= 1MB should return 202
	body := make([]byte, 1<<20) // exactly 1MB
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/big.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if plan.UploadID == "" {
		t.Error("expected upload_id")
	}
	if len(plan.Parts) == 0 {
		t.Error("expected parts")
	}
}

func TestSmallFilePut200(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// PUT with Content-Length < 1MB should return 200 (proxied)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/small.txt", bytes.NewReader([]byte("hello")))
	req.ContentLength = 5
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestUploadCompleteEndpoint(t *testing.T) {
	s, s3c := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Initiate via PUT 202
	body := make([]byte, 1<<20)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/complete-test.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	// Get the upload to find s3_upload_id
	upload, _ := s.fallback.GetUpload(plan.UploadID)

	// Upload all parts via S3 client directly
	for _, p := range plan.Parts {
		start := int64(p.Number-1) * s3client.PartSize
		end := start + p.Size
		if end > int64(len(body)) {
			end = int64(len(body))
		}
		if _, err := s3c.UploadPart(context.Background(), upload.S3UploadID, p.Number, bytes.NewReader(body[start:end])); err != nil {
			t.Fatalf("upload part %d: %v", p.Number, err)
		}
	}

	// POST /v1/uploads/{id}/complete
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/complete", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete: expected 200, got %d", resp.StatusCode)
	}
}

func TestUploadResumeEndpoint(t *testing.T) {
	s, s3c := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Initiate a 20MB upload (3 parts)
	totalSize := int64(20 << 20)
	body := make([]byte, totalSize)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/resume-test.bin", bytes.NewReader(body))
	req.ContentLength = totalSize
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	upload, _ := s.fallback.GetUpload(plan.UploadID)

	// Upload only part 1
	if _, err := s3c.UploadPart(context.Background(), upload.S3UploadID, 1, bytes.NewReader(make([]byte, s3client.PartSize))); err != nil {
		t.Fatal(err)
	}

	// POST /v1/uploads/{id}/resume
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/resume", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("resume: expected 200, got %d: %s", resp.StatusCode, body)
	}

	var resumed backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&resumed); err != nil {
		t.Fatal(err)
	}
	if len(resumed.Parts) != 2 {
		t.Errorf("expected 2 missing parts, got %d", len(resumed.Parts))
	}
}

func TestLargeUploadOverwritesExistingSmallFile(t *testing.T) {
	s, s3c := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Seed an existing small file at the target path.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/overwrite.bin", bytes.NewReader([]byte("small")))
	req.ContentLength = 5
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("small seed: expected 200, got %d", resp.StatusCode)
	}

	// Initiate a large upload to the same path.
	totalSize := int64(2 << 20)
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/overwrite.bin", bytes.NewReader(make([]byte, totalSize)))
	req.ContentLength = totalSize
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("initiate overwrite: expected 202, got %d", resp.StatusCode)
	}

	upload, err := s.fallback.GetUpload(plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	// Upload all parts through the local S3 stand-in.
	for _, p := range plan.Parts {
		start := int64(p.Number-1) * s3client.PartSize
		end := start + p.Size
		if _, err := s3c.UploadPart(context.Background(), upload.S3UploadID, p.Number,
			bytes.NewReader(make([]byte, end-start))); err != nil {
			t.Fatalf("upload part %d: %v", p.Number, err)
		}
	}

	// Complete should now overwrite the existing small-file node.
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/complete", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete overwrite: expected 200, got %d", resp.StatusCode)
	}

	nf, err := s.fallback.Store().Stat("/overwrite.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nf.File == nil || nf.File.StorageType != datastore.StorageS3 {
		t.Fatalf("expected overwrite.bin to point at S3-backed file, got %+v", nf.File)
	}
	if nf.File.SizeBytes != totalSize {
		t.Fatalf("expected size %d, got %d", totalSize, nf.File.SizeBytes)
	}
}

func TestListUploadsEndpoint(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Create one upload
	body := make([]byte, 1<<20)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/list-test.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()

	// GET /v1/uploads?path=/list-test.bin&status=UPLOADING
	resp, err := http.Get(ts.URL + "/v1/uploads?path=/list-test.bin&status=UPLOADING")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Uploads []struct {
			UploadID   string `json:"upload_id"`
			PartsTotal int    `json:"parts_total"`
			Status     string `json:"status"`
		} `json:"uploads"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Uploads) != 1 {
		t.Errorf("expected 1 upload, got %d", len(result.Uploads))
	}
	if result.Uploads[0].PartsTotal != 1 {
		t.Errorf("expected parts_total=1, got %d", result.Uploads[0].PartsTotal)
	}
}

func TestOneUploadPerPath(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// First upload should succeed with 202
	body := make([]byte, 1<<20)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/dup-test.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first upload: expected 202, got %d", resp.StatusCode)
	}

	// Second upload for same path should fail
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/dup-test.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, _ = http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second upload: expected 409 (conflict), got %d", resp.StatusCode)
	}
}

func TestAbortUploadEndpoint(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// Create upload
	body := make([]byte, 1<<20)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/abort-test.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, _ := http.DefaultClient.Do(req)

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	// DELETE /v1/uploads/{id}
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/v1/uploads/"+plan.UploadID, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("abort: expected 200, got %d", resp.StatusCode)
	}

	// Verify upload is aborted
	upload, _ := s.fallback.GetUpload(plan.UploadID)
	if upload.Status != datastore.UploadAborted {
		t.Errorf("expected ABORTED, got %s", upload.Status)
	}
}
