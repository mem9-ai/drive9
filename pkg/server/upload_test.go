package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/internal/testmysql"
	"github.com/mem9-ai/dat9/pkg/backend"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

func newTestServerWithS3(t *testing.T) (*Server, *s3client.LocalS3Client) {
	t.Helper()
	return newTestServerWithS3Config(t, backend.Options{}, SemanticWorkerOptions{})
}

func newTestServerWithS3Config(t *testing.T, backendOpts backend.Options, workerOpts SemanticWorkerOptions) (*Server, *s3client.LocalS3Client) {
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

	b, err := backend.NewWithS3ModeAndOptions(store, s3c, true, backendOpts)
	if err != nil {
		t.Fatal(err)
	}
	return NewWithConfig(Config{Backend: b, SemanticWorkers: workerOpts}), s3c
}

func partChecksumHeader(data []byte) string {
	parts := s3client.CalcParts(int64(len(data)), s3client.PartSize)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		start := int64(p.Number-1) * s3client.PartSize
		end := start + p.Size
		h := sha256.Sum256(data[start:end])
		out = append(out, base64.StdEncoding.EncodeToString(h[:]))
	}
	return strings.Join(out, ",")
}

func TestLargeFilePut202(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	// PUT with Content-Length >= 1MB should return 202
	body := make([]byte, 1<<20) // exactly 1MB
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/big.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
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

func TestUploadInitiateByBody202(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	body := make([]byte, 1<<20) // exactly 1MB
	reqBody := map[string]any{
		"path":           "/big-body.bin",
		"total_size":     len(body),
		"part_checksums": strings.Split(partChecksumHeader(body), ","),
	}
	p, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/initiate", bytes.NewReader(p))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, b)
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

func TestV1UploadInitiateReturnsAllPresignedParts(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	totalSize := 20 << 20
	body := make([]byte, totalSize)
	reqBody := map[string]any{
		"path":           "/all-parts.bin",
		"total_size":     totalSize,
		"part_checksums": strings.Split(partChecksumHeader(body), ","),
	}
	p, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/initiate", bytes.NewReader(p))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, b)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if plan.PartSize != s3client.PartSize {
		t.Fatalf("part size = %d, want %d", plan.PartSize, s3client.PartSize)
	}
	if len(plan.Parts) != 3 {
		t.Fatalf("expected 3 presigned parts, got %d", len(plan.Parts))
	}
	wantSizes := []int64{s3client.PartSize, s3client.PartSize, 4 << 20}
	for i, part := range plan.Parts {
		if part.Number != i+1 {
			t.Fatalf("part %d number = %d, want %d", i, part.Number, i+1)
		}
		if part.URL == "" {
			t.Fatalf("part %d missing presigned URL", part.Number)
		}
		if part.Size != wantSizes[i] {
			t.Fatalf("part %d size = %d, want %d", part.Number, part.Size, wantSizes[i])
		}
	}
}

func TestV1LargeFilePutRequiresChecksumHeader(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	body := make([]byte, 1<<20)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/missing-checksums.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "missing X-Dat9-Part-Checksums header") {
		t.Fatalf("expected missing checksum header error, got %s", b)
	}
}

func TestV1UploadInitiateByBodyRequiresPartChecksums(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	reqBody := map[string]any{
		"path":       "/missing-body-checksums.bin",
		"total_size": 1 << 20,
	}
	p, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/initiate", bytes.NewReader(p))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "missing part_checksums") {
		t.Fatalf("expected missing part_checksums error, got %s", b)
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

func TestAutoImagePutWritesContentTextEndToEnd(t *testing.T) {
	s, _ := newTestServerWithS3Config(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: staticServerImageExtractor{text: "caption from http put"},
		},
	}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	})
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/e2e-put.png", bytes.NewReader([]byte("fake-png")))
	req.ContentLength = int64(len("fake-png"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	file := mustServerFile(t, s.fallback, "/e2e-put.png")
	waitForContentTextOnServer(t, s.fallback, "/e2e-put.png", "caption from http put", 3*time.Second)
	waitForTaskStatus(t, s.fallback, file.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)

	tasks := loadSemanticTaskRowsForResource(t, s.fallback, file.FileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != string(semantic.TaskTypeImgExtractText) || tasks[0].Status != string(semantic.TaskSucceeded) {
		t.Fatalf("unexpected semantic task rows: %+v", tasks)
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
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
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
	upload, _ := s.fallback.GetUpload(context.Background(), plan.UploadID)

	// Upload all parts via S3 client directly
	for _, p := range plan.Parts {
		start := int64(p.Number-1) * plan.PartSize
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
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	upload, _ := s.fallback.GetUpload(context.Background(), plan.UploadID)

	// Upload only part 1
	if _, err := s3c.UploadPart(context.Background(), upload.S3UploadID, 1, bytes.NewReader(make([]byte, upload.PartSize))); err != nil {
		t.Fatal(err)
	}

	// POST /v1/uploads/{id}/resume
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/resume", nil)
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
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

func TestUploadResumeEndpointByBody(t *testing.T) {
	s, s3c := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	totalSize := int64(20 << 20)
	body := make([]byte, totalSize)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/resume-body-test.bin", bytes.NewReader(body))
	req.ContentLength = totalSize
	checksumsHeader := partChecksumHeader(body)
	req.Header.Set("X-Dat9-Part-Checksums", checksumsHeader)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	upload, _ := s.fallback.GetUpload(context.Background(), plan.UploadID)
	if _, err := s3c.UploadPart(context.Background(), upload.S3UploadID, 1, bytes.NewReader(make([]byte, upload.PartSize))); err != nil {
		t.Fatal(err)
	}

	resumePayload, _ := json.Marshal(map[string]any{"part_checksums": strings.Split(checksumsHeader, ",")})
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/resume", bytes.NewReader(resumePayload))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("resume by body: expected 200, got %d: %s", resp.StatusCode, b)
	}

	var resumed backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&resumed); err != nil {
		t.Fatal(err)
	}
	if len(resumed.Parts) != 2 {
		t.Errorf("expected 2 missing parts, got %d", len(resumed.Parts))
	}
}

func TestV1UploadResumeRequiresChecksums(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	totalSize := int64(20 << 20)
	body := make([]byte, totalSize)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/resume-missing-checksums.bin", bytes.NewReader(body))
	req.ContentLength = totalSize
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/resume", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "missing X-Dat9-Part-Checksums header") {
		t.Fatalf("expected missing checksum header error, got %s", b)
	}
}

func TestV1UploadResumeByBodyRequiresPartChecksums(t *testing.T) {
	s, _ := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	totalSize := int64(20 << 20)
	body := make([]byte, totalSize)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/resume-missing-body-checksums.bin", bytes.NewReader(body))
	req.ContentLength = totalSize
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var plan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/resume", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "missing part_checksums") {
		t.Fatalf("expected missing part_checksums error, got %s", b)
	}
}

func TestV1PatchUsesFixedPartSizeBoundary(t *testing.T) {
	s, s3c := newTestServerWithS3(t)
	ts := httptest.NewServer(s)
	defer ts.Close()

	totalSize := int64(20 << 20)
	body := make([]byte, totalSize)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/patch-fixed-boundary.bin", bytes.NewReader(body))
	req.ContentLength = totalSize
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var uploadPlan backend.UploadPlan
	if err := json.NewDecoder(resp.Body).Decode(&uploadPlan); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	upload, err := s.fallback.GetUpload(context.Background(), uploadPlan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range uploadPlan.Parts {
		start := int64(part.Number-1) * s3client.PartSize
		end := start + part.Size
		if _, err := s3c.UploadPart(context.Background(), upload.S3UploadID, part.Number, bytes.NewReader(body[start:end])); err != nil {
			t.Fatalf("upload part %d: %v", part.Number, err)
		}
	}

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+uploadPlan.UploadID+"/complete", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete: expected 200, got %d", resp.StatusCode)
	}

	patchReq := map[string]any{
		"new_size":    totalSize,
		"dirty_parts": []int{2},
	}
	p, _ := json.Marshal(patchReq)
	req, _ = http.NewRequest(http.MethodPatch, ts.URL+"/v1/fs/patch-fixed-boundary.bin", bytes.NewReader(p))
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("patch: expected 202, got %d: %s", resp.StatusCode, b)
	}

	var plan backend.PatchPlan
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		t.Fatal(err)
	}
	if plan.PartSize != s3client.PartSize {
		t.Fatalf("patch part size = %d, want %d", plan.PartSize, s3client.PartSize)
	}
	if len(plan.UploadParts) != 1 {
		t.Fatalf("upload parts = %d, want 1", len(plan.UploadParts))
	}
	part := plan.UploadParts[0]
	if part.Number != 2 {
		t.Fatalf("upload part number = %d, want 2", part.Number)
	}
	if part.Size != s3client.PartSize {
		t.Fatalf("upload part size = %d, want %d", part.Size, s3client.PartSize)
	}
	if part.ReadURL == "" {
		t.Fatal("expected read_url for dirty part within original file")
	}
	if got := part.ReadHeaders["Range"]; got != fmt.Sprintf("bytes=%d-%d", s3client.PartSize, 2*s3client.PartSize-1) {
		t.Fatalf("range header = %q, want %q", got, fmt.Sprintf("bytes=%d-%d", s3client.PartSize, 2*s3client.PartSize-1))
	}
	if len(plan.CopiedParts) != 2 || plan.CopiedParts[0] != 1 || plan.CopiedParts[1] != 3 {
		t.Fatalf("copied parts = %v, want [1 3]", plan.CopiedParts)
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
	largeBody := make([]byte, totalSize)
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/overwrite.bin", bytes.NewReader(largeBody))
	req.ContentLength = totalSize
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(largeBody))
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

	upload, err := s.fallback.GetUpload(context.Background(), plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}

	// Upload all parts through the local S3 stand-in.
	for _, p := range plan.Parts {
		start := int64(p.Number-1) * plan.PartSize
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

	nf, err := s.fallback.Store().Stat(context.Background(), "/overwrite.bin")
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

func TestAutoImageMultipartOverwriteWritesContentTextEndToEnd(t *testing.T) {
	s, s3c := newTestServerWithS3Config(t, backend.Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: backend.AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: staticServerImageExtractor{text: "caption from multipart overwrite"},
		},
	}, SemanticWorkerOptions{
		Workers:         1,
		PollInterval:    10 * time.Millisecond,
		RecoverInterval: 50 * time.Millisecond,
		LeaseDuration:   200 * time.Millisecond,
	})
	ts := httptest.NewServer(s)
	defer ts.Close()

	seedReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/overwrite-auto.png", bytes.NewReader([]byte("seed-image")))
	seedReq.ContentLength = int64(len("seed-image"))
	seedResp, err := http.DefaultClient.Do(seedReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seedResp.Body.Close() }()
	if seedResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(seedResp.Body)
		t.Fatalf("seed put: expected 200, got %d: %s", seedResp.StatusCode, body)
	}

	original := mustServerFile(t, s.fallback, "/overwrite-auto.png")
	waitForContentTextOnServer(t, s.fallback, "/overwrite-auto.png", "caption from multipart overwrite", 3*time.Second)
	waitForTaskStatus(t, s.fallback, original.FileID, 1, string(semantic.TaskSucceeded), 3*time.Second)

	totalSize := int64(2 << 20)
	body := make([]byte, totalSize)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/overwrite-auto.png", bytes.NewReader(body))
	req.ContentLength = totalSize
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
	resp, err := http.DefaultClient.Do(req)
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

	upload, err := s.fallback.GetUpload(context.Background(), plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range plan.Parts {
		start := int64(part.Number-1) * plan.PartSize
		end := start + part.Size
		if _, err := s3c.UploadPart(context.Background(), upload.S3UploadID, part.Number, bytes.NewReader(make([]byte, end-start))); err != nil {
			t.Fatalf("upload part %d: %v", part.Number, err)
		}
	}

	completeReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/complete", nil)
	completeResp, err := http.DefaultClient.Do(completeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = completeResp.Body.Close() }()
	if completeResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(completeResp.Body)
		t.Fatalf("complete overwrite: expected 200, got %d: %s", completeResp.StatusCode, body)
	}

	updated := mustServerFile(t, s.fallback, "/overwrite-auto.png")
	if updated.FileID != original.FileID {
		t.Fatalf("overwrite file_id=%q, want %q", updated.FileID, original.FileID)
	}
	if updated.Revision != 2 {
		t.Fatalf("revision=%d, want 2", updated.Revision)
	}
	waitForContentTextOnServer(t, s.fallback, "/overwrite-auto.png", "caption from multipart overwrite", 3*time.Second)
	waitForTaskStatus(t, s.fallback, updated.FileID, 2, string(semantic.TaskSucceeded), 3*time.Second)

	tasks := loadSemanticTaskRowsForResource(t, s.fallback, updated.FileID)
	if len(tasks) != 2 {
		t.Fatalf("semantic task count=%d, want 2", len(tasks))
	}
	for _, task := range tasks {
		if task.TaskType != string(semantic.TaskTypeImgExtractText) || task.Status != string(semantic.TaskSucceeded) {
			t.Fatalf("unexpected semantic task rows: %+v", tasks)
		}
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
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
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
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
	resp, _ := http.DefaultClient.Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first upload: expected 202, got %d", resp.StatusCode)
	}

	// Second upload for same path should fail
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/dup-test.bin", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
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
	req.Header.Set("X-Dat9-Part-Checksums", partChecksumHeader(body))
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
	upload, _ := s.fallback.GetUpload(context.Background(), plan.UploadID)
	if upload.Status != datastore.UploadAborted {
		t.Errorf("expected ABORTED, got %s", upload.Status)
	}
}

func TestParsePartChecksumsHeaderValidation(t *testing.T) {
	if _, err := parsePartChecksumsHeader(""); err != nil {
		t.Fatalf("empty header should be allowed: %v", err)
	}

	_, err := parsePartChecksumsHeader("not-base64")
	if err == nil || !strings.Contains(err.Error(), "invalid base64") {
		t.Fatalf("expected base64 error, got %v", err)
	}

	short := base64.StdEncoding.EncodeToString([]byte("short"))
	_, err = parsePartChecksumsHeader(short)
	if err == nil || !strings.Contains(err.Error(), "expected 32") {
		t.Fatalf("expected decoded length error, got %v", err)
	}
}

func TestUploadRespectsMaxUploadBytes(t *testing.T) {
	base, _ := newTestServerWithS3(t)
	s := NewWithConfig(Config{Backend: base.fallback, MaxUploadBytes: 10})
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/too-big.bin", bytes.NewReader([]byte("12345678901")))
	req.ContentLength = 11
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 413, got %d: %s", resp.StatusCode, body)
	}
}

func TestNewWithConfigUsesDefaultMaxUploadBytes(t *testing.T) {
	base, _ := newTestServerWithS3(t)
	s := NewWithConfig(Config{Backend: base.fallback})
	if s.maxUploadBytes != DefaultMaxUploadBytes {
		t.Fatalf("default maxUploadBytes = %d, want %d", s.maxUploadBytes, DefaultMaxUploadBytes)
	}
}

func TestDeclaredContentLengthOverMaxRejected(t *testing.T) {
	base, _ := newTestServerWithS3(t)
	s := NewWithConfig(Config{Backend: base.fallback, MaxUploadBytes: 10})
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/declared-too-big.bin", http.NoBody)
	req.Header.Set("X-Dat9-Content-Length", "11")
	req.ContentLength = 0
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 413, got %d: %s", resp.StatusCode, body)
	}
}

func TestContentLengthHeaderMismatchRejected(t *testing.T) {
	base, _ := newTestServerWithS3(t)
	s := NewWithConfig(Config{Backend: base.fallback, MaxUploadBytes: 1 << 20})
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/fs/mismatch.bin", bytes.NewReader([]byte("1234")))
	req.ContentLength = 4
	req.Header.Set("X-Dat9-Content-Length", "5")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}
}
