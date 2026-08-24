package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testtidb"
	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

type patchCopyE2ES3 struct {
	s3client.S3Client

	mu sync.Mutex

	active          int
	maxActive       int
	calls           []int
	ranges          map[int][2]int64
	completedOrder  []int
	gates           map[int]chan struct{}
	started         chan int
	completed       chan int
	failPart        int
	failErr         error
	abortCalls      int
	activeAtAbort   int
	abortedKey      string
	abortedUploadID string
	aborted         chan struct{}
}

func (s *patchCopyE2ES3) UploadPartCopy(
	ctx context.Context,
	destKey string,
	uploadID string,
	partNumber int,
	sourceKey string,
	startByte int64,
	endByte int64,
) (etag string, err error) {
	s.mu.Lock()
	s.active++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	s.calls = append(s.calls, partNumber)
	s.ranges[partNumber] = [2]int64{startByte, endByte}
	gate := s.gates[partNumber]
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.active--
		if err == nil {
			s.completedOrder = append(s.completedOrder, partNumber)
		}
		s.mu.Unlock()
		if err == nil {
			s.completed <- partNumber
		}
	}()

	s.started <- partNumber
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if partNumber == s.failPart {
		return "", s.failErr
	}
	return s.S3Client.UploadPartCopy(ctx, destKey, uploadID, partNumber, sourceKey, startByte, endByte)
}

func (s *patchCopyE2ES3) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	s.mu.Lock()
	s.abortCalls++
	s.activeAtAbort = s.active
	s.abortedKey = key
	s.abortedUploadID = uploadID
	s.mu.Unlock()

	err := s.S3Client.AbortMultipartUpload(ctx, key, uploadID)
	s.aborted <- struct{}{}
	return err
}

func newPatchCopyE2EServer(
	t *testing.T,
	configure func(*patchCopyE2ES3),
) (*Server, *s3client.LocalS3Client, *patchCopyE2ES3) {
	t.Helper()

	s3Dir, err := os.MkdirTemp("", "dat9-patch-copy-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(s3Dir) })

	initServerTenantSchema(t, testDSN)
	store, err := datastore.Open(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	testtidb.ResetDB(t, store.DB())
	t.Cleanup(func() { _ = store.Close() })

	localS3, err := s3client.NewLocal(s3Dir, "/s3")
	if err != nil {
		t.Fatal(err)
	}
	recorder := &patchCopyE2ES3{
		S3Client:  localS3,
		ranges:    make(map[int][2]int64),
		gates:     make(map[int]chan struct{}),
		started:   make(chan int, 32),
		completed: make(chan int, 32),
		aborted:   make(chan struct{}, 4),
	}
	if configure != nil {
		configure(recorder)
	}

	b, err := backend.NewWithS3ModeAndOptions(store, recorder, true, backend.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)

	server := NewWithConfig(Config{Backend: b, LocalS3: localS3})
	t.Cleanup(server.Close)
	return server, localS3, recorder
}

func patchCopyE2EPartBounds(totalSize int64, partSize int64, partNumber int) (int64, int64) {
	start := int64(partNumber-1) * partSize
	end := start + partSize
	if end > totalSize {
		end = totalSize
	}
	return start, end
}

func patchCopyE2EBody(totalSize int64, partSize int64) []byte {
	body := make([]byte, int(totalSize))
	partCount := int((totalSize + partSize - 1) / partSize)
	for partNumber := 1; partNumber <= partCount; partNumber++ {
		start, end := patchCopyE2EPartBounds(totalSize, partSize, partNumber)
		for offset := start; offset < end; offset++ {
			body[offset] = byte((int64(partNumber)*37 + (offset-start)%251) % 256)
		}
	}
	return body
}

func patchCopyE2EExpected(original []byte, partSize int64, dirtyParts []int) []byte {
	expected := bytes.Clone(original)
	totalSize := int64(len(expected))
	for _, partNumber := range dirtyParts {
		start, end := patchCopyE2EPartBounds(totalSize, partSize, partNumber)
		for offset := start; offset < end; offset++ {
			expected[offset] = byte((int64(partNumber)*83 + (offset-start)%239) % 256)
		}
	}
	return expected
}

func waitPatchCopyE2EPart(t *testing.T, events <-chan int, want int) {
	t.Helper()
	select {
	case got := <-events:
		if got != want {
			t.Fatalf("copy event = part %d, want part %d", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for copy event for part %d", want)
	}
}

func waitPatchCopyE2EStarts(t *testing.T, started <-chan int, count int) []int {
	t.Helper()
	parts := make([]int, 0, count)
	for len(parts) < count {
		select {
		case partNumber := <-started:
			parts = append(parts, partNumber)
		case <-time.After(10 * time.Second):
			t.Fatalf("started %d copies, want %d", len(parts), count)
		}
	}
	return parts
}

func equalPatchCopyE2EParts(got []int, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func assertPatchCopyE2EPartSet(t *testing.T, got []int, want []int) {
	t.Helper()
	counts := make(map[int]int, len(got))
	for _, partNumber := range got {
		counts[partNumber]++
	}
	if len(counts) != len(want) {
		t.Fatalf("part set = %v, want %v", got, want)
	}
	for _, partNumber := range want {
		if counts[partNumber] != 1 {
			t.Fatalf("part %d count = %d in %v, want 1", partNumber, counts[partNumber], got)
		}
	}
}

func assertPatchCopyE2EPlanCoverage(t *testing.T, plan *backend.PatchPlan, partCount int) {
	t.Helper()
	counts := make(map[int]int, partCount)
	for _, partNumber := range plan.CopiedParts {
		counts[partNumber]++
	}
	for _, part := range plan.UploadParts {
		counts[part.Number]++
	}
	if len(counts) != partCount {
		t.Fatalf("patch plan covers %d distinct parts, want %d", len(counts), partCount)
	}
	for partNumber := 1; partNumber <= partCount; partNumber++ {
		if counts[partNumber] != 1 {
			t.Fatalf("patch plan part %d count = %d, want 1", partNumber, counts[partNumber])
		}
	}
}

func readPatchCopyE2EFile(t *testing.T, client *http.Client, baseURL string, path string) []byte {
	t.Helper()
	resp, err := client.Get(baseURL + "/v1/fs" + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200: %s", path, resp.StatusCode, body)
	}
	return body
}

func assertNoPatchCopyE2EUploadMetadata(t *testing.T, s *Server, path string) {
	t.Helper()
	var totalCount int
	if err := s.fallback.Store().DB().QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM uploads WHERE target_path = ?`,
		path,
	).Scan(&totalCount); err != nil {
		t.Fatalf("count uploads for %s: %v", path, err)
	}
	if totalCount != 1 {
		t.Fatalf("upload metadata for %s = %d rows, want only the 1 completed seed upload", path, totalCount)
	}

	var activeCount int
	err := s.fallback.Store().DB().QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM uploads
		 WHERE target_path = ? AND status IN (?, ?)`,
		path,
		datastore.UploadInitiated,
		datastore.UploadUploading,
	).Scan(&activeCount)
	if err != nil {
		t.Fatalf("count active uploads for %s: %v", path, err)
	}
	if activeCount != 0 {
		t.Fatalf("active upload metadata for %s = %d rows, want 0", path, activeCount)
	}
}

func assertPatchCopyE2EAbort(t *testing.T, localS3 *s3client.LocalS3Client, recorder *patchCopyE2ES3) {
	t.Helper()
	recorder.mu.Lock()
	abortCalls := recorder.abortCalls
	activeAtAbort := recorder.activeAtAbort
	abortedKey := recorder.abortedKey
	abortedUploadID := recorder.abortedUploadID
	recorder.mu.Unlock()

	if abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", abortCalls)
	}
	if activeAtAbort != 0 {
		t.Fatalf("active copies when abort started = %d, want 0", activeAtAbort)
	}
	if _, err := localS3.ListParts(context.Background(), abortedKey, abortedUploadID); err == nil {
		t.Fatal("ListParts after abort = nil error, want missing multipart upload")
	}
	if err := localS3.CompleteMultipartUpload(context.Background(), abortedKey, abortedUploadID, nil); err == nil {
		t.Fatal("CompleteMultipartUpload after abort = nil error, want missing multipart upload")
	}
}

type patchCopyE2EHTTPResult struct {
	resp *http.Response
	err  error
}

func startPatchCopyE2ERequest(client *http.Client, req *http.Request) <-chan patchCopyE2EHTTPResult {
	done := make(chan patchCopyE2EHTTPResult, 1)
	go func() {
		resp, err := client.Do(req)
		done <- patchCopyE2EHTTPResult{resp: resp, err: err}
	}()
	return done
}

func waitPatchCopyE2EHTTPResult(t *testing.T, done <-chan patchCopyE2EHTTPResult) patchCopyE2EHTTPResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for PATCH response")
		return patchCopyE2EHTTPResult{}
	}
}

func TestPatchCopyE2EConcurrentRetainedPartsPreserveWholeFile(t *testing.T) {
	const (
		path      = "/patch-copy-e2e.bin"
		partCount = 12
	)
	partSize := int64(s3client.MinPartSize)
	totalSize := int64(partCount-1)*partSize + 257
	dirtyParts := []int{2, 5, 9}
	retainedParts := []int{1, 3, 4, 6, 7, 8, 10, 11, 12}
	firstWave := []int{1, 3, 4, 6, 7, 8, 10, 11}
	completionOrder := []int{11, 12, 10, 8, 7, 6, 4, 3, 1}

	gates := make(map[int]chan struct{}, len(retainedParts))
	for _, partNumber := range retainedParts {
		gates[partNumber] = make(chan struct{})
	}
	s, localS3, recorder := newPatchCopyE2EServer(t, func(recorder *patchCopyE2ES3) {
		recorder.gates = gates
	})
	ts := httptest.NewServer(s)
	defer ts.Close()
	client := &http.Client{Timeout: 30 * time.Second}

	original := patchCopyE2EBody(totalSize, partSize)
	expected := patchCopyE2EExpected(original, partSize, dirtyParts)
	mustUploadLargeServerFile(t, ts, s, localS3, path, original)

	requestBody, err := json.Marshal(map[string]any{
		"new_size":    totalSize,
		"dirty_parts": dirtyParts,
		"part_size":   partSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPatch, ts.URL+"/v1/fs"+path, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	patchDone := startPatchCopyE2ERequest(client, req)

	started := waitPatchCopyE2EStarts(t, recorder.started, 8)
	assertPatchCopyE2EPartSet(t, started, firstWave)
	recorder.mu.Lock()
	callCountBeforeRelease := len(recorder.calls)
	maxActiveBeforeRelease := recorder.maxActive
	recorder.mu.Unlock()
	if callCountBeforeRelease != 8 {
		t.Fatalf("copy calls before release = %d, want 8", callCountBeforeRelease)
	}
	if maxActiveBeforeRelease != 8 {
		t.Fatalf("max active copies before release = %d, want 8", maxActiveBeforeRelease)
	}

	close(gates[11])
	waitPatchCopyE2EPart(t, recorder.completed, 11)
	waitPatchCopyE2EPart(t, recorder.started, 12)
	for _, partNumber := range completionOrder[1:] {
		close(gates[partNumber])
		waitPatchCopyE2EPart(t, recorder.completed, partNumber)
	}

	result := waitPatchCopyE2EHTTPResult(t, patchDone)
	if result.err != nil {
		t.Fatalf("PATCH %s: %v", path, result.err)
	}
	defer func() { _ = result.resp.Body.Close() }()
	responseBody, err := io.ReadAll(result.resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if result.resp.StatusCode != http.StatusAccepted {
		t.Fatalf("PATCH %s = %d, want 202: %s", path, result.resp.StatusCode, responseBody)
	}
	var plan backend.PatchPlan
	if err := json.Unmarshal(responseBody, &plan); err != nil {
		t.Fatalf("decode patch plan: %v", err)
	}
	if !equalPatchCopyE2EParts(plan.CopiedParts, retainedParts) {
		t.Fatalf("copied parts = %v, want %v", plan.CopiedParts, retainedParts)
	}
	gotDirtyParts := make([]int, 0, len(plan.UploadParts))
	for _, part := range plan.UploadParts {
		gotDirtyParts = append(gotDirtyParts, part.Number)
	}
	if !equalPatchCopyE2EParts(gotDirtyParts, dirtyParts) {
		t.Fatalf("dirty upload parts = %v, want %v", gotDirtyParts, dirtyParts)
	}
	assertPatchCopyE2EPlanCoverage(t, &plan, partCount)

	recorder.mu.Lock()
	calls := append([]int(nil), recorder.calls...)
	ranges := make(map[int][2]int64, len(recorder.ranges))
	for partNumber, byteRange := range recorder.ranges {
		ranges[partNumber] = byteRange
	}
	gotCompletionOrder := append([]int(nil), recorder.completedOrder...)
	maxActive := recorder.maxActive
	abortCalls := recorder.abortCalls
	recorder.mu.Unlock()
	assertPatchCopyE2EPartSet(t, calls, retainedParts)
	if maxActive != 8 {
		t.Fatalf("max active copies = %d, want 8", maxActive)
	}
	if !equalPatchCopyE2EParts(gotCompletionOrder, completionOrder) {
		t.Fatalf("copy completion order = %v, want %v", gotCompletionOrder, completionOrder)
	}
	if abortCalls != 0 {
		t.Fatalf("abort calls = %d, want 0", abortCalls)
	}
	for _, partNumber := range retainedParts {
		start, end := patchCopyE2EPartBounds(totalSize, partSize, partNumber)
		wantRange := [2]int64{start, end - 1}
		if got := ranges[partNumber]; got != wantRange {
			t.Fatalf("part %d copied range = %v, want %v", partNumber, got, wantRange)
		}
	}

	upload, err := s.fallback.GetUpload(context.Background(), plan.UploadID)
	if err != nil {
		t.Fatalf("GetUpload(%q): %v", plan.UploadID, err)
	}
	for _, part := range plan.UploadParts {
		start, end := patchCopyE2EPartBounds(totalSize, plan.PartSize, part.Number)
		if _, err := localS3.UploadPart(
			context.Background(),
			upload.S3UploadID,
			part.Number,
			bytes.NewReader(expected[start:end]),
		); err != nil {
			t.Fatalf("upload dirty part %d: %v", part.Number, err)
		}
	}

	completeReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/uploads/"+plan.UploadID+"/complete", nil)
	if err != nil {
		t.Fatal(err)
	}
	completeResp, err := client.Do(completeReq)
	if err != nil {
		t.Fatalf("complete patch upload: %v", err)
	}
	defer func() { _ = completeResp.Body.Close() }()
	completeBody, err := io.ReadAll(completeResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if completeResp.StatusCode != http.StatusOK {
		t.Fatalf("complete patch upload = %d, want 200: %s", completeResp.StatusCode, completeBody)
	}

	got := readPatchCopyE2EFile(t, client, ts.URL, path)
	if len(got) != len(expected) {
		t.Fatalf("patched file size = %d, want %d", len(got), len(expected))
	}
	gotSHA256 := sha256.Sum256(got)
	wantSHA256 := sha256.Sum256(expected)
	if gotSHA256 != wantSHA256 || !bytes.Equal(got, expected) {
		t.Fatalf("patched file bytes differ: sha256 got %x, want %x", gotSHA256, wantSHA256)
	}
}

func TestPatchCopyE2ECopyFailureLeavesOriginalAndNoUpload(t *testing.T) {
	const path = "/patch-copy-failure-e2e.bin"
	partSize := int64(s3client.MinPartSize)
	totalSize := 3*partSize + 193
	copyErr := errors.New("injected retained-part copy failure")

	s, localS3, recorder := newPatchCopyE2EServer(t, func(recorder *patchCopyE2ES3) {
		recorder.failPart = 2
		recorder.failErr = copyErr
	})
	ts := httptest.NewServer(s)
	defer ts.Close()
	client := &http.Client{Timeout: 30 * time.Second}

	original := patchCopyE2EBody(totalSize, partSize)
	mustUploadLargeServerFile(t, ts, s, localS3, path, original)

	requestBody, err := json.Marshal(map[string]any{
		"new_size":    totalSize,
		"dirty_parts": []int{4},
		"part_size":   partSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPatch, ts.URL+"/v1/fs"+path, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PATCH %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("PATCH %s = %d, want 500: %s", path, resp.StatusCode, responseBody)
	}

	assertPatchCopyE2EAbort(t, localS3, recorder)
	assertNoPatchCopyE2EUploadMetadata(t, s, path)
	got := readPatchCopyE2EFile(t, client, ts.URL, path)
	if gotSHA256, wantSHA256 := sha256.Sum256(got), sha256.Sum256(original); gotSHA256 != wantSHA256 || !bytes.Equal(got, original) {
		t.Fatalf("original file changed after copy failure: sha256 got %x, want %x", gotSHA256, wantSHA256)
	}
}

func TestPatchCopyE2ERequestCancellationAbortsAfterCopiesExit(t *testing.T) {
	const path = "/patch-copy-cancel-e2e.bin"
	partSize := int64(s3client.MinPartSize)
	totalSize := 2*partSize + 211
	gates := map[int]chan struct{}{
		1: make(chan struct{}),
		2: make(chan struct{}),
	}

	s, localS3, recorder := newPatchCopyE2EServer(t, func(recorder *patchCopyE2ES3) {
		recorder.gates = gates
	})
	ts := httptest.NewServer(s)
	defer ts.Close()
	client := &http.Client{Timeout: 30 * time.Second}

	original := patchCopyE2EBody(totalSize, partSize)
	mustUploadLargeServerFile(t, ts, s, localS3, path, original)

	requestBody, err := json.Marshal(map[string]any{
		"new_size":    totalSize,
		"dirty_parts": []int{3},
		"part_size":   partSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, ts.URL+"/v1/fs"+path, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	patchDone := startPatchCopyE2ERequest(client, req)

	started := waitPatchCopyE2EStarts(t, recorder.started, 2)
	assertPatchCopyE2EPartSet(t, started, []int{1, 2})
	cancel()

	result := waitPatchCopyE2EHTTPResult(t, patchDone)
	if result.resp != nil {
		_ = result.resp.Body.Close()
	}
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("PATCH cancellation error = %v, want context.Canceled", result.err)
	}
	select {
	case <-recorder.aborted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for detached multipart abort")
	}

	assertPatchCopyE2EAbort(t, localS3, recorder)
	assertNoPatchCopyE2EUploadMetadata(t, s, path)
	got := readPatchCopyE2EFile(t, client, ts.URL, path)
	if gotSHA256, wantSHA256 := sha256.Sum256(got), sha256.Sum256(original); gotSHA256 != wantSHA256 || !bytes.Equal(got, original) {
		t.Fatalf("original file changed after request cancellation: sha256 got %x, want %x", gotSHA256, wantSHA256)
	}
}
