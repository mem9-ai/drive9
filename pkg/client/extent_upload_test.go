package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func extentUploadTestPlan(size int64) uploadPlanV2 {
	return uploadPlanV2{UploadID: "upload", ContentLayout: ContentLayoutExtent, PartSize: 4 << 20,
		TotalParts: int((size-1)/(4<<20) + 1), ExpiresAt: time.Now().Add(time.Hour).Format(time.RFC3339),
		ChecksumContract: checksumContract{Required: true, Supported: []string{"SHA-256"}}}
}

func TestExtentUploadUsesChecksummedPartsAndDedup(t *testing.T) {
	data := bytes.Repeat([]byte("a"), (8<<20)+3)
	var puts, completed, progress atomic.Int32
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		puts.Add(1)
		if r.Method != "PUT" || r.Header.Get("If-None-Match") != "*" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("x-amz-checksum-crc32c") != "" {
			t.Errorf("invalid object request method/headers: %s %v", r.Method, r.Header)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		digest := sha256.Sum256(body)
		if r.URL.Path != "/"+hex.EncodeToString(digest[:]) || r.ContentLength != int64(len(body)) {
			t.Error("object did not receive the checksummed part")
		}
		w.Header().Set("ETag", "not-the-content-hash")
		w.WriteHeader(200)
	}))
	defer object.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing API authorization")
		}
		switch r.URL.Path {
		case "/v2/uploads/initiate":
			var req struct {
				Path      string
				TotalSize int64 `json:"total_size"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Path != "/big.bin" || req.TotalSize != int64(len(data)) {
				t.Error("initiate request", req, err)
			}
			w.WriteHeader(202)
			_ = json.NewEncoder(w).Encode(extentUploadTestPlan(int64(len(data))))
		case "/v2/uploads/upload/presign":
			var req struct {
				Number   int `json:"part_number"`
				Checksum struct{ Algorithm, Value string }
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Number < 1 || req.Number > 3 || req.Checksum.Algorithm != "sha256" {
				t.Error("presign request", req, err)
				w.WriteHeader(400)
				return
			}
			offset := (req.Number - 1) * (4 << 20)
			n := min(4<<20, len(data)-offset)
			digest := sha256.Sum256(data[offset : offset+n])
			if req.Checksum.Value != hex.EncodeToString(digest[:]) {
				t.Error("checksum mismatch")
			}
			part := PartURL{Number: req.Number, Size: int64(n), ChecksumSHA256: req.Checksum.Value,
				URL: object.URL + "/" + req.Checksum.Value, Headers: map[string]string{"If-None-Match": "*"}, ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)}
			if req.Number == 2 {
				part.SkipUpload, part.URL, part.Headers = true, "", nil
			}
			_ = json.NewEncoder(w).Encode(part)
		case "/v2/uploads/upload/complete":
			completed.Add(1)
			var req struct {
				Parts []completePart
				Tags  map[string]string
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Parts) != 3 || req.Tags["purpose"] != "test" {
				t.Error("completion", req, err)
				w.WriteHeader(400)
				return
			}
			for i, part := range req.Parts {
				offset := i * (4 << 20)
				n := min(4<<20, len(data)-offset)
				digest := sha256.Sum256(data[offset : offset+n])
				if part.Number != i+1 || part.ETag != hex.EncodeToString(digest[:]) {
					t.Error("completion did not use ordered content hashes", part)
				}
			}
			_, _ = io.WriteString(w, `{"status":"completed","revision":1,"resource_id":"inode"}`)
		default:
			t.Error("unexpected route", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := New(api.URL, "test-token")
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(object.URL)
	jar.SetCookies(u, []*http.Cookie{{Name: "session", Value: "private"}})
	c.httpClient.Jar = jar
	summary, err := c.WriteStreamWithSummaryAndTags(context.Background(), "/big.bin", bytes.NewReader(data), int64(len(data)), func(int, int, int64) { progress.Add(1) }, map[string]string{"purpose": "test"})
	if err != nil || summary.Mode != "extent_v2" || puts.Load() != 2 || completed.Load() != 1 || progress.Load() != 3 || summary.UploadedParts != 3 {
		t.Fatalf("summary=%+v err=%v puts=%d complete=%d progress=%d", summary, err, puts.Load(), completed.Load(), progress.Load())
	}
}

func TestExtentUploadRejectsBadPlanBeforeStorage(t *testing.T) {
	for _, tc := range []string{"part size", "count", "too many", "id", "expiry", "checksum", "key", "truncated", "trailing", "unknown layout"} {
		t.Run(tc, func(t *testing.T) {
			var calls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				p := extentUploadTestPlan(1)
				switch tc {
				case "part size":
					p.PartSize = 1 << 40
				case "count":
					p.TotalParts = 2
				case "too many":
					p.TotalParts = 10001
				case "id":
					p.UploadID = "../other"
				case "expiry":
					p.ExpiresAt = time.Now().Add(-time.Hour).Format(time.RFC3339)
				case "checksum":
					p.ChecksumContract.Supported = []string{"CRC32C"}
				case "key":
					p.Key = "multipart"
				case "truncated":
					w.Header().Set("Content-Length", "9999")
				case "unknown layout":
					p.ContentLayout = "future"
				}
				w.WriteHeader(202)
				_ = json.NewEncoder(w).Encode(p)
				if tc == "trailing" {
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			if err := New(api.URL, "test-token").WriteStream(context.Background(), "/a", bytes.NewReader([]byte("a")), 1, nil); err == nil || calls.Load() != 1 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestExtentUploadCompletionUnknownKeepsSession(t *testing.T) {
	for _, failure := range []string{"truncated", "trailing", "empty", "revision", "identity", "unknown", "precheck", "short source"} {
		t.Run(failure, func(t *testing.T) {
			var aborts, completes atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v2/uploads/initiate":
					w.WriteHeader(202)
					_ = json.NewEncoder(w).Encode(extentUploadTestPlan(1))
				case "/v2/uploads/upload/presign":
					digest := sha256.Sum256([]byte("a"))
					_ = json.NewEncoder(w).Encode(PartURL{Number: 1, Size: 1, ChecksumSHA256: hex.EncodeToString(digest[:]), SkipUpload: true, ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)})
				case "/v2/uploads/upload/complete":
					completes.Add(1)
					body := `{"status":"completed","revision":1,"resource_id":"inode"}`
					switch failure {
					case "truncated":
						w.Header().Set("Content-Length", "9999")
					case "trailing":
						body += `{}`
					case "empty":
						body = ""
					case "revision":
						body = `{"status":"completed","resource_id":"inode"}`
					case "identity":
						body = `{"status":"completed","revision":1}`
					case "unknown":
						w.WriteHeader(503)
					}
					_, _ = io.WriteString(w, body)
				case "/v2/uploads/upload/abort":
					aborts.Add(1)
					_, _ = io.WriteString(w, `{}`)
				default:
					t.Error("unexpected", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer api.Close()
			data := []byte("a")
			if failure == "short source" {
				data = nil
			}
			check := func() error { return nil }
			if failure == "precheck" {
				check = func() error { return errors.New("precheck failed") }
			}
			_, err := New(api.URL, "test-token").writeStreamConditionalWithSummaryAndPreCompleteCheck(context.Background(), "/a", bytes.NewReader(data), 1, nil, 0, nil, "", "", check)
			pre := failure == "precheck" || failure == "short source"
			if err == nil || IsCommitAttempted(err) == pre || !strings.Contains(err.Error(), "upload") {
				t.Fatalf("err=%v", err)
			}
			if pre {
				if aborts.Load() != 1 || completes.Load() != 0 {
					t.Fatal(aborts.Load(), completes.Load())
				}
			} else if aborts.Load() != 0 || completes.Load() != 1 {
				t.Fatal("unknown completion was aborted", aborts.Load(), completes.Load())
			}
		})
	}
}

func TestExtentUploadObjectRetryAndRedirect(t *testing.T) {
	for _, status := range []int{200, 403, 412, 307, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var signs, puts atomic.Int32
			object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				puts.Add(1)
				if r.URL.Path != "/object" {
					t.Error("followed redirect")
				}
				w.Header().Set("Location", "/unexpected")
				w.WriteHeader(status)
			}))
			defer object.Close()
			digest := sha256.Sum256([]byte("a"))
			checksum := hex.EncodeToString(digest[:])
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := signs.Add(1)
				var req struct {
					Number   int `json:"part_number"`
					Checksum struct{ Algorithm, Value string }
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Number != 1 || req.Checksum.Value != checksum {
					t.Error(req, err)
				}
				p := PartURL{Number: 1, Size: 1, ChecksumSHA256: checksum, URL: object.URL + "/object?signature=test", ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)}
				if n == 2 {
					p.SkipUpload, p.URL = true, ""
				}
				_ = json.NewEncoder(w).Encode(p)
			}))
			defer api.Close()
			err := New(api.URL, "test-token").uploadExtentPart(context.Background(), "upload", 1, []byte("a"), checksum)
			wantSigns := int32(1)
			if status == 403 || status == 412 {
				wantSigns = 2
			}
			wantErr := status == 307 || status == 500
			if (err != nil) != wantErr || signs.Load() != wantSigns || puts.Load() != 1 || (err != nil && strings.Contains(err.Error(), "signature=")) {
				t.Fatalf("err=%v signs=%d puts=%d", err, signs.Load(), puts.Load())
			}
		})
	}
}

func TestExtentUploadCancelsPeerOnPartFailure(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	var aborts, completes atomic.Int32
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/1" {
			close(started)
			<-r.Context().Done()
			close(canceled)
			return
		}
		select {
		case <-started:
			w.WriteHeader(500)
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
			t.Error("peer request did not start")
			w.WriteHeader(500)
		}
	}))
	defer object.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/uploads/initiate":
			w.WriteHeader(202)
			_ = json.NewEncoder(w).Encode(extentUploadTestPlan(8 << 20))
		case "/v2/uploads/upload/presign":
			var req struct {
				Number   int `json:"part_number"`
				Checksum struct{ Algorithm, Value string }
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(PartURL{Number: req.Number, Size: 4 << 20, ChecksumSHA256: req.Checksum.Value,
				URL: fmt.Sprintf("%s/%d", object.URL, req.Number), ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)})
		case "/v2/uploads/upload/abort":
			aborts.Add(1)
		case "/v2/uploads/upload/complete":
			completes.Add(1)
		default:
			t.Error("unexpected route", r.URL.Path)
		}
	}))
	defer api.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := New(api.URL, "test-token").WriteStream(ctx, "/a", bytes.NewReader(make([]byte, 8<<20)), 8<<20, nil)
	if err == nil || !strings.Contains(err.Error(), "500") || aborts.Load() != 1 || completes.Load() != 0 {
		t.Fatalf("err=%v aborts=%d completes=%d", err, aborts.Load(), completes.Load())
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("failed part did not cancel the concurrent PUT")
	}
}

func TestExtentUploadRejectsObjectCredentialHeaders(t *testing.T) {
	var calls atomic.Int32
	object := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer object.Close()
	for _, header := range []string{"Authorization", "Cookie", "Proxy-Authorization", "Host", "X-Drive9-Work-ID", "X-Dat9-Actor"} {
		_, err := New(object.URL, "test-token").putExtentPart(context.Background(), PartURL{URL: object.URL, Headers: map[string]string{header: "forbidden"}}, []byte("a"))
		if err == nil {
			t.Errorf("accepted %s", header)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("forbidden request reached storage")
	}
}

func TestExtentUploadPlanBounds(t *testing.T) {
	for _, size := range []int64{1, 4 << 20, (4 << 20) + 1, (64 << 20) * 10000} {
		plan := extentUploadTestPlan(size)
		if size > (4<<20)*10000 {
			plan.PartSize = 64 << 20
			plan.TotalParts = int((size-1)/plan.PartSize + 1)
		}
		if err := validateExtentUploadPlan(&plan, size); err != nil {
			t.Fatal(size, err)
		}
	}
	plan := extentUploadTestPlan(1)
	if err := validateExtentUploadPlan(&plan, 0); err == nil {
		t.Fatal("accepted empty multipart")
	}
	if err := validateExtentUploadPlan(nil, 1); err == nil {
		t.Fatal("accepted missing plan")
	}
}
