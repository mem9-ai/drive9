package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestMergeProbeOverwritesExtentThroughMultipart(t *testing.T) {
	for _, failure := range []string{"", "stat", "layout", "initiate", "complete"} {
		t.Run(failure, func(t *testing.T) {
			var calls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch r.URL.Path {
				case "/v1/fs/file":
					if r.Method != "HEAD" {
						t.Error("extent probe used direct PUT")
						w.WriteHeader(400)
						return
					}
					if failure == "stat" {
						w.WriteHeader(503)
						return
					}
					w.Header().Set("X-Dat9-Revision", "7")
					if failure != "layout" {
						w.Header().Set("X-Dat9-Content-Layout", "extent")
					}
				case "/v2/uploads/initiate":
					var in struct {
						Path             string
						TotalSize        int64 `json:"total_size"`
						ExpectedRevision int64 `json:"expected_revision"`
					}
					if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Path != "/file" || in.TotalSize != 10 || in.ExpectedRevision != 7 {
						t.Error(in, err)
						w.WriteHeader(400)
						return
					}
					if failure == "initiate" {
						w.WriteHeader(503)
						return
					}
					w.WriteHeader(202)
					_ = json.NewEncoder(w).Encode(map[string]any{"upload_id": "upload", "content_layout": "extent", "part_size": 4 << 20, "total_parts": 1, "expires_at": time.Now().Add(time.Minute).Format(time.RFC3339), "checksum_contract": map[string]any{"required": true, "supported": []string{"SHA-256"}}})
				case "/v2/uploads/upload/presign":
					sum := sha256.Sum256([]byte("newer main"))
					_ = json.NewEncoder(w).Encode(client.PartURL{Number: 1, Size: 10, ChecksumSHA256: hex.EncodeToString(sum[:]), SkipUpload: true, ExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339)})
				case "/v2/uploads/upload/complete":
					if failure == "complete" {
						w.WriteHeader(503)
						return
					}
					_, _ = io.WriteString(w, `{"status":"completed","revision":8,"resource_id":"inode"}`)
				default:
					t.Error("unexpected request", r.Method, r.URL.Path)
					w.WriteHeader(400)
				}
			}))
			defer s.Close()
			err := overwriteMergedMain(t.Context(), client.New(s.URL, ""))
			if (err != nil) != (failure != "") {
				t.Fatal(err)
			}
			want := int32(4)
			switch failure {
			case "stat", "layout":
				want = 1
			case "initiate":
				want = 2
			}
			if calls.Load() != want {
				t.Fatal("unexpected requests", calls.Load(), want)
			}
		})
	}
}
