package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestFixedUploadTransferOriginalIdentityAndBoundedResponses(t *testing.T) {
	for _, completing := range []bool{false, true} {
		for _, kind := range []string{"valid", "expired-plan", "truncated", "trailing", "invalid", "redirect", "unavailable", "missing", "zero-size", "missing-size", "null-size"} {
			if !completing && strings.HasSuffix(kind, "-size") {
				continue
			}
			t.Run(map[bool]string{false: "initiate/", true: "complete/"}[completing]+kind, func(t *testing.T) {
				var calls atomic.Int32
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					path := "/v1/views/a/v2/uploads/initiate"
					if completing {
						path = "/v1/views/a/v2/uploads/upload/complete"
					}
					if r.Method != "POST" || r.URL.Path != path || r.Header.Get(viewprotocol.OperationHeader) != "original" || r.Header.Get(viewprotocol.WorkHeader) != "work" || r.Header.Get("Authorization") != "Bearer test-token" {
						t.Error(r.Method, r.URL.Path, r.Header)
					}
					var body map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if completing {
						if len(body) != 1 || body["parts"] == nil {
							t.Error("unexpected completion fields", body)
						}
					} else if len(body) != 3 || string(body["path"]) != `"/file"` || string(body["total_size"]) != "6" || string(body["expected_revision"]) != "2" || r.Header.Get("X-Dat9-Expected-Resource") != "inode" {
						t.Error(body, r.Header)
					}
					status := 202
					var result any = extentUploadTestPlan(6)
					if completing {
						status = 200
						result = FixedUploadCompletion{Status: "completed", OperationID: "original", View: FixedViewState{LayerID: "a", Fence: 1, Generation: 3, Phase: "OPEN"}, ResourceID: "inode", Revision: 3, Size: 6}
					}
					switch kind {
					case "expired-plan":
						if !completing {
							p := result.(uploadPlanV2)
							p.ExpiresAt = time.Now().Add(-time.Hour).Format(time.RFC3339)
							result = p
						}
					case "zero-size", "missing-size", "null-size":
						raw, err := json.Marshal(result)
						if err != nil {
							t.Fatal(err)
						}
						var changed map[string]any
						if err := json.Unmarshal(raw, &changed); err != nil {
							t.Fatal(err)
						}
						switch kind {
						case "zero-size":
							changed["size_bytes"] = 0
						case "null-size":
							changed["size_bytes"] = nil
						case "missing-size":
							delete(changed, "size_bytes")
						}
						result = changed
					case "truncated":
						w.Header().Set("Content-Length", "10000")
					case "invalid":
						if completing {
							p := result.(FixedUploadCompletion)
							p.OperationID = "different"
							result = p
						} else {
							p := result.(uploadPlanV2)
							p.Resumable = true
							result = p
						}
					case "redirect":
						status = 307
						w.Header().Set("Location", "/main")
					case "unavailable":
						status = 503
					case "missing":
						status = 404
					}
					w.WriteHeader(status)
					_ = json.NewEncoder(w).Encode(result)
					if kind == "trailing" {
						_, _ = io.WriteString(w, " {}")
					}
				}))
				defer api.Close()
				c, err := New(api.URL, "test-token").WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
				var out any
				if completing {
					out, err = c.CompleteFixedExtentUpload(context.Background(), "original", "upload", []FixedExtentPart{{Number: 1, ETag: strings.Repeat("a", 64)}})
				} else {
					out, err = c.InitiateFixedExtentUpload(context.Background(), "original", "/file", 6, 2, "inode")
				}
				valid := kind == "valid" || kind == "expired-plan"
				if valid {
					if err != nil {
						t.Fatal(out, err)
					}
				} else {
					var identified *ViewOperationError
					if !errors.As(err, &identified) || identified.OperationID != "original" || identified.LayerID != "a" || (completing && !IsCommitAttempted(err)) {
						t.Fatal(out, err)
					}
					zero := any(FixedExtentUploadPlan{})
					if completing {
						zero = FixedUploadCompletion{}
					}
					if !reflect.DeepEqual(out, zero) {
						t.Fatal("partial result exposed", out)
					}
				}
				if calls.Load() != 1 {
					t.Fatal("automatic retry or fallback", calls.Load())
				}
			})
		}
	}
}

func TestFixedUploadTransferRejectsInvalidInputsWithoutIO(t *testing.T) {
	c := New("http://unused.invalid", "test-token")
	if _, err := c.InitiateFixedExtentUpload(context.Background(), "init", "/file", 1, -1, ""); err == nil {
		t.Fatal("main accepted")
	}
	c, _ = c.WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
	for _, tc := range []struct {
		path           string
		size, revision int64
		resource       string
	}{{"/", 1, 0, ""}, {"/dir/", 1, 0, ""}, {"/file", 0, 0, ""}, {"/file", 1, -2, ""}, {"/file", 256*(64<<20) + 1, 0, ""}, {"/file", 1, 0, "bad/id"}} {
		if _, err := c.InitiateFixedExtentUpload(context.Background(), "init", tc.path, tc.size, tc.revision, tc.resource); err == nil {
			t.Fatal(tc)
		}
	}
	for _, parts := range [][]FixedExtentPart{nil, {{Number: 1, ETag: "bad"}}, {{Number: 2, ETag: strings.Repeat("a", 64)}}, {{Number: 1, ETag: strings.Repeat("a", 64)}, {Number: 1, ETag: strings.Repeat("a", 64)}}, make([]FixedExtentPart, 257)} {
		if _, err := c.CompleteFixedExtentUpload(context.Background(), "complete", "upload", parts); err == nil {
			t.Fatal(parts)
		}
	}
	validation, _ := c.WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1, Validation: true})
	if _, err := validation.InitiateFixedExtentUpload(context.Background(), "init", "/file", 1, -1, ""); err == nil {
		t.Fatal("validation accepted")
	}
	if _, err := validation.CompleteFixedExtentUpload(context.Background(), "complete", "upload", []FixedExtentPart{{Number: 1, ETag: strings.Repeat("a", 64)}}); err == nil {
		t.Fatal("validation completed")
	}
}
