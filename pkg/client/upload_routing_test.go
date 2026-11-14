package client

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestUploadRoutingBoundary(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		name := "main"
		if fixed {
			name = "fixed"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name            string
				size, threshold int64
				direct          bool
			}{
				{"empty-unknown", 0, 0, true},
				{"empty-known", 0, 4, true},
				{"unknown", 1, 0, false},
				{"below", 3, 4, true},
				{"equal", 4, 4, false},
				{"above", 5, 4, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if got := UseDirectPUT(tc.size, tc.threshold); got != tc.direct {
						t.Fatalf("direct=%v want=%v", got, tc.direct)
					}
					calls := 0
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls++
						method, path := http.MethodPost, "/v2/uploads/initiate"
						if tc.direct {
							method, path = http.MethodPut, "/v1/fs/file"
						}
						if fixed {
							path = "/v1/views/a" + path
							if r.Header.Get(viewprotocol.OperationHeader) == "" {
								t.Error("missing operation identity")
							}
						}
						if r.Method != method || r.URL.Path != path {
							t.Errorf("route=%s %s want=%s %s", r.Method, r.URL.Path, method, path)
						}
						// A failed selected protocol must not fall back to another route.
						w.WriteHeader(http.StatusServiceUnavailable)
					}))
					defer server.Close()
					cl := New(server.URL, "")
					if fixed {
						var err error
						cl, err = cl.WithView(ViewBinding{LayerID: "a", WorkID: "work", Fence: 1})
						if err != nil {
							t.Fatal(err)
						}
					}
					cl.SetSmallFileThresholdForTests(tc.threshold)
					data := make([]byte, tc.size)
					if err := cl.WriteStreamConditional(t.Context(), "/file", bytes.NewReader(data), tc.size, nil, 1); err == nil || calls != 1 {
						t.Fatalf("failed route: calls=%d err=%v", calls, err)
					}
				})
			}
		})
	}
}
