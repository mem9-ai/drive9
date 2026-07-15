package fuse

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestValidateWorkspaceRootRequestBehavior(t *testing.T) {
	tests := []struct {
		name         string
		headStatus   int
		headIsDir    bool
		listStatus   int
		wantRequests []string
		wantErr      string
	}{
		{
			name:         "stat succeeds",
			headStatus:   http.StatusOK,
			headIsDir:    true,
			wantRequests: []string{"HEAD /v1/fs/"},
		},
		{
			name:         "not found falls back once",
			headStatus:   http.StatusNotFound,
			listStatus:   http.StatusOK,
			wantRequests: []string{"HEAD /v1/fs/", "GET /v1/fs/?list=1"},
		},
		{
			name:         "method not allowed falls back once",
			headStatus:   http.StatusMethodNotAllowed,
			listStatus:   http.StatusOK,
			wantRequests: []string{"HEAD /v1/fs/", "GET /v1/fs/?list=1"},
		},
		{
			name:         "authentication failure does not fall back",
			headStatus:   http.StatusUnauthorized,
			wantRequests: []string{"HEAD /v1/fs/"},
			wantErr:      "HTTP 401",
		},
		{
			name:         "server failure does not fall back",
			headStatus:   http.StatusInternalServerError,
			wantRequests: []string{"HEAD /v1/fs/"},
			wantErr:      "HTTP 500",
		},
		{
			name:         "root must be a directory",
			headStatus:   http.StatusOK,
			wantRequests: []string{"HEAD /v1/fs/"},
			wantErr:      `remote root "/" is not a directory`,
		},
		{
			name:         "fallback list failure is returned",
			headStatus:   http.StatusNotFound,
			listStatus:   http.StatusInternalServerError,
			wantRequests: []string{"HEAD /v1/fs/", "GET /v1/fs/?list=1"},
			wantErr:      "HTTP 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.RequestURI())
				mu.Unlock()

				switch r.Method {
				case http.MethodHead:
					if tt.headIsDir {
						w.Header().Set("X-Dat9-IsDir", "true")
					} else {
						w.Header().Set("X-Dat9-IsDir", "false")
					}
					w.WriteHeader(tt.headStatus)
				case http.MethodGet:
					if r.URL.Query().Get("list") != "1" {
						t.Errorf("list query = %q, want 1", r.URL.Query().Get("list"))
					}
					listStatus := tt.listStatus
					if listStatus == 0 {
						listStatus = http.StatusOK
					}
					w.WriteHeader(listStatus)
					if listStatus < http.StatusMultipleChoices {
						_, _ = w.Write([]byte(`{"entries":[]}`))
					}
				default:
					t.Errorf("request method = %s, want HEAD or GET", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			t.Cleanup(server.Close)

			err := validateWorkspaceRoot(client.New(server.URL, ""))
			if tt.wantErr == "" && err != nil {
				t.Fatalf("validateWorkspaceRoot() error = %v, want nil", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("validateWorkspaceRoot() error = %v, want containing %q", err, tt.wantErr)
			}

			mu.Lock()
			defer mu.Unlock()
			if len(requests) != len(tt.wantRequests) {
				t.Fatalf("requests = %v, want %v", requests, tt.wantRequests)
			}
			for i := range tt.wantRequests {
				if requests[i] != tt.wantRequests[i] {
					t.Fatalf("requests[%d] = %q, want %q", i, requests[i], tt.wantRequests[i])
				}
			}
		})
	}
}
