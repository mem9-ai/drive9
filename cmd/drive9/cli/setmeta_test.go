package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestSetMetaSendsTagsAndDescription(t *testing.T) {
	var gotMethod, gotPath, gotQuery string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":3}`))
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	if err := SetMeta(c, []string{"--tag", "owner=alice", "--tag=env=prod", "--description", "hello", "/a.txt"}); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/fs/a.txt" || gotQuery != "setmeta=1" {
		t.Fatalf("request = %s %s?%s, want POST /v1/fs/a.txt?setmeta=1", gotMethod, gotPath, gotQuery)
	}
	var body map[string]any
	if err := json.Unmarshal(gotBody, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	tags, ok := body["tags"].(map[string]any)
	if !ok || tags["owner"] != "alice" || tags["env"] != "prod" {
		t.Fatalf("body tags = %v", body["tags"])
	}
	if body["description"] != "hello" {
		t.Fatalf("body description = %v", body["description"])
	}
}

func TestSetMetaClearSendsEmptyValues(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	if err := SetMeta(c, []string{"--clear-tags", "--clear-description", "/a.txt"}); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(gotBody, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if string(body["tags"]) != `{}` {
		t.Fatalf("tags = %s, want {}", body["tags"])
	}
	if string(body["description"]) != `""` {
		t.Fatalf("description = %s, want empty string", body["description"])
	}
}

func TestSetMetaValidation(t *testing.T) {
	c := client.New("http://127.0.0.1:0", "")
	cases := [][]string{
		{"/a.txt"},               // nothing to update
		{"--tag", "owner=alice"}, // missing path
		{"--tag", "owner=alice", "--clear-tags", "/a"}, // conflicting tag flags
		{"--description", "x", "--clear-description", "/a"},
		{"--tag", "bad", "/a.txt"},                 // missing '='
		{"--tag", "a=b", "--tag", "a=c", "/a.txt"}, // duplicate key
		{"--bogus", "/a.txt"},
	}
	for _, args := range cases {
		if err := SetMeta(c, args); err == nil {
			t.Fatalf("SetMeta(%v) succeeded, want error", args)
		}
	}
}

func TestSetMetaServerErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
	}))
	defer srv.Close()

	c := client.New(srv.URL, "")
	err := SetMeta(c, []string{"--tag", "a=b", "/missing.txt"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("SetMeta err = %v, want not found", err)
	}
}
