package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMintObjectCredentialsPostsURIAndWrite(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_key_id":     "ASIA",
			"secret_access_key": "secret",
			"session_token":     "tok",
			"scheme":            "s3",
			"bucket":            "b",
			"prefix":            "cust",
		})
	}))
	defer srv.Close()

	out, err := New(srv.URL, "k").MintObjectCredentials(context.Background(), "s3://b/cust/a", true)
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/object-credentials" {
		t.Fatalf("request %s %s", gotMethod, gotPath)
	}
	if gotBody["uri"] != "s3://b/cust/a" || gotBody["write"] != true {
		t.Fatalf("body=%v", gotBody)
	}
	if out.AccessKeyID != "ASIA" || out.SessionToken != "tok" {
		t.Fatalf("out=%+v", out)
	}
}

func TestMintObjectCredentialsRejectsEmptyKeys(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"scheme": "s3"})
	}))
	defer srv.Close()
	if _, err := New(srv.URL, "k").MintObjectCredentials(context.Background(), "s3://b/a", false); err == nil {
		t.Fatal("expected empty credentials error")
	}
}

func TestMintObjectCredentialsAcceptsAzureSASAndGCSToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"sas_url": "https://acct.blob.core.windows.net/c?sig=1", "scheme": "az"})
	}))
	defer srv.Close()
	out, err := New(srv.URL, "k").MintObjectCredentials(context.Background(), "az://c/", false)
	if err != nil || out.SASURL == "" {
		t.Fatalf("out=%+v err=%v", out, err)
	}

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "ya29.tok", "scheme": "gs"})
	}))
	defer srv2.Close()
	out, err = New(srv2.URL, "k").MintObjectCredentials(context.Background(), "gs://b/", false)
	if err != nil || out.AccessToken != "ya29.tok" {
		t.Fatalf("gcs out=%+v err=%v", out, err)
	}
}
