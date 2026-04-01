package embedding

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFormatVector(t *testing.T) {
	got := FormatVector([]float32{0.1, float32(math.NaN()), float32(math.Inf(1)), 0.3})
	if got != "[0.1,0,0,0.3]" {
		t.Fatalf("FormatVector=%q, want %q", got, "[0.1,0,0,0.3]")
	}
}

func TestOpenAIClientEmbedText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" {
			t.Fatalf("path=%q, want /v1/embeddings", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer secret" {
			t.Fatalf("authorization=%q, want Bearer secret", auth)
		}
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.1,0.2,0.3]}]}`))
	}))
	defer server.Close()

	client, err := NewOpenAIClient(OpenAIClientConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "text-embedding-3-small",
	})
	if err != nil {
		t.Fatal(err)
	}
	vec, err := client.EmbedText(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if len(vec) != 3 || vec[0] != 0.1 || vec[2] != 0.3 {
		t.Fatalf("unexpected embedding: %#v", vec)
	}
}

func TestOpenAIClientEmbedTextError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":{"message":"upstream unavailable"}}`))
	}))
	defer server.Close()

	client, err := NewOpenAIClient(OpenAIClientConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "text-embedding-3-small",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.EmbedText(context.Background(), "hello"); err == nil || err.Error() != "embedding api status 502: upstream unavailable" {
		t.Fatalf("unexpected error: %v", err)
	}
}
