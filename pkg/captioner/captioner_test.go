package captioner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid", "A cat sitting on a mat", false},
		{"empty", "", true},
		{"whitespace only", "   \n\t  ", true},
		{"too long", strings.Repeat("x", MaxCaptionLen+1), true},
		{"max length", strings.Repeat("x", MaxCaptionLen), false},
		{"trimmed", "  hello  ", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := validate(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				if !errors.Is(err, ErrInvalidOutput) {
					t.Errorf("expected ErrInvalidOutput, got %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != strings.TrimSpace(tt.input) {
					t.Errorf("got %q, want %q", result, strings.TrimSpace(tt.input))
				}
			}
		})
	}
}

func TestVisionCaptioner_Caption(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer test-key" {
				t.Errorf("bad auth: %s", r.Header.Get("Authorization"))
			}
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("bad content-type: %s", r.Header.Get("Content-Type"))
			}
			resp := chatResponse{Choices: []choice{{Message: responseMessage{Text: "A red cat on a blue mat"}}}}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "test-key", Model: "test-model", Endpoint: srv.URL})
		result, err := c.Caption(context.Background(), []byte{0xFF, 0xD8, 0xFF}, "image/jpeg")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "A red cat on a blue mat" {
			t.Errorf("got %q", result)
		}
	})

	t.Run("empty response", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := chatResponse{Choices: []choice{{Message: responseMessage{Text: ""}}}}
			json.NewEncoder(w).Encode(resp)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: srv.URL})
		_, err := c.Caption(context.Background(), []byte{0xFF}, "image/png")
		if !errors.Is(err, ErrInvalidOutput) {
			t.Errorf("expected ErrInvalidOutput, got %v", err)
		}
	})

	t.Run("whitespace response", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := chatResponse{Choices: []choice{{Message: responseMessage{Text: "   \n  "}}}}
			json.NewEncoder(w).Encode(resp)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: srv.URL})
		_, err := c.Caption(context.Background(), []byte{0xFF}, "image/png")
		if !errors.Is(err, ErrInvalidOutput) {
			t.Errorf("expected ErrInvalidOutput, got %v", err)
		}
	})

	t.Run("no choices", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := chatResponse{Choices: []choice{}}
			json.NewEncoder(w).Encode(resp)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: srv.URL})
		_, err := c.Caption(context.Background(), []byte{0xFF}, "image/png")
		if !errors.Is(err, ErrInvalidOutput) {
			t.Errorf("expected ErrInvalidOutput, got %v", err)
		}
	})

	t.Run("4xx non-retryable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: srv.URL})
		_, err := c.Caption(context.Background(), []byte{0xFF}, "image/png")
		if !errors.Is(err, ErrInvalidOutput) {
			t.Errorf("expected ErrInvalidOutput for 4xx, got %v", err)
		}
	})

	t.Run("5xx retryable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: srv.URL})
		_, err := c.Caption(context.Background(), []byte{0xFF}, "image/png")
		if err == nil {
			t.Fatal("expected error")
		}
		// 5xx should NOT be ErrInvalidOutput (retryable)
		if errors.Is(err, ErrInvalidOutput) {
			t.Errorf("5xx should not be ErrInvalidOutput")
		}
	})

	t.Run("image too large", func(t *testing.T) {
		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: "http://unused", MaxBytes: 100})
		_, err := c.Caption(context.Background(), make([]byte, 101), "image/png")
		if !errors.Is(err, ErrImageTooLarge) {
			t.Errorf("expected ErrImageTooLarge, got %v", err)
		}
	})

	t.Run("image within limit", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := chatResponse{Choices: []choice{{Message: responseMessage{Text: "small image"}}}}
			json.NewEncoder(w).Encode(resp)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: srv.URL, MaxBytes: 100})
		result, err := c.Caption(context.Background(), make([]byte, 50), "image/png")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "small image" {
			t.Errorf("got %q", result)
		}
	})

	t.Run("invalid content type", func(t *testing.T) {
		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: "http://unused"})
		_, err := c.Caption(context.Background(), []byte{0xFF}, "application/pdf")
		if !errors.Is(err, ErrInvalidOutput) {
			t.Errorf("expected ErrInvalidOutput for non-image content type, got %v", err)
		}
	})

	t.Run("too long caption", func(t *testing.T) {
		longCaption := strings.Repeat("word ", MaxCaptionLen/5+1)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := chatResponse{Choices: []choice{{Message: responseMessage{Text: longCaption}}}}
			json.NewEncoder(w).Encode(resp)
		}))
		defer srv.Close()

		c := NewVision(VisionConfig{APIKey: "k", Model: "m", Endpoint: srv.URL})
		_, err := c.Caption(context.Background(), []byte{0xFF}, "image/png")
		if !errors.Is(err, ErrInvalidOutput) {
			t.Errorf("expected ErrInvalidOutput for too-long caption, got %v", err)
		}
	})
}

func TestNewVisionFromEnv(t *testing.T) {
	t.Run("not configured", func(t *testing.T) {
		t.Setenv("DAT9_CAPTIONER_API_KEY", "")
		c := NewVisionFromEnv()
		if c != nil {
			t.Error("expected nil when API key not set")
		}
	})

	t.Run("configured with defaults", func(t *testing.T) {
		t.Setenv("DAT9_CAPTIONER_API_KEY", "my-key")
		t.Setenv("DAT9_CAPTIONER_MODEL", "")
		t.Setenv("DAT9_CAPTIONER_ENDPOINT", "")
		t.Setenv("DAT9_IMAGE_CAPTION_MAX_BYTES", "")
		c := NewVisionFromEnv()
		if c == nil {
			t.Fatal("expected non-nil")
		}
		if c.cfg.APIKey != "my-key" {
			t.Errorf("APIKey = %q", c.cfg.APIKey)
		}
		if c.cfg.Model != "gpt-4o" {
			t.Errorf("Model = %q", c.cfg.Model)
		}
		if c.cfg.Endpoint != "https://api.openai.com/v1" {
			t.Errorf("Endpoint = %q", c.cfg.Endpoint)
		}
		if c.cfg.MaxBytes != 0 {
			t.Errorf("MaxBytes = %d", c.cfg.MaxBytes)
		}
	})

	t.Run("configured with custom values", func(t *testing.T) {
		t.Setenv("DAT9_CAPTIONER_API_KEY", "sk-abc")
		t.Setenv("DAT9_CAPTIONER_MODEL", "gpt-4o-mini")
		t.Setenv("DAT9_CAPTIONER_ENDPOINT", "https://custom.openai-compatible.example/v1")
		t.Setenv("DAT9_IMAGE_CAPTION_MAX_BYTES", "5242880")
		c := NewVisionFromEnv()
		if c == nil {
			t.Fatal("expected non-nil")
		}
		if c.cfg.Model != "gpt-4o-mini" {
			t.Errorf("Model = %q", c.cfg.Model)
		}
		if c.cfg.Endpoint != "https://custom.openai-compatible.example/v1" {
			t.Errorf("Endpoint = %q", c.cfg.Endpoint)
		}
		if c.cfg.MaxBytes != 5242880 {
			t.Errorf("MaxBytes = %d", c.cfg.MaxBytes)
		}
	})
}
