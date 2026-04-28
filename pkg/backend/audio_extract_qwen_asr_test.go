package backend

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewQwenASRAudioTextExtractorValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  QwenASRAudioTextExtractorConfig
		want string
	}{
		{
			name: "missing_base_url",
			cfg: QwenASRAudioTextExtractorConfig{
				APIKey: "secret",
				Model:  "qwen3-asr-flash",
			},
			want: "qwen asr extractor base url is required",
		},
		{
			name: "missing_api_key",
			cfg: QwenASRAudioTextExtractorConfig{
				BaseURL: "https://example.com",
				Model:   "qwen3-asr-flash",
			},
			want: "qwen asr extractor api key is required",
		},
		{
			name: "missing_model",
			cfg: QwenASRAudioTextExtractorConfig{
				BaseURL: "https://example.com",
				APIKey:  "secret",
			},
			want: "qwen asr extractor model is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewQwenASRAudioTextExtractor(tc.cfg)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestNewQwenASRAudioTextExtractorEndpointCanonicalization(t *testing.T) {
	tests := []struct {
		baseURL string
		want    string
	}{
		{baseURL: "https://dashscope.aliyuncs.com/compatible-mode", want: "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"},
		{baseURL: "https://dashscope.aliyuncs.com/compatible-mode/", want: "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"},
		{baseURL: "https://dashscope.aliyuncs.com/compatible-mode/v1", want: "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"},
		{baseURL: "https://dashscope.aliyuncs.com/compatible-mode/v1/", want: "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"},
	}

	for _, tc := range tests {
		extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
			BaseURL: tc.baseURL,
			APIKey:  "secret",
			Model:   "qwen3-asr-flash",
		})
		if err != nil {
			t.Fatalf("NewQwenASRAudioTextExtractor(%q): %v", tc.baseURL, err)
		}
		if extractor.endpoint != tc.want {
			t.Fatalf("endpoint=%q, want %q", extractor.endpoint, tc.want)
		}
	}
}

func TestQwenASRAudioTextExtractorExtractAudioText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path=%q, want /v1/chat/completions", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer secret" {
			t.Fatalf("authorization=%q, want Bearer secret", auth)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content-type=%q, want application/json", ct)
		}
		var payload struct {
			Model    string `json:"model"`
			Stream   bool   `json:"stream"`
			Messages []struct {
				Role    string `json:"role"`
				Content []struct {
					Text       string `json:"text"`
					Type       string `json:"type"`
					InputAudio *struct {
						Data string `json:"data"`
					} `json:"input_audio"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload.Model != "qwen3-asr-flash" {
			t.Fatalf("model=%q, want qwen3-asr-flash", payload.Model)
		}
		if payload.Stream {
			t.Fatal("stream=true, want false")
		}
		if len(payload.Messages) != 2 {
			t.Fatalf("messages=%d, want 2", len(payload.Messages))
		}
		if payload.Messages[0].Role != "system" || payload.Messages[0].Content[0].Text != "transcribe in zh" {
			t.Fatalf("system message=%#v", payload.Messages[0])
		}
		user := payload.Messages[1]
		if user.Role != "user" || len(user.Content) != 1 {
			t.Fatalf("user message=%#v", user)
		}
		audio := user.Content[0]
		if audio.Type != "input_audio" || audio.InputAudio == nil {
			t.Fatalf("audio content=%#v", audio)
		}
		wantData := "data:audio/mpeg;base64," + base64.StdEncoding.EncodeToString([]byte("fake-mp3"))
		if audio.InputAudio.Data != wantData {
			t.Fatalf("audio data=%q, want %q", audio.InputAudio.Data, wantData)
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":" spoken transcript "}}],"usage":{"prompt_tokens":42,"completion_tokens":7,"seconds":3}}`))
	}))
	defer server.Close()

	extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
		BaseURL: server.URL + "/v1",
		APIKey:  "secret",
		Model:   "qwen3-asr-flash",
		Prompt:  "transcribe in zh",
	})
	if err != nil {
		t.Fatal(err)
	}

	got, usage, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path:        "/audio/clip.mp3",
		ContentType: "audio/mpeg; charset=binary",
		Data:        []byte("fake-mp3"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "spoken transcript" {
		t.Fatalf("text=%q, want spoken transcript", got)
	}
	if usage.InputTokens != 42 {
		t.Fatalf("input_tokens=%d, want 42", usage.InputTokens)
	}
	if usage.OutputTokens != 7 {
		t.Fatalf("output_tokens=%d, want 7", usage.OutputTokens)
	}
	if usage.DurationSeconds != 3 {
		t.Fatalf("duration=%v, want 3", usage.DurationSeconds)
	}
}

func TestQwenASRAudioTextExtractorErrorMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":{"message":"upstream unavailable"}}`))
	}))
	defer server.Close()

	extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "qwen3-asr-flash",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = extractor.ExtractAudioText(context.Background(), AudioExtractRequest{Path: "/audio/clip.mp3", Data: []byte("fake")})
	if err == nil || !strings.Contains(err.Error(), "qwen asr api status 502: upstream unavailable") {
		t.Fatalf("err=%v, want upstream unavailable status", err)
	}
	if IsNonRetryableAudioExtractError(err) {
		t.Fatalf("err=%v, should be retryable", err)
	}
}

func TestQwenASRAudioTextExtractorRejectsOversizedBase64Payload(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "qwen3-asr-flash",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path:        "/audio/too-large.mp3",
		ContentType: "audio/mpeg",
		Data:        make([]byte, 8<<20),
	})
	if err == nil {
		t.Fatal("expected oversized payload error")
	}
	var apiErr *AudioExtractAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err=%T %[1]v, want AudioExtractAPIError", err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want %d", apiErr.StatusCode, http.StatusBadRequest)
	}
	if !strings.Contains(apiErr.Message, "10 MB limit") {
		t.Fatalf("message=%q, want limit detail", apiErr.Message)
	}
	if !IsNonRetryableAudioExtractError(err) {
		t.Fatalf("err=%v, want non-retryable", err)
	}
	if called {
		t.Fatal("server should not be called for oversized payload")
	}
}

func TestQwenASRAudioTextExtractorNoChoicesErrorIncludesRawResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[],"request_id":"abc"}`))
	}))
	defer server.Close()

	extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "qwen3-asr-flash",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path: "/audio/malformed.mp3",
		Data: []byte("fake"),
	})
	if err == nil {
		t.Fatal("expected no choices error")
	}
	if !strings.Contains(err.Error(), "qwen asr api returned no choices") || !strings.Contains(err.Error(), `"request_id":"abc"`) {
		t.Fatalf("err=%v, want raw response detail", err)
	}
}

func TestQwenASRAudioTextExtractorAllowsLargeSuccessfulResponse(t *testing.T) {
	transcript := strings.Repeat("a", (1<<20)+1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"content": transcript}},
			},
		})
	}))
	defer server.Close()

	extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "qwen3-asr-flash",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path: "/audio/long.mp3",
		Data: []byte("fake"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != transcript {
		t.Fatalf("text length=%d, want %d", len(got), len(transcript))
	}
}

func TestQwenASRAudioTextExtractorDecodeErrorIncludesRawResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[`))
	}))
	defer server.Close()

	extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "qwen3-asr-flash",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path: "/audio/bad-json.mp3",
		Data: []byte("fake"),
	})
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !strings.Contains(err.Error(), "decode qwen asr response") || !strings.Contains(err.Error(), `raw={"choices":[`) {
		t.Fatalf("err=%v, want raw response detail", err)
	}
}

func TestQwenASRAudioTextExtractorNonRetryableClientErrors(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		message    string
	}{
		{
			name:       "free_tier_exhausted",
			statusCode: http.StatusForbidden,
			message:    "The free tier of the model has been exhausted.",
		},
		{
			name:       "illegal_audio_format",
			statusCode: http.StatusBadRequest,
			message:    "<400> InternalError.Algo.InvalidParameter: The audio format is illegal and cannot be opened",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]string{"message": tc.message},
				})
			}))
			defer server.Close()

			extractor, err := NewQwenASRAudioTextExtractor(QwenASRAudioTextExtractorConfig{
				BaseURL: server.URL,
				APIKey:  "secret",
				Model:   "qwen3-asr-flash",
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
				Path: "/audio/clip.mp3",
				Data: []byte("fake"),
			})
			if err == nil {
				t.Fatal("expected error")
			}
			var apiErr *AudioExtractAPIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("err=%T %[1]v, want AudioExtractAPIError", err)
			}
			if apiErr.StatusCode != tc.statusCode {
				t.Fatalf("status=%d, want %d", apiErr.StatusCode, tc.statusCode)
			}
			if !IsNonRetryableAudioExtractError(err) {
				t.Fatalf("err=%v, want non-retryable", err)
			}
		})
	}
}
