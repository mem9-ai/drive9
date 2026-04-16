package backend

import (
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewOpenAIAudioTextExtractorValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  OpenAIAudioTextExtractorConfig
		want string
	}{
		{
			name: "missing_base_url",
			cfg: OpenAIAudioTextExtractorConfig{
				APIKey: "secret",
				Model:  "whisper-1",
			},
			want: "audio extractor base url is required",
		},
		{
			name: "missing_api_key",
			cfg: OpenAIAudioTextExtractorConfig{
				BaseURL: "https://example.com",
				Model:   "whisper-1",
			},
			want: "audio extractor api key is required",
		},
		{
			name: "missing_model",
			cfg: OpenAIAudioTextExtractorConfig{
				BaseURL: "https://example.com",
				APIKey:  "secret",
			},
			want: "audio extractor model is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewOpenAIAudioTextExtractor(tc.cfg)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestNewOpenAIAudioTextExtractorEndpointCanonicalization(t *testing.T) {
	tests := []struct {
		baseURL string
		want    string
	}{
		{baseURL: "https://example.com", want: "https://example.com/v1/audio/transcriptions"},
		{baseURL: "https://example.com/", want: "https://example.com/v1/audio/transcriptions"},
		{baseURL: "https://example.com/v1", want: "https://example.com/v1/audio/transcriptions"},
		{baseURL: "https://example.com/v1/", want: "https://example.com/v1/audio/transcriptions"},
	}

	for _, tc := range tests {
		extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
			BaseURL: tc.baseURL,
			APIKey:  "secret",
			Model:   "whisper-1",
		})
		if err != nil {
			t.Fatalf("NewOpenAIAudioTextExtractor(%q): %v", tc.baseURL, err)
		}
		if extractor.endpoint != tc.want {
			t.Fatalf("endpoint=%q, want %q", extractor.endpoint, tc.want)
		}
	}
}

func TestOpenAIAudioTextExtractorExtractAudioText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/audio/transcriptions" {
			t.Fatalf("path=%q, want /v1/audio/transcriptions", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer secret" {
			t.Fatalf("authorization=%q, want Bearer secret", auth)
		}
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Fatalf("parse content type: %v", err)
		}
		if mediaType != "multipart/form-data" {
			t.Fatalf("mediaType=%q, want multipart/form-data", mediaType)
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		defer func() { _ = r.Body.Close() }()

		var model, prompt, responseFormat, fileName, fileContentType, fileBody string
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("next part: %v", err)
			}
			data, err := io.ReadAll(part)
			if err != nil {
				t.Fatalf("read part: %v", err)
			}
			switch part.FormName() {
			case "model":
				model = string(data)
			case "prompt":
				prompt = string(data)
			case "response_format":
				responseFormat = string(data)
			case "file":
				fileName = part.FileName()
				fileContentType = part.Header.Get("Content-Type")
				fileBody = string(data)
			default:
				t.Fatalf("unexpected part name %q", part.FormName())
			}
		}
		if model != "whisper-1" {
			t.Fatalf("model=%q, want whisper-1", model)
		}
		if responseFormat != "json" {
			t.Fatalf("response_format=%q, want json", responseFormat)
		}
		if prompt != "transcribe in zh" {
			t.Fatalf("prompt=%q, want transcribe in zh", prompt)
		}
		if fileName != "clip.mp3" {
			t.Fatalf("fileName=%q, want clip.mp3", fileName)
		}
		if fileContentType != "audio/mpeg" {
			t.Fatalf("file content type=%q, want audio/mpeg", fileContentType)
		}
		if fileBody != "fake-mp3" {
			t.Fatalf("file body=%q, want fake-mp3", fileBody)
		}
		_, _ = w.Write([]byte(`{"text":"spoken transcript"}`))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "whisper-1",
		Prompt:  "transcribe in zh",
	})
	if err != nil {
		t.Fatal(err)
	}

	got, _, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path:        "/audio/clip.mp3",
		ContentType: "audio/mpeg",
		Data:        []byte("fake-mp3"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "spoken transcript" {
		t.Fatalf("text=%q, want spoken transcript", got)
	}
}

func TestOpenAIAudioTextExtractorVerboseJSONWhisperDuration(t *testing.T) {
	var gotResponseFormat string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaType != "multipart/form-data" {
			t.Fatalf("mediaType=%q", mediaType)
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("next part: %v", err)
			}
			data, _ := io.ReadAll(part)
			if part.FormName() == "response_format" {
				gotResponseFormat = string(data)
			}
		}
		_, _ = w.Write([]byte(`{"text":"whisper transcript","duration":42.5}`))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL:        server.URL,
		APIKey:         "secret",
		Model:          "whisper-1",
		ResponseFormat: "verbose_json",
	})
	if err != nil {
		t.Fatal(err)
	}
	text, usage, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path: "/clip.mp3", ContentType: "audio/mpeg", Data: []byte("x"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotResponseFormat != "verbose_json" {
		t.Fatalf("response_format=%q, want verbose_json", gotResponseFormat)
	}
	if text != "whisper transcript" {
		t.Fatalf("text=%q", text)
	}
	if usage.DurationSeconds != 42.5 {
		t.Fatalf("duration=%v, want 42.5", usage.DurationSeconds)
	}
}

func TestOpenAIAudioTextExtractorTokenUsageParsing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"text":"token transcript","usage":{"input_tokens":100,"output_tokens":50}}`))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "gpt-4o-transcribe",
	})
	if err != nil {
		t.Fatal(err)
	}
	text, usage, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path: "/clip.mp3", ContentType: "audio/mpeg", Data: []byte("x"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if text != "token transcript" {
		t.Fatalf("text=%q", text)
	}
	if usage.InputTokens != 100 {
		t.Fatalf("input_tokens=%d, want 100", usage.InputTokens)
	}
	if usage.OutputTokens != 50 {
		t.Fatalf("output_tokens=%d, want 50", usage.OutputTokens)
	}
}

func TestOpenAIAudioTextExtractorExtractAudioTextErrorMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":{"message":"upstream unavailable"}}`))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "whisper-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{}); err == nil || err.Error() != "audio transcription api status 502: upstream unavailable" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenAIAudioTextExtractorExtractAudioTextRawErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad gateway from proxy"))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "whisper-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{}); err == nil || err.Error() != "audio transcription api status 400: bad gateway from proxy" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenAIAudioTextExtractorExtractAudioTextEmptyText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"text":"   "}`))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "whisper-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{}); err == nil || err.Error() != "audio transcription api returned empty text" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenAIAudioTextExtractorFallsBackToGenericFilenameAndContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Fatalf("parse content type: %v", err)
		}
		if mediaType != "multipart/form-data" {
			t.Fatalf("mediaType=%q, want multipart/form-data", mediaType)
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		defer func() { _ = r.Body.Close() }()
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("next part: %v", err)
			}
			if part.FormName() != "file" {
				continue
			}
			if part.FileName() != "audio" {
				t.Fatalf("fileName=%q, want audio", part.FileName())
			}
			if got := part.Header.Get("Content-Type"); got != "application/octet-stream" {
				t.Fatalf("file content type=%q, want application/octet-stream", got)
			}
		}
		_, _ = w.Write([]byte(`{"text":"ok"}`))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "whisper-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path: " ",
		Data: []byte("x"),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenAIAudioTextExtractorOmitsBlankPrompt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Fatalf("parse content type: %v", err)
		}
		if mediaType != "multipart/form-data" {
			t.Fatalf("mediaType=%q, want multipart/form-data", mediaType)
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		defer func() { _ = r.Body.Close() }()

		var sawPrompt bool
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("next part: %v", err)
			}
			if part.FormName() == "prompt" {
				sawPrompt = true
			}
		}
		if sawPrompt {
			t.Fatal("did not expect prompt part for blank prompt")
		}
		_, _ = w.Write([]byte(`{"text":"ok"}`))
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "whisper-1",
		Prompt:  "   ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path: "/clip.mp3",
		Data: []byte("x"),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenAIAudioTextExtractorRejectsUnsafeMultipartFilename(t *testing.T) {
	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: "https://example.com",
		APIKey:  "secret",
		Model:   "whisper-1",
		Client:  &http.Client{},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = extractor.ExtractAudioText(context.Background(), AudioExtractRequest{
		Path:        `/music/a"b.mp3`,
		ContentType: "audio/mpeg",
		Data:        []byte("x"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteAudioMultipartFileRejectsCancelledContextIndirectly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("request should not be sent for canceled context")
	}))
	defer server.Close()

	extractor, err := NewOpenAIAudioTextExtractor(OpenAIAudioTextExtractorConfig{
		BaseURL: server.URL,
		APIKey:  "secret",
		Model:   "whisper-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := extractor.ExtractAudioText(ctx, AudioExtractRequest{}); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("unexpected error: %v", err)
	}
}
