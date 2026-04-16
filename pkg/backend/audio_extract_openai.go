package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"
	"strings"
	"time"
)

// OpenAIAudioTextExtractorConfig configures an OpenAI-compatible audio
// transcription endpoint.
type OpenAIAudioTextExtractorConfig struct {
	BaseURL        string
	APIKey         string
	Model          string
	Prompt         string
	ResponseFormat string // "verbose_json" for whisper-1, "json" for gpt-4o-transcribe
	Timeout        time.Duration
	Client         *http.Client
}

// OpenAIAudioTextExtractor calls an OpenAI-compatible /v1/audio/transcriptions
// API and returns plain transcript text.
type OpenAIAudioTextExtractor struct {
	endpoint       string
	apiKey         string
	model          string
	prompt         string
	responseFormat string
	client         *http.Client
}

// NewOpenAIAudioTextExtractor builds an audio extractor backed by an
// OpenAI-compatible transcription endpoint.
func NewOpenAIAudioTextExtractor(cfg OpenAIAudioTextExtractorConfig) (*OpenAIAudioTextExtractor, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		return nil, fmt.Errorf("audio extractor base url is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("audio extractor api key is required")
	}
	if cfg.Model == "" {
		return nil, fmt.Errorf("audio extractor model is required")
	}
	endpoint := base + "/v1/audio/transcriptions"
	if strings.HasSuffix(base, "/v1") {
		endpoint = base + "/audio/transcriptions"
	}
	responseFormat := cfg.ResponseFormat
	if responseFormat == "" {
		if strings.HasPrefix(cfg.Model, "whisper") {
			responseFormat = "verbose_json"
		} else {
			responseFormat = "json"
		}
	}
	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = defaultAudioExtractTimeout
		}
		client = &http.Client{Timeout: timeout}
	}
	return &OpenAIAudioTextExtractor{
		endpoint:       endpoint,
		apiKey:         cfg.APIKey,
		model:          cfg.Model,
		prompt:         cfg.Prompt,
		responseFormat: responseFormat,
		client:         client,
	}, nil
}

// ExtractAudioText implements AudioTextExtractor.
func (e *OpenAIAudioTextExtractor) ExtractAudioText(ctx context.Context, req AudioExtractRequest) (string, AudioExtractUsage, error) {
	select {
	case <-ctx.Done():
		return "", AudioExtractUsage{}, ctx.Err()
	default:
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", e.model); err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("write audio transcription model field for %q: %w", req.Path, err)
	}
	if err := writer.WriteField("response_format", e.responseFormat); err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("write audio transcription response_format field for %q: %w", req.Path, err)
	}
	if strings.TrimSpace(e.prompt) != "" {
		if err := writer.WriteField("prompt", e.prompt); err != nil {
			return "", AudioExtractUsage{}, fmt.Errorf("write audio transcription prompt field for %q: %w", req.Path, err)
		}
	}
	if err := writeAudioMultipartFile(writer, req); err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("write audio transcription file part for %q: %w", req.Path, err)
	}
	if err := writer.Close(); err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("finalize audio transcription multipart body for %q: %w", req.Path, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body.Bytes()))
	if err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("create audio transcription request for %q: %w", req.Path, err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+e.apiKey)
	httpReq.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("send audio transcription request for %q: %w", req.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("read audio transcription response for %q: %w", req.Path, err)
	}
	var parsed struct {
		Text     string  `json:"text"`
		Duration float64 `json:"duration"` // whisper-1 verbose_json
		Usage    *struct {
			InputTokens  int `json:"input_tokens"`  // gpt-4o-transcribe
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		if resp.StatusCode >= 300 {
			return "", AudioExtractUsage{}, fmt.Errorf("audio transcription api status %d: %s", resp.StatusCode, truncateString(string(raw), 256))
		}
		return "", AudioExtractUsage{}, fmt.Errorf("decode audio transcription response: %w", err)
	}
	if resp.StatusCode >= 300 {
		if parsed.Error != nil && parsed.Error.Message != "" {
			return "", AudioExtractUsage{}, fmt.Errorf("audio transcription api status %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return "", AudioExtractUsage{}, fmt.Errorf("audio transcription api status %d", resp.StatusCode)
	}
	text := strings.TrimSpace(parsed.Text)
	if text == "" {
		return "", AudioExtractUsage{}, fmt.Errorf("audio transcription api returned empty text")
	}
	var usage AudioExtractUsage
	if parsed.Usage != nil {
		usage.InputTokens = parsed.Usage.InputTokens
		usage.OutputTokens = parsed.Usage.OutputTokens
	}
	if parsed.Duration > 0 {
		usage.DurationSeconds = parsed.Duration
	}
	return text, usage, nil
}

func writeAudioMultipartFile(writer *multipart.Writer, req AudioExtractRequest) error {
	filename := strings.TrimSpace(path.Base(req.Path))
	if filename == "" || filename == "." || filename == "/" {
		filename = "audio"
	}
	// Reject characters that break a quoted filename= parameter or inject header lines.
	// TODO: Encode arbitrary basenames using RFC 5987 filename* (and/or safer multipart headers).
	if strings.ContainsAny(filename, "\"\r\n") {
		return fmt.Errorf("basename %q contains forbidden characters (\", CR, or LF)", filename)
	}
	contentType := stripMIMEParams(req.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Some ASR providers inspect the part-level Content-Type rather than
	// inferring solely from bytes, so keep the request MIME when available.
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, filename))
	header.Set("Content-Type", contentType)
	part, err := writer.CreatePart(header)
	if err != nil {
		return fmt.Errorf("create multipart file part: %w", err)
	}
	if _, err := part.Write(req.Data); err != nil {
		return fmt.Errorf("write multipart file bytes: %w", err)
	}
	return nil
}
