package backend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// QwenASRAudioTextExtractorConfig configures Alibaba Cloud Model Studio
// Qwen-ASR through DashScope's OpenAI-compatible chat/completions endpoint.
type QwenASRAudioTextExtractorConfig struct {
	BaseURL string
	APIKey  string
	Model   string
	Prompt  string
	Timeout time.Duration
	Client  *http.Client
}

// QwenASRAudioTextExtractor calls DashScope compatible-mode /chat/completions
// with an input_audio message and returns the assistant transcript text.
type QwenASRAudioTextExtractor struct {
	endpoint string
	apiKey   string
	model    string
	prompt   string
	client   *http.Client
}

// NewQwenASRAudioTextExtractor builds an extractor for Qwen3-ASR-Flash.
func NewQwenASRAudioTextExtractor(cfg QwenASRAudioTextExtractorConfig) (*QwenASRAudioTextExtractor, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		return nil, fmt.Errorf("qwen asr extractor base url is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("qwen asr extractor api key is required")
	}
	if cfg.Model == "" {
		return nil, fmt.Errorf("qwen asr extractor model is required")
	}
	endpoint := base + "/v1/chat/completions"
	if strings.HasSuffix(base, "/v1") {
		endpoint = base + "/chat/completions"
	}
	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = defaultAudioExtractTimeout
		}
		client = &http.Client{Timeout: timeout}
	}
	return &QwenASRAudioTextExtractor{
		endpoint: endpoint,
		apiKey:   cfg.APIKey,
		model:    cfg.Model,
		prompt:   cfg.Prompt,
		client:   client,
	}, nil
}

// ExtractAudioText implements AudioTextExtractor.
func (e *QwenASRAudioTextExtractor) ExtractAudioText(ctx context.Context, req AudioExtractRequest) (string, AudioExtractUsage, error) {
	select {
	case <-ctx.Done():
		return "", AudioExtractUsage{}, ctx.Err()
	default:
	}

	contentType := stripMIMEParams(req.ContentType)
	if contentType == "" {
		contentType = "audio/mpeg"
	}
	audioURL := "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(req.Data)
	messages := make([]map[string]any, 0, 2)
	if strings.TrimSpace(e.prompt) != "" {
		messages = append(messages, map[string]any{
			"role": "system",
			"content": []map[string]any{
				{"text": e.prompt},
			},
		})
	}
	messages = append(messages, map[string]any{
		"role": "user",
		"content": []map[string]any{
			{
				"type": "input_audio",
				"input_audio": map[string]any{
					"data": audioURL,
				},
			},
		},
	})
	payload := map[string]any{
		"model":    e.model,
		"messages": messages,
		"stream":   false,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("encode qwen asr request for %q: %w", req.Path, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("create qwen asr request for %q: %w", req.Path, err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+e.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("send qwen asr request for %q: %w", req.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", AudioExtractUsage{}, fmt.Errorf("read qwen asr response for %q: %w", req.Path, err)
	}
	var parsed struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Choices []struct {
			Message struct {
				Content any `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int     `json:"prompt_tokens"`
			CompletionTokens int     `json:"completion_tokens"`
			Seconds          float64 `json:"seconds"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		if resp.StatusCode >= 300 {
			return "", AudioExtractUsage{}, fmt.Errorf("qwen asr api status %d: %s", resp.StatusCode, truncateString(string(raw), 256))
		}
		return "", AudioExtractUsage{}, fmt.Errorf("decode qwen asr response: %w", err)
	}
	if resp.StatusCode >= 300 {
		if parsed.Error != nil && parsed.Error.Message != "" {
			return "", AudioExtractUsage{}, fmt.Errorf("qwen asr api status %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return "", AudioExtractUsage{}, fmt.Errorf("qwen asr api status %d", resp.StatusCode)
	}
	if len(parsed.Choices) == 0 {
		return "", AudioExtractUsage{}, fmt.Errorf("qwen asr api returned no choices")
	}
	text := extractOpenAIContentText(parsed.Choices[0].Message.Content)
	if strings.TrimSpace(text) == "" {
		return "", AudioExtractUsage{}, fmt.Errorf("qwen asr api returned empty text")
	}
	var usage AudioExtractUsage
	if parsed.Usage != nil {
		usage.InputTokens = parsed.Usage.PromptTokens
		usage.OutputTokens = parsed.Usage.CompletionTokens
		usage.DurationSeconds = parsed.Usage.Seconds
	}
	return text, usage, nil
}
