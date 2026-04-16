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

const defaultImageExtractPrompt = "用中文描述这张图片，用于文件搜索。包括：主要物体、场景描述、图中可见文字（OCR）、简洁标签。最后一行用英文写5-10个关键词标签（English tags），用逗号分隔。"

// OpenAIImageTextExtractorConfig configures an OpenAI-compatible vision endpoint.
// This works with providers that expose the /v1/chat/completions API surface.
type OpenAIImageTextExtractorConfig struct {
	BaseURL   string
	APIKey    string
	Model     string
	Prompt    string
	MaxTokens int
	Timeout   time.Duration
	Client    *http.Client
}

type OpenAIImageTextExtractor struct {
	endpoint  string
	apiKey    string
	model     string
	prompt    string
	maxTokens int
	client    *http.Client
}

func NewOpenAIImageTextExtractor(cfg OpenAIImageTextExtractorConfig) (*OpenAIImageTextExtractor, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		return nil, fmt.Errorf("openai extractor base url is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("openai extractor api key is required")
	}
	if cfg.Model == "" {
		return nil, fmt.Errorf("openai extractor model is required")
	}
	var endpoint string
	if strings.HasSuffix(base, "/v1") {
		endpoint = base + "/chat/completions"
	} else {
		endpoint = base + "/v1/chat/completions"
	}
	if cfg.Prompt == "" {
		cfg.Prompt = defaultImageExtractPrompt
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = 256
	}
	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	return &OpenAIImageTextExtractor{
		endpoint:  endpoint,
		apiKey:    cfg.APIKey,
		model:     cfg.Model,
		prompt:    cfg.Prompt,
		maxTokens: cfg.MaxTokens,
		client:    client,
	}, nil
}

func (e *OpenAIImageTextExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, ImageExtractUsage, error) {
	contentType := req.ContentType
	if contentType == "" {
		contentType = "image/png"
	}
	imageURL := "data:" + contentType + ";base64," + base64.StdEncoding.EncodeToString(req.Data)
	payload := map[string]any{
		"model": e.model,
		"messages": []map[string]any{
			{
				"role": "user",
				"content": []map[string]any{
					{"type": "text", "text": e.prompt},
					{"type": "image_url", "image_url": map[string]any{"url": imageURL}},
				},
			},
		},
		"temperature": 0,
		"max_tokens":  e.maxTokens,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", ImageExtractUsage{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", ImageExtractUsage{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+e.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return "", ImageExtractUsage{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", ImageExtractUsage{}, err
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
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		if resp.StatusCode >= 300 {
			return "", ImageExtractUsage{}, fmt.Errorf("vision api status %d: %s", resp.StatusCode, truncateString(string(raw), 256))
		}
		return "", ImageExtractUsage{}, fmt.Errorf("decode vision response: %w", err)
	}
	if resp.StatusCode >= 300 {
		if parsed.Error != nil && parsed.Error.Message != "" {
			return "", ImageExtractUsage{}, fmt.Errorf("vision api status %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return "", ImageExtractUsage{}, fmt.Errorf("vision api status %d", resp.StatusCode)
	}
	if len(parsed.Choices) == 0 {
		return "", ImageExtractUsage{}, fmt.Errorf("vision api returned no choices")
	}
	text := extractOpenAIContentText(parsed.Choices[0].Message.Content)
	if strings.TrimSpace(text) == "" {
		return "", ImageExtractUsage{}, fmt.Errorf("vision api returned empty text")
	}
	var usage ImageExtractUsage
	if parsed.Usage != nil {
		usage.PromptTokens = parsed.Usage.PromptTokens
		usage.CompletionTokens = parsed.Usage.CompletionTokens
	}
	return text, usage, nil
}

func extractOpenAIContentText(content any) string {
	switch v := content.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, it := range v {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			txt, _ := m["text"].(string)
			if strings.TrimSpace(txt) != "" {
				parts = append(parts, strings.TrimSpace(txt))
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n"))
	default:
		return ""
	}
}

func truncateString(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
}
