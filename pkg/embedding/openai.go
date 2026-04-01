package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenAIClientConfig configures an OpenAI-compatible embeddings endpoint.
type OpenAIClientConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	Dimensions int
	Timeout    time.Duration
	Client     *http.Client
}

// OpenAIClient calls an OpenAI-compatible /v1/embeddings API.
type OpenAIClient struct {
	endpoint   string
	apiKey     string
	model      string
	dimensions int
	client     *http.Client
}

// NewOpenAIClient builds a Client backed by an OpenAI-compatible embeddings API.
func NewOpenAIClient(cfg OpenAIClientConfig) (*OpenAIClient, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		return nil, fmt.Errorf("embedding base url is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("embedding api key is required")
	}
	if cfg.Model == "" {
		return nil, fmt.Errorf("embedding model is required")
	}
	endpoint := base + "/v1/embeddings"
	if strings.HasSuffix(base, "/v1") {
		endpoint = base + "/embeddings"
	}
	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 20 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	return &OpenAIClient{
		endpoint:   endpoint,
		apiKey:     cfg.APIKey,
		model:      cfg.Model,
		dimensions: cfg.Dimensions,
		client:     client,
	}, nil
}

// EmbedText implements Client.
func (c *OpenAIClient) EmbedText(ctx context.Context, text string) ([]float32, error) {
	payload := map[string]any{
		"model": c.model,
		"input": text,
	}
	if c.dimensions > 0 {
		payload["dimensions"] = c.dimensions
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("embedding api status %d: %s", resp.StatusCode, truncateString(string(raw), 256))
		}
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}
	if resp.StatusCode >= 300 {
		if parsed.Error != nil && parsed.Error.Message != "" {
			return nil, fmt.Errorf("embedding api status %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return nil, fmt.Errorf("embedding api status %d", resp.StatusCode)
	}
	if len(parsed.Data) == 0 || len(parsed.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("embedding api returned no embedding")
	}
	return append([]float32(nil), parsed.Data[0].Embedding...), nil
}

func truncateString(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
}
