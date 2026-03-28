package captioner

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// VisionConfig holds configuration for the Vision LLM captioner.
type VisionConfig struct {
	APIKey   string // required
	Model    string // e.g. "gpt-4o", "claude-sonnet-4-20250514"
	Endpoint string // base URL, e.g. "https://api.openai.com/v1"
	MaxBytes int64  // max image size in bytes (0 = no limit)
}

// NewVisionFromEnv creates a VisionCaptioner from environment variables.
// Returns nil if DAT9_CAPTIONER_API_KEY is not set.
func NewVisionFromEnv() *VisionCaptioner {
	apiKey := os.Getenv("DAT9_CAPTIONER_API_KEY")
	if apiKey == "" {
		return nil
	}
	model := os.Getenv("DAT9_CAPTIONER_MODEL")
	if model == "" {
		model = "gpt-4o"
	}
	endpoint := os.Getenv("DAT9_CAPTIONER_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1"
	}
	var maxBytes int64
	if v := os.Getenv("DAT9_IMAGE_CAPTION_MAX_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxBytes = n
		}
	}
	return NewVision(VisionConfig{
		APIKey:   apiKey,
		Model:    model,
		Endpoint: endpoint,
		MaxBytes: maxBytes,
	})
}

// VisionCaptioner calls an OpenAI-compatible chat/completions Vision API to
// caption images. It does NOT support Claude's native Messages API format.
type VisionCaptioner struct {
	cfg    VisionConfig
	client *http.Client
}

// NewVision creates a VisionCaptioner with the given config.
func NewVision(cfg VisionConfig) *VisionCaptioner {
	return &VisionCaptioner{
		cfg: cfg,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

const captionPrompt = "Describe this image concisely for search indexing. " +
	"Include key objects, text visible in the image, colors, and any notable details. " +
	"Keep the description under 500 words."

// Caption sends the image to a Vision LLM and returns a text description.
func (v *VisionCaptioner) Caption(ctx context.Context, imageBytes []byte, contentType string) (string, error) {
	if v.cfg.MaxBytes > 0 && int64(len(imageBytes)) > v.cfg.MaxBytes {
		return "", fmt.Errorf("%w: size %d exceeds limit %d", ErrImageTooLarge, len(imageBytes), v.cfg.MaxBytes)
	}
	if !strings.HasPrefix(contentType, "image/") {
		return "", fmt.Errorf("%w: unsupported content type %q", ErrInvalidOutput, contentType)
	}

	dataURI := fmt.Sprintf("data:%s;base64,%s", contentType, base64.StdEncoding.EncodeToString(imageBytes))

	body := chatRequest{
		Model: v.cfg.Model,
		Messages: []message{{
			Role: "user",
			Content: []contentPart{
				{Type: "text", Text: captionPrompt},
				{Type: "image_url", ImageURL: &imageURL{URL: dataURI}},
			},
		}},
		MaxTokens: 800,
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	endpoint := strings.TrimRight(v.cfg.Endpoint, "/") + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+v.cfg.APIKey)

	resp, err := v.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("vision API call: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MB max
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 500 {
		return "", fmt.Errorf("vision API error: HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200))
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("%w: HTTP %d: %s", ErrInvalidOutput, resp.StatusCode, truncate(string(respBody), 200))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(respBody, &chatResp); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}
	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("%w: no choices in response", ErrInvalidOutput)
	}

	return validate(chatResp.Choices[0].Message.Text)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// OpenAI-compatible chat completion request/response types.

type chatRequest struct {
	Model     string    `json:"model"`
	Messages  []message `json:"messages"`
	MaxTokens int       `json:"max_tokens"`
}

type message struct {
	Role    string        `json:"role"`
	Content []contentPart `json:"content"`
}

type contentPart struct {
	Type     string    `json:"type"`
	Text     string    `json:"text,omitempty"`
	ImageURL *imageURL `json:"image_url,omitempty"`
}

type imageURL struct {
	URL string `json:"url"`
}

type chatResponse struct {
	Choices []choice `json:"choices"`
}

type choice struct {
	Message responseMessage `json:"message"`
}

type responseMessage struct {
	Text string `json:"content"`
}
