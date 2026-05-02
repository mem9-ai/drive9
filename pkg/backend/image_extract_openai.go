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

const (
	defaultImageExtractPrompt = `请分析这张图片，输出用于 Drive9 文件搜索的结构化 JSON。只输出一个 JSON 对象，不要 Markdown，不要解释。字段固定为：
{
  "caption_zh": "中文一句话摘要，<=120字",
  "description_zh": "中文详细描述，覆盖主体、场景、动作、颜色、构图、风格、时间/季节/地点线索，<=1200字",
  "caption_en": "one concise English caption, <=30 words",
  "description_en": "English description covering subjects, scene, action, colors, composition, style, time/season/location clues, <=260 words",
  "ocr_text": ["图中可见文字；没有则空数组；最多100条，每条<=160字"],
  "tags_zh": ["中文搜索标签，5-30个，短词，不要泛词"],
  "tags_en": ["English search tags, 5-30, lowercase short phrases, no generic words like image/photo/picture/ocr/text"],
  "search_queries_zh": ["中文自然搜索短语，3-12个"],
  "search_queries_en": ["English natural search phrases, 3-12"]
}
要求：
- 中英文都要覆盖可搜索信息，但不要逐字翻译造成重复堆砌。
- 标签优先使用具体物体、场景、风格、颜色、季节、地点、动作、可见文字主题。
- 若不确定不要编造；没有 OCR 就使用 []。
- 整个 JSON 控制在 16000 字符以内。`

	// DefaultOpenAIImageExtractMaxTokens leaves enough completion budget for
	// structured bilingual extraction while the writeback path still enforces
	// Drive9's embedding input safety limits.
	DefaultOpenAIImageExtractMaxTokens = 4096
)

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
		cfg.MaxTokens = DefaultOpenAIImageExtractMaxTokens
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
