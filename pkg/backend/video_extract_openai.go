package backend

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultVideoExtractPrompt = `你正在看一段视频的截图帧。请综合所有帧，描述视频的视觉内容，输出用于 Drive9 文件搜索的结构化 JSON。只输出一个 JSON 对象，不要 Markdown，不要解释。字段固定为：
{
  "caption_zh": "中文一句话摘要，<=120字",
  "description_zh": "中文详细描述，覆盖场景、人物、动作、物体、颜色、风格、地点线索，<=1200字",
  "caption_en": "one concise English caption, <=30 words",
  "description_en": "English description covering scenes, people, actions, objects, colors, style, location clues, <=260 words",
  "tags_zh": ["中文搜索标签，5-30个，短词"],
  "tags_en": ["English search tags, 5-30, lowercase short phrases"],
  "search_queries_zh": ["中文自然搜索短语，3-12个"],
  "search_queries_en": ["English natural search phrases, 3-12"]
}
要求：
- 综合所有帧描述整段视频，不要逐帧列举。
- 中英文都要覆盖可搜索信息。
- 标签优先使用具体物体、场景、动作、风格。
- 若不确定不要编造。
- 整个 JSON 控制在 16000 字符以内。`

	// DefaultOpenAIVideoExtractMaxTokens leaves enough completion budget for
	// structured bilingual video extraction.
	DefaultOpenAIVideoExtractMaxTokens = 4096

	// defaultVideoFrameInterval is the interval in seconds between sampled frames.
	defaultVideoFrameInterval = 5

	// defaultVideoMaxFrames caps the number of frames sent to the Vision model.
	defaultVideoMaxFrames = 10
)

// OpenAIVideoTextExtractorConfig configures an OpenAI-compatible vision endpoint
// for video frame extraction.
type OpenAIVideoTextExtractorConfig struct {
	BaseURL        string
	APIKey         string
	Model          string
	Prompt         string
	MaxTokens      int
	Timeout        time.Duration
	Client         *http.Client
	FrameInterval  int // seconds between sampled frames
	MaxFrames      int // max frames to send to vision model
	FFmpegPath     string
}

// OpenAIVideoTextExtractor extracts searchable text from video bytes by:
// 1. Writing video to a temp file
// 2. Using FFmpeg to extract frames at regular intervals
// 3. Sending frames to a Vision model for description
type OpenAIVideoTextExtractor struct {
	endpoint      string
	apiKey        string
	model         string
	prompt        string
	maxTokens     int
	client        *http.Client
	frameInterval int
	maxFrames     int
	ffmpegPath    string
}

func NewOpenAIVideoTextExtractor(cfg OpenAIVideoTextExtractorConfig) (*OpenAIVideoTextExtractor, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		return nil, fmt.Errorf("openai video extractor base url is required")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("openai video extractor api key is required")
	}
	if cfg.Model == "" {
		return nil, fmt.Errorf("openai video extractor model is required")
	}
	var endpoint string
	if strings.HasSuffix(base, "/v1") {
		endpoint = base + "/chat/completions"
	} else {
		endpoint = base + "/v1/chat/completions"
	}
	if cfg.Prompt == "" {
		cfg.Prompt = defaultVideoExtractPrompt
	}
	if cfg.MaxTokens <= 0 {
		cfg.MaxTokens = DefaultOpenAIVideoExtractMaxTokens
	}
	if cfg.FrameInterval <= 0 {
		cfg.FrameInterval = defaultVideoFrameInterval
	}
	if cfg.MaxFrames <= 0 {
		cfg.MaxFrames = defaultVideoMaxFrames
	}
	ffmpegPath := cfg.FFmpegPath
	if ffmpegPath == "" {
		ffmpegPath = "ffmpeg"
	}
	client := cfg.Client
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 120 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	return &OpenAIVideoTextExtractor{
		endpoint:      endpoint,
		apiKey:        cfg.APIKey,
		model:         cfg.Model,
		prompt:        cfg.Prompt,
		maxTokens:     cfg.MaxTokens,
		client:        client,
		frameInterval: cfg.FrameInterval,
		maxFrames:     cfg.MaxFrames,
		ffmpegPath:    ffmpegPath,
	}, nil
}

func (e *OpenAIVideoTextExtractor) ExtractVideoText(ctx context.Context, req VideoExtractRequest) (string, VideoExtractUsage, error) {
	frames, err := e.extractFrames(ctx, req.Data, req.ContentType)
	if err != nil {
		return "", VideoExtractUsage{}, fmt.Errorf("extract frames: %w", err)
	}
	if len(frames) == 0 {
		return "", VideoExtractUsage{}, fmt.Errorf("no frames extracted from video")
	}

	text, usage, err := e.describeFrames(ctx, frames)
	if err != nil {
		return "", VideoExtractUsage{}, err
	}
	usage.FramesExtracted = len(frames)
	return text, usage, nil
}

// extractFrames writes video bytes to a temp file and uses FFmpeg to extract
// JPEG frames at the configured interval.
func (e *OpenAIVideoTextExtractor) extractFrames(ctx context.Context, data []byte, contentType string) ([][]byte, error) {
	tmpDir, err := os.MkdirTemp("", "drive9-video-extract-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	ext := videoMIMEToExt(contentType)
	videoPath := filepath.Join(tmpDir, "input"+ext)
	if err := os.WriteFile(videoPath, data, 0o600); err != nil {
		return nil, fmt.Errorf("write temp video: %w", err)
	}

	outputPattern := filepath.Join(tmpDir, "frame_%04d.jpg")
	// Use fps filter to sample at 1/interval fps, then limit total frames via -frames:v
	fpsFilter := fmt.Sprintf("fps=1/%d", e.frameInterval)
	args := []string{
		"-i", videoPath,
		"-vf", fpsFilter,
		"-frames:v", fmt.Sprintf("%d", e.maxFrames),
		"-q:v", "2", // JPEG quality
		"-f", "image2",
		outputPattern,
	}

	cmd := exec.CommandContext(ctx, e.ffmpegPath, args...)
	cmd.Stderr = io.Discard
	cmd.Stdout = io.Discard
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg: %w", err)
	}

	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, fmt.Errorf("read frame dir: %w", err)
	}
	var framePaths []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "frame_") && strings.HasSuffix(entry.Name(), ".jpg") {
			framePaths = append(framePaths, filepath.Join(tmpDir, entry.Name()))
		}
	}
	sort.Strings(framePaths)

	var frames [][]byte
	for _, fp := range framePaths {
		frameData, err := os.ReadFile(fp)
		if err != nil {
			continue
		}
		if len(frameData) > 0 {
			frames = append(frames, frameData)
		}
	}
	return frames, nil
}

// describeFrames sends extracted frames to the Vision model and returns
// the combined description text.
func (e *OpenAIVideoTextExtractor) describeFrames(ctx context.Context, frames [][]byte) (string, VideoExtractUsage, error) {
	content := []map[string]any{
		{"type": "text", "text": e.prompt},
	}
	for _, frame := range frames {
		imageURL := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(frame)
		content = append(content, map[string]any{
			"type":      "image_url",
			"image_url": map[string]any{"url": imageURL},
		})
	}

	payload := map[string]any{
		"model": e.model,
		"messages": []map[string]any{
			{
				"role":    "user",
				"content": content,
			},
		},
		"temperature": 0,
		"max_tokens":  e.maxTokens,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", VideoExtractUsage{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", VideoExtractUsage{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+e.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return "", VideoExtractUsage{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", VideoExtractUsage{}, err
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
			return "", VideoExtractUsage{}, fmt.Errorf("vision api status %d: %s", resp.StatusCode, truncateString(string(raw), 256))
		}
		return "", VideoExtractUsage{}, fmt.Errorf("decode vision response: %w", err)
	}
	if resp.StatusCode >= 300 {
		if parsed.Error != nil && parsed.Error.Message != "" {
			return "", VideoExtractUsage{}, fmt.Errorf("vision api status %d: %s", resp.StatusCode, parsed.Error.Message)
		}
		return "", VideoExtractUsage{}, fmt.Errorf("vision api status %d", resp.StatusCode)
	}
	if len(parsed.Choices) == 0 {
		return "", VideoExtractUsage{}, fmt.Errorf("vision api returned no choices")
	}
	text := extractOpenAIContentText(parsed.Choices[0].Message.Content)
	if strings.TrimSpace(text) == "" {
		return "", VideoExtractUsage{}, fmt.Errorf("vision api returned empty text")
	}
	var usage VideoExtractUsage
	if parsed.Usage != nil {
		usage.PromptTokens = parsed.Usage.PromptTokens
		usage.CompletionTokens = parsed.Usage.CompletionTokens
	}
	return text, usage, nil
}

func videoMIMEToExt(mime string) string {
	switch stripMIMEParams(mime) {
	case "video/mp4":
		return ".mp4"
	case "video/quicktime":
		return ".mov"
	case "video/x-msvideo":
		return ".avi"
	case "video/webm":
		return ".webm"
	case "video/x-matroska":
		return ".mkv"
	default:
		return ".mp4"
	}
}
