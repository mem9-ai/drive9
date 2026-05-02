package backend

import (
	"strings"
	"testing"
)

func TestPrepareImageExtractWritebackStructuredJSON(t *testing.T) {
	raw := `{
  "caption_zh": "秋季林荫道路",
  "description_zh": "一条蜿蜒柏油路穿过金黄色树林，阳光穿过树冠形成斑驳光影。",
  "caption_en": "Autumn tree-lined road",
  "description_en": "A winding asphalt road runs through tall trees with golden leaves and warm sunlight.",
  "ocr_text": [],
  "tags_zh": ["秋季", "林荫路", "树木", "金黄色", "暖色调", "照片"],
  "tags_en": ["Autumn", "tree-lined road", "golden leaves", "warm light", "photo", "ocr"],
  "search_queries_zh": ["秋天金黄色树林道路", "暖色调林荫路风景"],
  "search_queries_en": ["autumn golden tree road", "warm tree-lined road landscape"]
}`

	writeback := prepareImageExtractWriteback(raw, 32<<10)
	if writeback.text == "" {
		t.Fatal("text is empty")
	}
	if strings.Contains(writeback.text, "caption_zh") || strings.Contains(writeback.text, "{") {
		t.Fatalf("text should be canonical text, got %q", writeback.text)
	}
	if !strings.Contains(writeback.text, "秋季林荫道路\n中文描述") {
		t.Fatalf("structured text should preserve line breaks, got %q", writeback.text)
	}
	for _, want := range []string{
		"中文摘要：秋季林荫道路",
		"中文描述：一条蜿蜒柏油路穿过金黄色树林",
		"英文摘要：Autumn tree-lined road",
		"英文标签：autumn, tree-lined road, golden leaves, warm light",
		"中文搜索短语：秋天金黄色树林道路",
	} {
		if !strings.Contains(writeback.text, want) {
			t.Fatalf("text %q does not contain %q", writeback.text, want)
		}
	}
	if writeback.tags[imageExtractTagPrefix+"schema"] != imageExtractStructuredSchema {
		t.Fatalf("schema tag=%q, want %q", writeback.tags[imageExtractTagPrefix+"schema"], imageExtractStructuredSchema)
	}
	if writeback.tags[imageExtractTagPrefix+"source"] != imageExtractSource {
		t.Fatalf("source tag=%q, want %q", writeback.tags[imageExtractTagPrefix+"source"], imageExtractSource)
	}
	if got := writeback.tags[imageExtractTagPrefix+"tag.en.0"]; got != "autumn" {
		t.Fatalf("tag.en.0=%q, want autumn", got)
	}
	if got := writeback.tags[imageExtractTagPrefix+"tag.en.3"]; got != "warm light" {
		t.Fatalf("tag.en.3=%q, want warm light", got)
	}
	if _, ok := writeback.tags[imageExtractTagPrefix+"tag.en.4"]; ok {
		t.Fatalf("generic English tags should be filtered, got %+v", writeback.tags)
	}
	if _, ok := writeback.tags[imageExtractTagPrefix+"tag.zh.5"]; ok {
		t.Fatalf("generic Chinese tags should be filtered, got %+v", writeback.tags)
	}
}

func TestOpenAIImageTextExtractorStructuredDefaults(t *testing.T) {
	extractor, err := NewOpenAIImageTextExtractor(OpenAIImageTextExtractorConfig{
		BaseURL: "https://example.com/v1",
		APIKey:  "secret",
		Model:   "vision-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	if extractor.maxTokens != DefaultOpenAIImageExtractMaxTokens {
		t.Fatalf("maxTokens=%d, want %d", extractor.maxTokens, DefaultOpenAIImageExtractMaxTokens)
	}
	if !strings.Contains(extractor.prompt, `"caption_zh"`) || !strings.Contains(extractor.prompt, `"tags_en"`) {
		t.Fatalf("default prompt is not structured JSON prompt: %q", extractor.prompt)
	}
}

func TestPrepareImageExtractWritebackParsesMarkdownFencedJSON(t *testing.T) {
	raw := "```json\n" + `{
  "caption_zh": "白板会议",
  "description_zh": "办公室里有人在白板前讨论。",
  "caption_en": "Whiteboard meeting",
  "description_en": "People discuss ideas in front of a whiteboard.",
  "ocr_text": "Q2 Roadmap",
  "tags_zh": ["白板", "会议"],
  "tags_en": ["whiteboard", "meeting"],
  "search_queries_zh": [],
  "search_queries_en": []
}` + "\n```"

	writeback := prepareImageExtractWriteback(raw, 32<<10)
	if !strings.Contains(writeback.text, "图中文字：Q2 Roadmap") {
		t.Fatalf("expected OCR text in canonical output, got %q", writeback.text)
	}
	if got := writeback.tags[imageExtractTagPrefix+"tag.en.1"]; got != "meeting" {
		t.Fatalf("tag.en.1=%q, want meeting", got)
	}
}

func TestPrepareImageExtractWritebackFallbackRawText(t *testing.T) {
	raw := "cat on sofa screenshot invoice"
	writeback := prepareImageExtractWriteback(raw, 32<<10)
	if writeback.text != raw {
		t.Fatalf("text=%q, want %q", writeback.text, raw)
	}
	if len(writeback.tags) != 0 {
		t.Fatalf("fallback tags=%+v, want empty", writeback.tags)
	}
}

func TestPrepareImageExtractWritebackAppliesByteLimit(t *testing.T) {
	raw := `{
  "caption_zh": "很长的描述",
  "description_zh": "` + strings.Repeat("秋", 200) + `",
  "caption_en": "",
  "description_en": "",
  "ocr_text": [],
  "tags_zh": ["秋季"],
  "tags_en": ["autumn"],
  "search_queries_zh": [],
  "search_queries_en": []
}`

	writeback := prepareImageExtractWriteback(raw, 64)
	if len(writeback.text) > 64 {
		t.Fatalf("text length=%d, want <=64", len(writeback.text))
	}
	if writeback.tags[imageExtractTagPrefix+"tag.zh.0"] != "秋季" {
		t.Fatalf("tags should still be generated after text truncation, got %+v", writeback.tags)
	}
}

func TestTruncateByEstimatedTokens(t *testing.T) {
	text := strings.Repeat("秋", 9000)
	got := truncateByEstimatedTokens(text, 8000)
	if estimateImageExtractTokens(got) > 8000 {
		t.Fatalf("estimated tokens=%d, want <=8000", estimateImageExtractTokens(got))
	}
	if len([]rune(got)) != 8000 {
		t.Fatalf("rune count=%d, want 8000", len([]rune(got)))
	}
}
