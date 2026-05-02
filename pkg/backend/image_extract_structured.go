package backend

import (
	"encoding/json"
	"strconv"
	"strings"
	"unicode"
)

const (
	imageExtractTagPrefix        = "drive9.image."
	imageExtractStructuredSchema = "structured_v1"
	imageExtractSource           = "image_extract"

	maxStructuredImageExtractEstimatedTokens = 8000
	maxImageExtractCaptionZHRuneCount        = 120
	maxImageExtractDescriptionZHRuneCount    = 1200
	maxImageExtractCaptionENRuneCount        = 240
	maxImageExtractDescriptionENRuneCount    = 1800
	maxImageExtractListItemRuneCount         = 160
	maxImageExtractTagRuneCount              = 64
	maxImageExtractOCRItems                  = 100
	maxImageExtractTags                      = 30
	maxImageExtractSearchQueries             = 12
)

type imageExtractWriteback struct {
	text string
	tags map[string]string
}

type structuredImageExtractResponse struct {
	CaptionZH       string                 `json:"caption_zh"`
	DescriptionZH   string                 `json:"description_zh"`
	CaptionEN       string                 `json:"caption_en"`
	DescriptionEN   string                 `json:"description_en"`
	OCRText         imageExtractStringList `json:"ocr_text"`
	TagsZH          imageExtractStringList `json:"tags_zh"`
	TagsEN          imageExtractStringList `json:"tags_en"`
	SearchQueriesZH imageExtractStringList `json:"search_queries_zh"`
	SearchQueriesEN imageExtractStringList `json:"search_queries_en"`
}

type normalizedStructuredImageExtract struct {
	captionZH       string
	descriptionZH   string
	captionEN       string
	descriptionEN   string
	ocrText         []string
	tagsZH          []string
	tagsEN          []string
	searchQueriesZH []string
	searchQueriesEN []string
}

type imageExtractStringList []string

func (l *imageExtractStringList) UnmarshalJSON(data []byte) error {
	if strings.EqualFold(strings.TrimSpace(string(data)), "null") {
		*l = nil
		return nil
	}
	var values []string
	if err := json.Unmarshal(data, &values); err == nil {
		*l = values
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err == nil {
		if strings.TrimSpace(value) == "" {
			*l = nil
		} else {
			*l = []string{value}
		}
		return nil
	}
	return nil
}

func prepareImageExtractWriteback(raw string, maxBytes int) imageExtractWriteback {
	meta, ok := parseStructuredImageExtract(raw)
	if !ok {
		return imageExtractWriteback{
			text: sanitizeExtractedText(raw, maxBytes),
			tags: map[string]string{},
		}
	}

	text := buildStructuredImageExtractText(meta)
	text = truncateByEstimatedTokens(text, maxStructuredImageExtractEstimatedTokens)
	text = sanitizeStructuredImageExtractText(text, maxBytes)
	return imageExtractWriteback{
		text: text,
		tags: buildStructuredImageExtractTags(meta),
	}
}

func parseStructuredImageExtract(raw string) (normalizedStructuredImageExtract, bool) {
	candidate, ok := structuredImageExtractJSONCandidate(raw)
	if !ok {
		return normalizedStructuredImageExtract{}, false
	}
	var resp structuredImageExtractResponse
	if err := json.Unmarshal([]byte(candidate), &resp); err != nil {
		return normalizedStructuredImageExtract{}, false
	}
	meta := normalizeStructuredImageExtract(resp)
	if meta.empty() {
		return normalizedStructuredImageExtract{}, false
	}
	return meta, true
}

func structuredImageExtractJSONCandidate(raw string) (string, bool) {
	text := strings.TrimSpace(strings.TrimPrefix(raw, "\ufeff"))
	if text == "" {
		return "", false
	}
	if strings.HasPrefix(text, "```") {
		lines := strings.Split(text, "\n")
		if len(lines) >= 2 && strings.HasPrefix(strings.TrimSpace(lines[0]), "```") {
			lines = lines[1:]
			if len(lines) > 0 && strings.HasPrefix(strings.TrimSpace(lines[len(lines)-1]), "```") {
				lines = lines[:len(lines)-1]
			}
			text = strings.TrimSpace(strings.Join(lines, "\n"))
		}
	}
	if strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}") {
		return text, true
	}
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start < 0 || end <= start {
		return "", false
	}
	return text[start : end+1], true
}

func normalizeStructuredImageExtract(resp structuredImageExtractResponse) normalizedStructuredImageExtract {
	return normalizedStructuredImageExtract{
		captionZH:       cleanImageExtractField(resp.CaptionZH, maxImageExtractCaptionZHRuneCount),
		descriptionZH:   cleanImageExtractField(resp.DescriptionZH, maxImageExtractDescriptionZHRuneCount),
		captionEN:       cleanImageExtractField(resp.CaptionEN, maxImageExtractCaptionENRuneCount),
		descriptionEN:   cleanImageExtractField(resp.DescriptionEN, maxImageExtractDescriptionENRuneCount),
		ocrText:         cleanImageExtractList(resp.OCRText, maxImageExtractOCRItems, maxImageExtractListItemRuneCount),
		tagsZH:          cleanImageExtractTags(resp.TagsZH, maxImageExtractTags, false),
		tagsEN:          cleanImageExtractTags(resp.TagsEN, maxImageExtractTags, true),
		searchQueriesZH: cleanImageExtractList(resp.SearchQueriesZH, maxImageExtractSearchQueries, maxImageExtractListItemRuneCount),
		searchQueriesEN: cleanImageExtractList(resp.SearchQueriesEN, maxImageExtractSearchQueries, maxImageExtractListItemRuneCount),
	}
}

func (m normalizedStructuredImageExtract) empty() bool {
	return m.captionZH == "" &&
		m.descriptionZH == "" &&
		m.captionEN == "" &&
		m.descriptionEN == "" &&
		len(m.ocrText) == 0 &&
		len(m.tagsZH) == 0 &&
		len(m.tagsEN) == 0 &&
		len(m.searchQueriesZH) == 0 &&
		len(m.searchQueriesEN) == 0
}

func buildStructuredImageExtractText(meta normalizedStructuredImageExtract) string {
	lines := make([]string, 0, 9)
	addLine := func(label, value string) {
		if value != "" {
			lines = append(lines, label+"："+value)
		}
	}
	addLine("中文摘要", meta.captionZH)
	addLine("中文描述", meta.descriptionZH)
	addLine("英文摘要", meta.captionEN)
	addLine("英文描述", meta.descriptionEN)
	if len(meta.ocrText) > 0 {
		addLine("图中文字", strings.Join(meta.ocrText, "；"))
	}
	if len(meta.tagsZH) > 0 {
		addLine("中文标签", strings.Join(meta.tagsZH, "，"))
	}
	if len(meta.tagsEN) > 0 {
		addLine("英文标签", strings.Join(meta.tagsEN, ", "))
	}
	if len(meta.searchQueriesZH) > 0 {
		addLine("中文搜索短语", strings.Join(meta.searchQueriesZH, "；"))
	}
	if len(meta.searchQueriesEN) > 0 {
		addLine("英文搜索短语", strings.Join(meta.searchQueriesEN, "; "))
	}
	return strings.Join(lines, "\n")
}

func sanitizeStructuredImageExtractText(in string, maxBytes int) string {
	in = strings.TrimSpace(in)
	if in == "" {
		return ""
	}
	rawLines := strings.Split(in, "\n")
	lines := make([]string, 0, len(rawLines))
	for _, line := range rawLines {
		line = strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
		if line != "" {
			lines = append(lines, line)
		}
	}
	text := strings.Join(lines, "\n")
	if maxBytes <= 0 || len(text) <= maxBytes {
		return text
	}
	return truncateUTF8Bytes(text, maxBytes)
}

func buildStructuredImageExtractTags(meta normalizedStructuredImageExtract) map[string]string {
	tags := map[string]string{
		imageExtractTagPrefix + "schema": imageExtractStructuredSchema,
		imageExtractTagPrefix + "source": imageExtractSource,
	}
	for i, tag := range meta.tagsZH {
		tags[imageExtractTagPrefix+"tag.zh."+strconv.Itoa(i)] = tag
	}
	for i, tag := range meta.tagsEN {
		tags[imageExtractTagPrefix+"tag.en."+strconv.Itoa(i)] = tag
	}
	return tags
}

func cleanImageExtractList(values []string, maxItems int, maxRunes int) []string {
	if maxItems <= 0 {
		return nil
	}
	out := make([]string, 0, min(len(values), maxItems))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		cleaned := cleanImageExtractField(value, maxRunes)
		if cleaned == "" {
			continue
		}
		key := strings.ToLower(cleaned)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, cleaned)
		if len(out) >= maxItems {
			break
		}
	}
	return out
}

func cleanImageExtractTags(values []string, maxItems int, lowercase bool) []string {
	if maxItems <= 0 {
		return nil
	}
	out := make([]string, 0, min(len(values), maxItems))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		cleaned := cleanImageExtractTag(value, lowercase)
		if cleaned == "" || isGenericImageExtractTag(cleaned) {
			continue
		}
		key := strings.ToLower(cleaned)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, cleaned)
		if len(out) >= maxItems {
			break
		}
	}
	return out
}

func cleanImageExtractTag(value string, lowercase bool) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"'“”‘’.,;:!?()[]{}<>#|`)
	value = strings.ReplaceAll(value, "_", " ")
	value = strings.Join(strings.Fields(value), " ")
	if lowercase {
		value = strings.ToLower(value)
	}
	return truncateRunes(value, maxImageExtractTagRuneCount)
}

func cleanImageExtractField(value string, maxRunes int) string {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if value == "" {
		return ""
	}
	return truncateRunes(value, maxRunes)
}

func truncateRunes(value string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return strings.TrimSpace(string(runes[:maxRunes]))
}

func isGenericImageExtractTag(tag string) bool {
	switch strings.ToLower(strings.TrimSpace(tag)) {
	case "image", "photo", "picture", "pic", "photograph", "ocr", "text", "visible text", "caption", "description", "tag", "tags", "file",
		"图片", "图像", "照片", "文字", "标签", "描述":
		return true
	default:
		return false
	}
}

func truncateByEstimatedTokens(value string, maxTokens int) string {
	if maxTokens <= 0 || estimateImageExtractTokens(value) <= maxTokens {
		return value
	}
	runes := []rune(value)
	lo, hi := 0, len(runes)
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if estimateImageExtractTokens(string(runes[:mid])) <= maxTokens {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return strings.TrimSpace(string(runes[:lo]))
}

func estimateImageExtractTokens(value string) int {
	var tokens int
	var asciiRunes int
	flushASCII := func() {
		if asciiRunes > 0 {
			tokens += (asciiRunes + 3) / 4
			asciiRunes = 0
		}
	}
	for _, r := range value {
		if r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
			asciiRunes++
			continue
		}
		flushASCII()
		if unicode.IsSpace(r) {
			continue
		}
		tokens++
	}
	flushASCII()
	if tokens == 0 && strings.TrimSpace(value) != "" {
		return 1
	}
	return tokens
}
