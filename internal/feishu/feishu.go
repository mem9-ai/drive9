// Package feishu sends drive9 e2e notification cards to Feishu/Lark. It supports
// two transports, auto-detected from the environment, so the deployment can use
// whichever the team has set up:
//
//   - custom-bot webhook: set FEISHU_WEBHOOK to the bot hook URL.
//   - app (tenant) API:   set FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_CHAT_ID.
//
// If neither is configured, callers should skip silently — a missing secret must
// never fail the CI run.
package feishu

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

const (
	defaultBaseURL = "https://open.feishu.cn"
	httpTimeout    = 15 * time.Second
)

// Transport identifies which Feishu send mechanism is configured.
type Transport string

const (
	TransportNone    Transport = ""
	TransportWebhook Transport = "webhook"
	TransportApp     Transport = "app"
)

// Config holds Feishu connection settings, typically sourced from CI secrets.
type Config struct {
	Webhook   string
	AppID     string
	AppSecret string
	ChatID    string
	// BaseURL overrides the app-API base (tests). Empty means the public host.
	BaseURL string
}

// ConfigFromEnv reads Feishu settings from the standard environment variables.
func ConfigFromEnv(getenv func(string) string) Config {
	return Config{
		Webhook:   strings.TrimSpace(getenv("FEISHU_WEBHOOK")),
		AppID:     strings.TrimSpace(getenv("FEISHU_APP_ID")),
		AppSecret: strings.TrimSpace(getenv("FEISHU_APP_SECRET")),
		ChatID:    strings.TrimSpace(getenv("FEISHU_CHAT_ID")),
		BaseURL:   strings.TrimSpace(getenv("FEISHU_BASE_URL")),
	}
}

// Transport reports which mechanism this config selects. Webhook takes
// precedence when both are present.
func (c Config) Transport() Transport {
	switch {
	case c.Webhook != "":
		return TransportWebhook
	case c.AppID != "" && c.AppSecret != "" && c.ChatID != "":
		return TransportApp
	default:
		return TransportNone
	}
}

func (c Config) baseURL() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	return defaultBaseURL
}

// webhookBody is the custom-bot webhook payload for an interactive card.
func webhookBody(card map[string]any) map[string]any {
	return map[string]any{"msg_type": "interactive", "card": card}
}

// appMessageBody is the im/v1/messages payload for an interactive card. Feishu
// requires `content` to be a JSON string, not a nested object.
func appMessageBody(chatID string, card map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(card)
	if err != nil {
		return nil, fmt.Errorf("marshal card: %w", err)
	}
	return map[string]any{
		"receive_id": chatID,
		"msg_type":   "interactive",
		"content":    string(raw),
	}, nil
}

// Send delivers the card via the configured transport. It returns (false, nil)
// when no transport is configured so callers can skip without failing.
func Send(ctx context.Context, c Config, card map[string]any) (sent bool, err error) {
	switch c.Transport() {
	case TransportWebhook:
		return true, c.sendWebhook(ctx, card)
	case TransportApp:
		return true, c.sendApp(ctx, card)
	default:
		return false, nil
	}
}

func (c Config) sendWebhook(ctx context.Context, card map[string]any) error {
	return postJSON(ctx, c.Webhook, nil, webhookBody(card))
}

func (c Config) sendApp(ctx context.Context, card map[string]any) error {
	token, err := c.tenantToken(ctx)
	if err != nil {
		return err
	}
	body, err := appMessageBody(c.ChatID, card)
	if err != nil {
		return err
	}
	url := c.baseURL() + "/open-apis/im/v1/messages?receive_id_type=chat_id"
	return postJSON(ctx, url, map[string]string{"Authorization": "Bearer " + token}, body)
}

func (c Config) tenantToken(ctx context.Context) (string, error) {
	url := c.baseURL() + "/open-apis/auth/v3/tenant_access_token/internal"
	reqBody, _ := json.Marshal(map[string]string{"app_id": c.AppID, "app_secret": c.AppSecret})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := httpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("request tenant token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	var out struct {
		Code  int    `json:"code"`
		Msg   string `json:"msg"`
		Token string `json:"tenant_access_token"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", fmt.Errorf("decode tenant token (status %d): %w", resp.StatusCode, err)
	}
	if out.Code != 0 || out.Token == "" {
		return "", fmt.Errorf("tenant token error: code=%d msg=%s", out.Code, out.Msg)
	}
	return out.Token, nil
}

func postJSON(ctx context.Context, url string, headers map[string]string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("post %s: status %d: %s", url, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	// Feishu returns 200 with a non-zero `code` on logical errors.
	var out struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(data, &out); err == nil && out.Code != 0 {
		return fmt.Errorf("post %s: feishu code=%d msg=%s", url, out.Code, out.Msg)
	}
	return nil
}

func httpClient() *http.Client { return &http.Client{Timeout: httpTimeout} }
