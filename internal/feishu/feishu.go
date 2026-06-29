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
	return postJSON(ctx, "Feishu webhook", c.Webhook, nil, webhookBody(card))
}

func (c Config) sendApp(ctx context.Context, card map[string]any) error {
	// Fetches a fresh tenant token per send. Fine for the one-send-per-CI-run use
	// case; TODO: cache the token (2h TTL) if this is ever used for multi-send.
	token, err := c.tenantToken(ctx)
	if err != nil {
		return err
	}
	body, err := appMessageBody(c.ChatID, card)
	if err != nil {
		return err
	}
	url := c.baseURL() + "/open-apis/im/v1/messages?receive_id_type=chat_id"
	return postJSON(ctx, "Feishu message API", url, map[string]string{"Authorization": "Bearer " + token}, body)
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

// postJSON posts body to url and validates the Feishu response. Errors carry the
// endpoint label, never the URL: the custom-bot webhook URL embeds the bot token,
// so logging it on a transient failure would leak the secret into CI logs.
func postJSON(ctx context.Context, endpoint, url string, headers map[string]string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", endpoint, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build %s request: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("post %s: status %d: %s", endpoint, resp.StatusCode, snippet(data))
	}
	// Feishu reports logical failures in the body while returning HTTP 200. The
	// app API uses {code,msg}; the custom-bot webhook uses {StatusCode,
	// StatusMessage}. A 2xx body that is not valid JSON (proxy, wrong host, HTML
	// error page) must fail closed rather than look delivered.
	var out struct {
		Code          int    `json:"code"`
		Msg           string `json:"msg"`
		StatusCode    int    `json:"StatusCode"`
		StatusMessage string `json:"StatusMessage"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return fmt.Errorf("post %s: unexpected non-JSON 2xx response: %s", endpoint, snippet(data))
	}
	if out.Code != 0 {
		return fmt.Errorf("post %s: feishu code=%d msg=%s", endpoint, out.Code, out.Msg)
	}
	if out.StatusCode != 0 {
		return fmt.Errorf("post %s: feishu StatusCode=%d msg=%s", endpoint, out.StatusCode, out.StatusMessage)
	}
	return nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

func httpClient() *http.Client { return &http.Client{Timeout: httpTimeout} }
