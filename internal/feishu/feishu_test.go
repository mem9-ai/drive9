package feishu

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTransportSelection(t *testing.T) {
	if got := (Config{}).Transport(); got != TransportNone {
		t.Errorf("empty config = %q, want none", got)
	}
	if got := (Config{Webhook: "https://h"}).Transport(); got != TransportWebhook {
		t.Errorf("webhook = %q", got)
	}
	app := Config{AppID: "a", AppSecret: "s", ChatID: "c"}
	if got := app.Transport(); got != TransportApp {
		t.Errorf("app = %q", got)
	}
	// Incomplete app config is not selectable.
	if got := (Config{AppID: "a", AppSecret: "s"}).Transport(); got != TransportNone {
		t.Errorf("incomplete app = %q, want none", got)
	}
	// Webhook wins when both present.
	both := Config{Webhook: "https://h", AppID: "a", AppSecret: "s", ChatID: "c"}
	if got := both.Transport(); got != TransportWebhook {
		t.Errorf("both = %q, want webhook", got)
	}
}

func TestConfigFromEnv(t *testing.T) {
	env := map[string]string{
		"FEISHU_APP_ID":     " app123 ",
		"FEISHU_APP_SECRET": "secret",
		"FEISHU_CHAT_ID":    "oc_x",
	}
	c := ConfigFromEnv(func(k string) string { return env[k] })
	if c.AppID != "app123" || c.AppSecret != "secret" || c.ChatID != "oc_x" {
		t.Fatalf("config from env wrong: %+v", c)
	}
	if c.Transport() != TransportApp {
		t.Fatalf("transport = %q", c.Transport())
	}
}

func TestAppMessageBodyContentIsJSONString(t *testing.T) {
	card := map[string]any{"header": map[string]any{"title": "x"}}
	body, err := appMessageBody("oc_chat", card)
	if err != nil {
		t.Fatal(err)
	}
	if body["receive_id"] != "oc_chat" || body["msg_type"] != "interactive" {
		t.Fatalf("body wrong: %+v", body)
	}
	content, ok := body["content"].(string)
	if !ok || !strings.Contains(content, "\"header\"") {
		t.Fatalf("content must be a JSON string: %#v", body["content"])
	}
}

func TestSendNoneSkips(t *testing.T) {
	sent, err := Send(context.Background(), Config{}, map[string]any{})
	if sent || err != nil {
		t.Fatalf("unconfigured send should skip: sent=%v err=%v", sent, err)
	}
}

func TestSendWebhook(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		_, _ = w.Write([]byte(`{"code":0,"msg":"success"}`))
	}))
	defer srv.Close()

	sent, err := Send(context.Background(), Config{Webhook: srv.URL}, map[string]any{"header": "h"})
	if !sent || err != nil {
		t.Fatalf("webhook send: sent=%v err=%v", sent, err)
	}
	if got["msg_type"] != "interactive" || got["card"] == nil {
		t.Fatalf("webhook payload wrong: %+v", got)
	}
}

func TestSendAppFlow(t *testing.T) {
	var tokenHits, msgHits int
	var msgAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "tenant_access_token"):
			tokenHits++
			_, _ = w.Write([]byte(`{"code":0,"tenant_access_token":"t-abc","expire":7200}`))
		case strings.Contains(r.URL.Path, "/im/v1/messages"):
			msgHits++
			msgAuth = r.Header.Get("Authorization")
			if r.URL.Query().Get("receive_id_type") != "chat_id" {
				t.Errorf("missing receive_id_type=chat_id")
			}
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok"}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	cfg := Config{AppID: "a", AppSecret: "s", ChatID: "oc_x", BaseURL: srv.URL}
	sent, err := Send(context.Background(), cfg, map[string]any{"header": "h"})
	if !sent || err != nil {
		t.Fatalf("app send: sent=%v err=%v", sent, err)
	}
	if tokenHits != 1 || msgHits != 1 {
		t.Fatalf("expected 1 token + 1 message hit, got %d/%d", tokenHits, msgHits)
	}
	if msgAuth != "Bearer t-abc" {
		t.Fatalf("auth header = %q", msgAuth)
	}
}

func TestPostJSONSurfacesFeishuLogicalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":9499,"msg":"bad chat id"}`))
	}))
	defer srv.Close()

	_, err := Send(context.Background(), Config{Webhook: srv.URL}, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "9499") {
		t.Fatalf("expected logical error surfaced, got %v", err)
	}
}
