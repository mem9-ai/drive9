package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
)

func TestLsUsesExplicitRemoteContext(t *testing.T) {
	withIsolatedHome(t)

	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("current context should not be used: %s %s", r.Method, r.URL.Path)
	}))
	defer current.Close()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer fork-key" {
			t.Fatalf("Authorization = %q, want Bearer fork-key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []map[string]any{
				{"name": "docs", "isDir": true},
			},
		})
	}))
	defer target.Close()

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "current", &Context{Type: PrincipalOwner, APIKey: "current-key", Server: current.URL}); err != nil {
		t.Fatalf("ctxAdd current: %v", err)
	}
	if _, err := ctxAdd(cfg, "fork", &Context{Type: PrincipalOwner, APIKey: "fork-key", Server: target.URL}); err != nil {
		t.Fatalf("ctxAdd fork: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	out, err := captureStdoutE(t, func() error {
		return Ls(client.New(current.URL, "current-key"), []string{"fork:/"})
	})
	if err != nil {
		t.Fatalf("Ls: %v", err)
	}
	if !strings.Contains(out, "docs/") {
		t.Fatalf("Ls output = %q, want docs/", out)
	}
}

func TestLsRejectsDelegatedExplicitRemoteContext(t *testing.T) {
	withIsolatedHome(t)

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "delegated", &Context{Type: PrincipalDelegated, Token: "tok", Server: "https://drive9.example"}); err != nil {
		t.Fatalf("ctxAdd delegated: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	err := Ls(client.New("https://unused.example", "unused"), []string{"delegated:/"})
	if err == nil || !strings.Contains(err.Error(), "require an owner or fs_scoped context") {
		t.Fatalf("Ls error = %v, want explicit delegated-context error", err)
	}
}

func TestExplicitContextUsesConfigServerFallbackInsteadOfActiveContextServer(t *testing.T) {
	withIsolatedHome(t)

	active := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("active context server should not be used: %s %s", r.Method, r.URL.Path)
	}))
	defer active.Close()

	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer fork-key" {
			t.Fatalf("Authorization = %q, want Bearer fork-key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []map[string]any{
				{"name": "docs", "isDir": true},
			},
		})
	}))
	defer fallback.Close()

	cfg := loadConfig()
	cfg.Server = fallback.URL
	cfg.CurrentContext = "current"
	cfg.Contexts = map[string]*Context{
		"current": {Type: PrincipalOwner, APIKey: "current-key", Server: active.URL},
		"fork":    {Type: PrincipalOwner, APIKey: "fork-key"},
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	out, err := captureStdoutE(t, func() error {
		return Ls(client.New(active.URL, "current-key"), []string{"fork:/"})
	})
	if err != nil {
		t.Fatalf("Ls: %v", err)
	}
	if !strings.Contains(out, "docs/") {
		t.Fatalf("Ls output = %q, want docs/", out)
	}
}

func TestCpRejectsMixedExplicitAndCurrentRemoteContexts(t *testing.T) {
	withIsolatedHome(t)

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "current", &Context{Type: PrincipalOwner, APIKey: "current-key", Server: "https://current.example"}); err != nil {
		t.Fatalf("ctxAdd current: %v", err)
	}
	if _, err := ctxAdd(cfg, "prod", &Context{Type: PrincipalOwner, APIKey: "prod-key", Server: "https://prod.example"}); err != nil {
		t.Fatalf("ctxAdd prod: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	err := Cp(client.New("https://current.example", "current-key"), []string{"prod:/a", ":/b"})
	if err == nil || !strings.Contains(err.Error(), "cross-context copy not supported") {
		t.Fatalf("Cp error = %v, want cross-context rejection", err)
	}
}

func TestCpRecursiveRejectsMixedExplicitAndCurrentRemoteContexts(t *testing.T) {
	withIsolatedHome(t)

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "current", &Context{Type: PrincipalOwner, APIKey: "current-key", Server: "https://current.example"}); err != nil {
		t.Fatalf("ctxAdd current: %v", err)
	}
	if _, err := ctxAdd(cfg, "prod", &Context{Type: PrincipalOwner, APIKey: "prod-key", Server: "https://prod.example"}); err != nil {
		t.Fatalf("ctxAdd prod: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	err := Cp(client.New("https://current.example", "current-key"), []string{"-r", "prod:/dir", ":/dir"})
	if err == nil || !strings.Contains(err.Error(), "cross-context copy not supported") {
		t.Fatalf("Cp recursive error = %v, want cross-context rejection", err)
	}
}

func TestMvRejectsMixedExplicitAndCurrentRemoteContexts(t *testing.T) {
	withIsolatedHome(t)

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "current", &Context{Type: PrincipalOwner, APIKey: "current-key", Server: "https://current.example"}); err != nil {
		t.Fatalf("ctxAdd current: %v", err)
	}
	if _, err := ctxAdd(cfg, "prod", &Context{Type: PrincipalOwner, APIKey: "prod-key", Server: "https://prod.example"}); err != nil {
		t.Fatalf("ctxAdd prod: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	err := Mv(client.New("https://current.example", "current-key"), []string{":/old", "prod:/new"})
	if err == nil || !strings.Contains(err.Error(), "cross-context rename not supported") {
		t.Fatalf("Mv error = %v, want cross-context rejection", err)
	}
}
