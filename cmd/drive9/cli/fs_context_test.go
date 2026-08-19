package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
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

func TestExplicitContextUsesEnvServerFallback(t *testing.T) {
	withIsolatedHome(t)

	configServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("config server should not be used: %s %s", r.Method, r.URL.Path)
	}))
	defer configServer.Close()

	envServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer fork-key" {
			t.Fatalf("Authorization = %q, want Bearer fork-key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []map[string]any{
				{"name": "env-docs", "isDir": true},
			},
		})
	}))
	defer envServer.Close()

	cfg := loadConfig()
	cfg.Server = configServer.URL
	cfg.Contexts = map[string]*Context{
		"fork": {Type: PrincipalOwner, APIKey: "fork-key"},
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	t.Setenv(EnvServer, envServer.URL)

	out, err := captureStdoutE(t, func() error {
		return Ls(client.New(configServer.URL, "unused"), []string{"fork:/"})
	})
	if err != nil {
		t.Fatalf("Ls: %v", err)
	}
	if !strings.Contains(out, "env-docs/") {
		t.Fatalf("Ls output = %q, want env-docs/", out)
	}
}

func TestExplicitContextUsesResolverConfigSnapshot(t *testing.T) {
	withIsolatedHome(t)

	initial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/fs/" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer fork-key" {
			t.Fatalf("Authorization = %q, want Bearer fork-key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": []map[string]any{
				{"name": "snapshot-docs", "isDir": true},
			},
		})
	}))
	defer initial.Close()

	later := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("later config server should not be observed: %s %s", r.Method, r.URL.Path)
	}))
	defer later.Close()

	cfg := loadConfig()
	cfg.CurrentContext = "current"
	cfg.Contexts = map[string]*Context{
		"current": {Type: PrincipalOwner, APIKey: "current-key", Server: initial.URL},
		"fork":    {Type: PrincipalOwner, APIKey: "fork-key", Server: initial.URL},
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	_ = ResolveCredentials()

	mutated := &Config{
		CurrentContext: "current",
		Contexts: map[string]*Context{
			"current": {Type: PrincipalOwner, APIKey: "current-key", Server: later.URL},
			"fork":    {Type: PrincipalOwner, APIKey: "fork-key", Server: later.URL},
		},
	}
	if err := saveConfig(mutated); err != nil {
		t.Fatalf("save mutated config: %v", err)
	}

	out, err := captureStdoutE(t, func() error {
		return Ls(client.New(initial.URL, "current-key"), []string{"fork:/"})
	})
	if err != nil {
		t.Fatalf("Ls: %v", err)
	}
	if !strings.Contains(out, "snapshot-docs/") {
		t.Fatalf("Ls output = %q, want snapshot-docs/", out)
	}
}

func TestCpStreamsMixedExplicitAndCurrentRemoteContexts(t *testing.T) {
	withIsolatedHome(t)

	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", "5")
			w.Header().Set("X-Dat9-IsDir", "false")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet:
			_, _ = w.Write([]byte("hello"))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer src.Close()

	var putPath string
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 1 << 20})
			return
		}
		if r.Method == http.MethodPut {
			putPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer dst.Close()

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "current", &Context{Type: PrincipalOwner, APIKey: "current-key", Server: dst.URL}); err != nil {
		t.Fatalf("ctxAdd current: %v", err)
	}
	if _, err := ctxAdd(cfg, "prod", &Context{Type: PrincipalOwner, APIKey: "prod-key", Server: src.URL}); err != nil {
		t.Fatalf("ctxAdd prod: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	if err := Cp(client.New(dst.URL, "current-key"), []string{"prod:/a", ":/b"}); err != nil {
		t.Fatalf("Cp cross-context stream: %v", err)
	}
	if putPath != "/v1/fs/b" {
		t.Fatalf("dst PUT path = %q, want /v1/fs/b", putPath)
	}
}

func TestCpRecursiveStreamsMixedExplicitAndCurrentRemoteContexts(t *testing.T) {
	withIsolatedHome(t)

	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list") == "1" {
			w.Header().Set("Content-Type", "application/json")
			entries := []map[string]any{{"name": "f", "isDir": false, "size": 5}}
			if strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/dir") {
				entries = []map[string]any{
					{"name": "f", "isDir": false, "size": 5},
					{"name": "sub", "isDir": true, "size": 0},
				}
			}
			if strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/sub") {
				entries = []map[string]any{{"name": "n.txt", "isDir": false, "size": 6}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
			return
		}
		if r.Method == http.MethodHead {
			isDir := strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/dir") ||
				strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/sub")
			w.Header().Set("X-Dat9-IsDir", fmt.Sprintf("%t", isDir))
			if !isDir {
				w.Header().Set("Content-Length", "5")
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("hello"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer src.Close()

	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"inline_threshold": 1 << 20})
			return
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"revision": 1})
	}))
	defer dst.Close()

	cfg := loadConfig()
	if _, err := ctxAdd(cfg, "current", &Context{Type: PrincipalOwner, APIKey: "current-key", Server: dst.URL}); err != nil {
		t.Fatalf("ctxAdd current: %v", err)
	}
	if _, err := ctxAdd(cfg, "prod", &Context{Type: PrincipalOwner, APIKey: "prod-key", Server: src.URL}); err != nil {
		t.Fatalf("ctxAdd prod: %v", err)
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	if err := Cp(client.New(dst.URL, "current-key"), []string{"-r", "prod:/dir", ":/dir"}); err != nil {
		t.Fatalf("Cp -r cross-context stream: %v", err)
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
