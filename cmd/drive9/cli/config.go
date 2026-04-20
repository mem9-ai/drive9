package cli

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// PrincipalType identifies whether a context holds an owner API key or a
// delegated JWT. See spec §13.1.
type PrincipalType string

const (
	PrincipalOwner     PrincipalType = "owner"
	PrincipalDelegated PrincipalType = "delegated"
)

// Perm is the scope permission carried by a delegated context's JWT. Spec §6
// restricts this to {read, write}; "admin" is NOT a valid value.
type Perm string

const (
	PermRead  Perm = "read"
	PermWrite Perm = "write"
)

// Context is a single entry in ~/.drive9/config. Field presence depends on
// Type per spec §13.1:
//
//   - owner:     APIKey, Server
//   - delegated: Token, Server (from iss), Agent, Scope[], Perm, ExpiresAt,
//                GrantID, optional LabelHint
//
// The delegated fields are populated by locally decoding the JWT payload at
// `ctx import` time. This is UX-only — authorization remains server-side
// (Invariant #7).
type Context struct {
	Type      PrincipalType `json:"type"`
	Server    string        `json:"server,omitempty"`
	APIKey    string        `json:"api_key,omitempty"`
	Token     string        `json:"token,omitempty"`
	Agent     string        `json:"agent,omitempty"`
	Scope     []string      `json:"scope,omitempty"`
	Perm      Perm          `json:"perm,omitempty"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
	GrantID   string        `json:"grant_id,omitempty"`
	LabelHint string        `json:"label_hint,omitempty"`
}

type Config struct {
	Server         string              `json:"server"`
	CurrentContext string              `json:"current_context,omitempty"`
	Contexts       map[string]*Context `json:"contexts"`
}

func configDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".drive9")
}

func configPath() string {
	dir := configDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "config")
}

// loadConfig reads ~/.drive9/config. Missing / malformed files yield an empty
// Config. Contexts without an explicit Type are treated as owner-kind so that
// configs written by the pre-spec single-field form continue to resolve.
func loadConfig() *Config {
	path := configPath()
	if path == "" {
		return &Config{Contexts: map[string]*Context{}}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return &Config{Contexts: map[string]*Context{}}
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return &Config{Contexts: map[string]*Context{}}
	}
	if cfg.Contexts == nil {
		cfg.Contexts = map[string]*Context{}
	}
	for _, ctx := range cfg.Contexts {
		if ctx == nil {
			continue
		}
		if ctx.Type == "" {
			ctx.Type = PrincipalOwner
		}
	}
	return &cfg
}

func saveConfig(cfg *Config) error {
	dir := configDir()
	if dir == "" {
		return fmt.Errorf("cannot determine home directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath(), append(data, '\n'), 0o600)
}

// CurrentAPIKey returns the active owner context's API key, or empty when the
// active context is delegated or absent.
func (c *Config) CurrentAPIKey() string {
	ctx := c.currentContextEntry()
	if ctx == nil || ctx.Type != PrincipalOwner {
		return ""
	}
	return ctx.APIKey
}

// CurrentToken returns the active delegated context's JWT, or empty when the
// active context is owner-kind or absent.
func (c *Config) CurrentToken() string {
	ctx := c.currentContextEntry()
	if ctx == nil || ctx.Type != PrincipalDelegated {
		return ""
	}
	return ctx.Token
}

func (c *Config) currentContextEntry() *Context {
	if c.CurrentContext == "" {
		return nil
	}
	ctx, ok := c.Contexts[c.CurrentContext]
	if !ok {
		return nil
	}
	return ctx
}

// ResolveServer returns the active context's server URL when set, falling back
// to the top-level Config.Server and finally the compiled-in default.
func (c *Config) ResolveServer() string {
	if ctx := c.currentContextEntry(); ctx != nil && ctx.Server != "" {
		return ctx.Server
	}
	if c.Server != "" {
		return c.Server
	}
	return "https://api.drive9.ai"
}

const nameChars = "abcdefghijklmnopqrstuvwxyz0123456789"

func randomName() string {
	b := make([]byte, 7)
	for i := range b {
		b[i] = nameChars[rand.Intn(len(nameChars))]
	}
	return string(b)
}
