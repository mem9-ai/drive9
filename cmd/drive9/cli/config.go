package cli

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

type Context struct {
	APIKey string `json:"api_key"`
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

func (c *Config) CurrentAPIKey() string {
	if c.CurrentContext == "" {
		return ""
	}
	ctx, ok := c.Contexts[c.CurrentContext]
	if !ok {
		return ""
	}
	return ctx.APIKey
}

func (c *Config) ResolveServer() string {
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
