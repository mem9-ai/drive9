package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type DBEntry struct {
	Server string `json:"server"`
	APIKey string `json:"api_key"`
}

type Config struct {
	Databases map[string]*DBEntry `json:"databases"`
	DefaultDB string              `json:"default_db,omitempty"`
}

func configDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".dat9")
}

func configPath() string {
	dir := configDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "credentials.json")
}

func loadConfig() *Config {
	path := configPath()
	if path == "" {
		return &Config{Databases: map[string]*DBEntry{}}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return &Config{Databases: map[string]*DBEntry{}}
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return &Config{Databases: map[string]*DBEntry{}}
	}
	if cfg.Databases == nil {
		cfg.Databases = map[string]*DBEntry{}
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

func (c *Config) GetDB(name string) *DBEntry {
	if name == "" {
		name = c.DefaultDB
	}
	if name == "" {
		return nil
	}
	return c.Databases[name]
}

func (c *Config) SetDB(name string, entry *DBEntry) {
	c.Databases[name] = entry
}

func (c *Config) SetDefault(name string) {
	c.DefaultDB = name
}
