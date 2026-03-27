package cli

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

func NewFromEnv() *client.Client {
	server := os.Getenv("DAT9_SERVER")
	if server == "" {
		server = "http://localhost:9009"
	}
	apiKey := os.Getenv("DAT9_API_KEY")
	if apiKey == "" {
		apiKey = loadCredential("api_key")
	}
	return client.New(server, apiKey)
}

func configDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".dat9")
}

func credentialsPath() string {
	dir := configDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "credentials")
}

func loadCredential(key string) string {
	path := credentialsPath()
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		v = strings.Trim(v, "\"")
		if k == key {
			return v
		}
	}
	return ""
}
