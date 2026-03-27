package cli

import (
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

func NewFromEnv() *client.Client {
	server := os.Getenv("DAT9_SERVER")
	apiKey := os.Getenv("DAT9_API_KEY")

	cfg := loadConfig()
	if server == "" {
		server = cfg.ResolveServer()
	}
	if apiKey == "" {
		apiKey = cfg.CurrentAPIKey()
	}
	return client.New(server, apiKey)
}
