package cli

import (
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

func NewClient(dbName string) *client.Client {
	server := os.Getenv("DAT9_SERVER")
	apiKey := os.Getenv("DAT9_API_KEY")

	cfg := loadConfig()
	if dbName == "" {
		dbName = cfg.DefaultDB
	}

	entry := cfg.GetDB(dbName)
	if entry != nil {
		if server == "" {
			server = entry.Server
		}
		if apiKey == "" {
			apiKey = entry.APIKey
		}
	}

	if server == "" {
		server = "http://localhost:9009"
	}
	return client.New(server, apiKey)
}

func NewFromEnv() *client.Client {
	return NewClient("")
}
