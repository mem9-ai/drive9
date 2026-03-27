package cli

import (
	"fmt"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

func NewClientForContext(ctxName string) *client.Client {
	server := os.Getenv("DAT9_SERVER")
	apiKey := os.Getenv("DAT9_API_KEY")

	cfg := loadConfig()
	if server == "" {
		server = cfg.ResolveServer()
	}
	if apiKey == "" {
		name := ctxName
		if name == "" {
			name = cfg.CurrentContext
		}
		if name != "" {
			ctx, ok := cfg.Contexts[name]
			if !ok && ctxName != "" {
				fmt.Fprintf(os.Stderr, "error: context %q not found in %s\n", ctxName, configPath())
				os.Exit(1)
			}
			if ok {
				apiKey = ctx.APIKey
			}
		}
	}
	return client.New(server, apiKey)
}

func NewFromEnv() *client.Client {
	return NewClientForContext("")
}
