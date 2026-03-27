package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/mem9-ai/dat9/pkg/client"
)

func DBCreate(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: dat9 db create <name> [--server URL]")
	}
	name := args[0]
	server := os.Getenv("DAT9_SERVER")
	if server == "" {
		server = "http://localhost:9009"
	}

	for i := 1; i < len(args); i++ {
		if args[i] == "--server" && i+1 < len(args) {
			i++
			server = args[i]
		}
	}

	c := client.New(server, "")
	resp, err := c.RawPost("/v1/provision", nil)
	if err != nil {
		return fmt.Errorf("provision request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("provision failed (HTTP %d): %s", resp.StatusCode, errResp.Error)
	}

	var result struct {
		TenantID string `json:"tenant_id"`
		APIKey   string `json:"api_key"`
		APIKeyID string `json:"api_key_id"`
		Status   string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode provision response: %w", err)
	}

	cfg := loadConfig()
	cfg.SetDB(name, &DBEntry{
		Server: server,
		APIKey: result.APIKey,
	})
	if cfg.DefaultDB == "" {
		cfg.SetDefault(name)
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save credentials: %w", err)
	}

	fmt.Printf("database %q created (tenant: %s, status: %s)\n", name, result.TenantID, result.Status)
	fmt.Printf("API key saved to %s\n", configPath())
	if cfg.DefaultDB == name {
		fmt.Printf("set as default database\n")
	}
	return nil
}

func DBList() error {
	cfg := loadConfig()
	if len(cfg.Databases) == 0 {
		fmt.Println("no databases configured")
		fmt.Println("usage: dat9 db create <name>")
		return nil
	}
	for name, entry := range cfg.Databases {
		marker := "  "
		if name == cfg.DefaultDB {
			marker = "* "
		}
		masked := entry.APIKey
		if len(masked) > 12 {
			masked = masked[:8] + "..." + masked[len(masked)-4:]
		}
		fmt.Printf("%s%-12s  server=%s  key=%s\n", marker, name, entry.Server, masked)
	}
	return nil
}

func DBStatus(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: dat9 db status <name>")
	}
	name := args[0]
	cfg := loadConfig()
	entry := cfg.GetDB(name)
	if entry == nil {
		return fmt.Errorf("database %q not found in credentials", name)
	}
	fmt.Printf("name:    %s\n", name)
	fmt.Printf("server:  %s\n", entry.Server)
	masked := entry.APIKey
	if len(masked) > 12 {
		masked = masked[:8] + "..." + masked[len(masked)-4:]
	}
	fmt.Printf("api_key: %s\n", masked)
	if name == cfg.DefaultDB {
		fmt.Printf("default: yes\n")
	}
	return nil
}
