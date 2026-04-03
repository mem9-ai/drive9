package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"

	"github.com/mem9-ai/dat9/pkg/client"
)

func Create(args []string) error {
	name := ""
	server := os.Getenv("DAT9_SERVER")

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--name":
			if i+1 >= len(args) {
				return fmt.Errorf("--name requires an argument")
			}
			i++
			name = args[i]
		case "--server":
			if i+1 >= len(args) {
				return fmt.Errorf("--server requires an argument")
			}
			i++
			server = args[i]
		default:
			return fmt.Errorf("unknown flag %q\nusage: dat9 create [--name NAME] [--server URL]", args[i])
		}
	}

	cfg := loadConfig()

	if server == "" {
		server = cfg.ResolveServer()
	}

	if name == "" {
		name = randomName()
	}

	if _, exists := cfg.Contexts[name]; exists {
		return fmt.Errorf("context %q already exists; use a different name", name)
	}

	c := client.New(server, "")
	resp, err := c.RawPost("/v1/provision", nil)
	if err != nil {
		return fmt.Errorf("provision failed: %w", err)
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
		Status   string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if cfg.Server == "" {
		cfg.Server = server
	}
	cfg.Contexts[name] = &Context{APIKey: result.APIKey}
	if cfg.CurrentContext == "" {
		cfg.CurrentContext = name
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	fmt.Printf("created %q (tenant: %s, status: %s)\n", name, result.TenantID, result.Status)
	if cfg.CurrentContext == name {
		fmt.Printf("switched to context %q\n", name)
	}
	fmt.Printf("config: %s\n", configPath())
	return nil
}

func Ctx(args []string) error {
	if len(args) == 0 {
		return ctxShow()
	}
	switch args[0] {
	case "list", "ls":
		return ctxList()
	default:
		return ctxSwitch(args[0])
	}
}

func ctxShow() error {
	cfg := loadConfig()
	if cfg.CurrentContext == "" {
		fmt.Println("no current context")
		return nil
	}
	fmt.Println(cfg.CurrentContext)
	return nil
}

func ctxList() error {
	cfg := loadConfig()
	if len(cfg.Contexts) == 0 {
		fmt.Println("no contexts configured")
		fmt.Println("run: dat9 create --name <name>")
		return nil
	}
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		marker := "  "
		if name == cfg.CurrentContext {
			marker = "* "
		}
		ctx := cfg.Contexts[name]
		masked := ctx.APIKey
		if len(masked) > 12 {
			masked = masked[:8] + "..." + masked[len(masked)-4:]
		}
		fmt.Printf("%s%s  (key=%s)\n", marker, name, masked)
	}
	return nil
}

func ctxSwitch(name string) error {
	cfg := loadConfig()
	if _, ok := cfg.Contexts[name]; !ok {
		return fmt.Errorf("context %q not found; run: dat9 ctx list", name)
	}
	cfg.CurrentContext = name
	if err := saveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("switched to context %q\n", name)
	return nil
}
