package cli

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/mem9-ai/dat9/pkg/client"
)

// Create provisions a new tenant and registers the returned API key as a
// local owner context. Both steps are performed from a single Go code path:
// Create calls ctxAdd(name, &Context{...}) after provisioning, which is the
// same helper `drive9 ctx add` calls. There is one writer for
// ~/.drive9/config — not a sub-command spawn, not a cmd re-entry.
//
// See migration call-out #4 in the impl PR body.
func Create(args []string) error {
	name := ""
	serverFlag := ""
	serverFlagGiven := false

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
			serverFlag = args[i]
			serverFlagGiven = true
		default:
			return fmt.Errorf("unknown flag %q\nusage: drive9 create [--name NAME] [--server URL]", args[i])
		}
	}

	if err := rejectEmptyFlag("server", serverFlag, serverFlagGiven); err != nil {
		return err
	}

	// Precedence per §14.2: explicit --server flag > DRIVE9_SERVER env > config.
	// ResolveCredentials consumes the env var (Unsetenv mitigation), so even when
	// the flag wins we still sink the env to keep downstream forks from inheriting
	// it. Credential resolution is not used here — provisioning is unauthenticated.
	r := ResolveCredentials()
	server := serverFlag
	if server == "" {
		server = r.Server
	}

	cfg := loadConfig()

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

	if _, err := ctxAdd(cfg, name, &Context{
		Type:   PrincipalOwner,
		Server: server,
		APIKey: result.APIKey,
	}); err != nil {
		return err
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
