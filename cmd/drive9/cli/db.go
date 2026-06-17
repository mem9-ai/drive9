package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

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
	regionCodeFlag := ""
	regionCodeGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	asJSON := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, createUsage())
			return nil
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
		case "--region-code":
			if i+1 >= len(args) {
				return fmt.Errorf("--region-code requires an argument")
			}
			i++
			regionCodeFlag = args[i]
			regionCodeGiven = true
		case "--tidbcloud-public-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			i++
			publicKeyFlag = args[i]
			publicKeyGiven = true
		case "--tidbcloud-private-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			i++
			privateKeyFlag = args[i]
			privateKeyGiven = true
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], createUsage())
		}
	}

	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverFlagGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(regionCodeFlag), regionCodeGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return err
	}

	// Precedence here is: explicit --server flag > active context.server >
	// DRIVE9_SERVER env > config.server/default. ResolveCredentials consumes the
	// env var (Unsetenv mitigation), so even when the flag wins we still sink the
	// env to keep downstream forks from inheriting it. Credential resolution is
	// not used here — provisioning is unauthenticated.
	r := ResolveCredentials()
	envPublicKey := consumeEnv(EnvTiDBCloudPublicKey)
	envPrivateKey := consumeEnv(EnvTiDBCloudPrivateKey)
	publicKey := publicKeyFlag
	if publicKey == "" {
		publicKey = envPublicKey
	}
	privateKey := privateKeyFlag
	if privateKey == "" {
		privateKey = envPrivateKey
	}
	regionCode := ""
	if strings.TrimSpace(serverFlag) == "" {
		regionCode = strings.TrimSpace(regionCodeFlag)
		if regionCode == "" {
			regionCode = readTrimEnv(EnvRegionCode)
		}
	}
	publicKey = strings.TrimSpace(publicKey)
	privateKey = strings.TrimSpace(privateKey)

	mode := provisionModeForCredentials(publicKey, privateKey)
	if mode == RegionModeTiDBCloudNative && (publicKey == "" || privateKey == "") {
		return fmt.Errorf("tidb_cloud_native create requires --tidbcloud-public-key and --tidbcloud-private-key, or %s/%s", EnvTiDBCloudPublicKey, EnvTiDBCloudPrivateKey)
	}
	server, regionEntry, err := resolveProvisionServer(serverFlag, regionCode, mode, r.Server)
	if err != nil {
		return err
	}

	cfg := loadConfig()

	if name == "" {
		name = randomName()
	}

	if _, exists := cfg.Contexts[name]; exists {
		return fmt.Errorf("context %q already exists; use a different name", name)
	}

	body, err := provisionRequestBody(publicKey, privateKey)
	if err != nil {
		return err
	}
	c := client.New(server, "")
	resp, err := c.RawPost("/v1/provision", body)
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

	var result createResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	ctx := &Context{
		Type:   PrincipalOwner,
		Server: server,
		APIKey: result.APIKey,
		Mode:   regionModeLabel(mode),
	}
	if mode == RegionModeTiDBCloudNative {
		ctx.CloudProvider = strings.TrimSpace(result.CloudProvider)
		ctx.Region = strings.TrimSpace(result.Region)
	}
	if _, err := ctxAdd(cfg, name, ctx); err != nil {
		return err
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	if asJSON {
		out := createOutput{
			Context:       name,
			TenantID:      result.TenantID,
			APIKey:        result.APIKey,
			Status:        result.Status,
			Server:        server,
			RegionCode:    regionCode,
			Mode:          regionModeLabel(mode),
			CloudProvider: ctx.CloudProvider,
			Region:        ctx.Region,
			Config:        configPath(),
		}
		if regionEntry != nil {
			out.RegionCode = strings.TrimSpace(regionEntry.RegionCode)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("created %q (tenant: %s, status: %s)\n", name, result.TenantID, result.Status)
	if cfg.CurrentContext == name {
		fmt.Printf("switched to context %q\n", name)
	}
	fmt.Printf("config: %s\n", configPath())
	return nil
}

const (
	RegionModeTiDBCloudNative  = "tidb_cloud_native"
	RegionModeTiDBCloudStarter = "tidb_cloud_starter"
)

type createResult struct {
	TenantID      string `json:"tenant_id"`
	APIKey        string `json:"api_key"`
	Status        string `json:"status"`
	CloudProvider string `json:"cloud_provider"`
	Region        string `json:"region"`
}

type createOutput struct {
	Context       string `json:"context"`
	TenantID      string `json:"tenant_id"`
	APIKey        string `json:"api_key"`
	Status        string `json:"status"`
	Server        string `json:"server"`
	RegionCode    string `json:"region_code,omitempty"`
	Mode          string `json:"mode,omitempty"`
	CloudProvider string `json:"cloud_provider,omitempty"`
	Region        string `json:"region,omitempty"`
	Config        string `json:"config"`
}

func createUsage() string {
	return "usage: drive9 create [--name NAME] [--region-code CODE] [--server URL] [--tidbcloud-public-key KEY] [--tidbcloud-private-key KEY] [--json]"
}

func provisionModeForCredentials(publicKey, privateKey string) string {
	if strings.TrimSpace(publicKey) != "" || strings.TrimSpace(privateKey) != "" {
		return RegionModeTiDBCloudNative
	}
	return RegionModeTiDBCloudStarter
}

func provisionRequestBody(publicKey, privateKey string) (io.Reader, error) {
	if publicKey == "" && privateKey == "" {
		return nil, nil
	}
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("tidb_cloud_native create requires both public and private keys")
	}
	body := map[string]string{
		"public_key":  publicKey,
		"private_key": privateKey,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(raw), nil
}

func resolveProvisionServer(serverFlag, regionCode, mode, fallbackServer string) (string, *RegionManifestEntry, error) {
	if strings.TrimSpace(serverFlag) != "" {
		return strings.TrimSpace(serverFlag), nil, nil
	}
	if strings.TrimSpace(regionCode) == "" {
		return fallbackServer, nil, nil
	}
	manifest, err := fetchRegionManifest(context.Background(), regionManifestURL())
	if err != nil {
		return "", nil, err
	}
	entry, err := selectRegionServer(manifest.Regions, regionCode, mode)
	if err != nil {
		return "", nil, err
	}
	return strings.TrimSpace(entry.ServerURL), entry, nil
}

func selectRegionServer(entries []RegionManifestEntry, regionCode, mode string) (*RegionManifestEntry, error) {
	regionCode = strings.TrimSpace(regionCode)
	mode = strings.TrimSpace(mode)
	var matches []RegionManifestEntry
	for _, entry := range entries {
		if strings.TrimSpace(entry.RegionCode) == regionCode && strings.TrimSpace(entry.Mode) == mode {
			matches = append(matches, entry)
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("region %q does not support mode %q; run `drive9 region list`", regionCode, mode)
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("region %q mode %q matches multiple servers; pass --server explicitly", regionCode, mode)
	}
	return &matches[0], nil
}

func DeleteTenant(args []string) error {
	serverFlag := ""
	serverFlagGiven := false
	apiKeyFlag := ""
	apiKeyGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	asJSON := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, deleteUsage())
			return nil
		case "--server":
			if i+1 >= len(args) {
				return fmt.Errorf("--server requires an argument")
			}
			i++
			serverFlag = args[i]
			serverFlagGiven = true
		case "--api-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--api-key requires an argument")
			}
			i++
			apiKeyFlag = args[i]
			apiKeyGiven = true
		case "--tidbcloud-public-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			i++
			publicKeyFlag = args[i]
			publicKeyGiven = true
		case "--tidbcloud-private-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			i++
			privateKeyFlag = args[i]
			privateKeyGiven = true
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], deleteUsage())
		}
	}

	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverFlagGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("api-key", strings.TrimSpace(apiKeyFlag), apiKeyGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return err
	}

	r := ResolveCredentials()
	server := strings.TrimSpace(serverFlag)
	if server == "" {
		server = r.Server
	}
	apiKey := strings.TrimSpace(apiKeyFlag)
	if apiKey == "" && r.Kind == CredentialOwner {
		apiKey = r.APIKey
	}
	if apiKey == "" {
		return fmt.Errorf("delete requires an owner API key; pass --api-key or set %s", EnvAPIKey)
	}
	envPublicKey := consumeEnv(EnvTiDBCloudPublicKey)
	envPrivateKey := consumeEnv(EnvTiDBCloudPrivateKey)
	publicKey := strings.TrimSpace(publicKeyFlag)
	if publicKey == "" {
		publicKey = envPublicKey
	}
	privateKey := strings.TrimSpace(privateKeyFlag)
	if privateKey == "" {
		privateKey = envPrivateKey
	}
	publicKey = strings.TrimSpace(publicKey)
	privateKey = strings.TrimSpace(privateKey)
	body, err := deprovisionRequestBody(publicKey, privateKey)
	if err != nil {
		return err
	}

	c := client.New(server, apiKey)
	resp, err := c.RawDelete("/v1/tenant", body)
	if err != nil {
		return fmt.Errorf("delete tenant failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read delete tenant response: %w", err)
	}
	var result struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	_ = json.Unmarshal(rawResp, &result)
	if resp.StatusCode != http.StatusAccepted {
		msg := strings.TrimSpace(result.Error)
		if msg == "" {
			msg = strings.TrimSpace(string(rawResp))
		}
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return fmt.Errorf("delete tenant failed (HTTP %d): %s", resp.StatusCode, msg)
	}
	if result.Status == "" {
		result.Status = "deleting"
	}
	cleanupMatchingOwnerContexts(server, apiKey)

	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]string{
			"status": result.Status,
			"server": server,
		})
	}
	fmt.Printf("delete accepted (status: %s)\n", result.Status)
	return nil
}

func cleanupMatchingOwnerContexts(server, apiKey string) {
	cfg := loadConfig()
	removed := false
	for name, ctx := range cfg.Contexts {
		if ctx.Type == PrincipalOwner && ctx.Server == server && ctx.APIKey == apiKey {
			delete(cfg.Contexts, name)
			if cfg.CurrentContext == name {
				cfg.CurrentContext = ""
			}
			removed = true
		}
	}
	if !removed {
		return
	}
	if err := saveConfig(cfg); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: failed to save config after context cleanup: %v\n", err)
	}
}

func deleteUsage() string {
	return "usage: drive9 delete [--server URL] [--api-key KEY] [--tidbcloud-public-key KEY] [--tidbcloud-private-key KEY] [--json]"
}

func deprovisionRequestBody(publicKey, privateKey string) (io.Reader, error) {
	if publicKey == "" && privateKey == "" {
		return nil, nil
	}
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("tidb_cloud_native delete requires both public and private keys")
	}
	raw, err := json.Marshal(map[string]string{
		"public_key":  publicKey,
		"private_key": privateKey,
	})
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(raw), nil
}
