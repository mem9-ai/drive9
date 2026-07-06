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

	"github.com/mem9-ai/drive9/pkg/client"
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
	var tidbCloudSpendingLimit *int64
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
		case "--tidbcloud-spending-limit":
			if i+1 >= len(args) {
				return fmt.Errorf("--tidbcloud-spending-limit requires an argument")
			}
			i++
			v, err := parseNonNegativeQuotaInt64Flag("--tidbcloud-spending-limit", args[i])
			if err != nil {
				return err
			}
			tidbCloudSpendingLimit = &v
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
	if tidbCloudSpendingLimit != nil && mode != RegionModeTiDBCloudNative {
		return fmt.Errorf("TiDBCloud Mode requires --tidbcloud-public-key and --tidbcloud-private-key when --tidbcloud-spending-limit is set")
	}
	if mode == RegionModeTiDBCloudNative && (publicKey == "" || privateKey == "") {
		return fmt.Errorf("TiDBCloud Mode requires --tidbcloud-public-key and --tidbcloud-private-key, or %s/%s", EnvTiDBCloudPublicKey, EnvTiDBCloudPrivateKey)
	}
	if mode == RegionModeTiDBCloudNative && strings.TrimSpace(serverFlag) == "" && strings.TrimSpace(regionCode) == "" {
		return fmt.Errorf("TiDBCloud Mode requires --region-code or --server; use drive9 region list to see available regions")
	}
	if mode == RegionModeTiDBCloudNative && tidbCloudSpendingLimit == nil {
		defaultSpendingLimit := int64(0)
		tidbCloudSpendingLimit = &defaultSpendingLimit
	}
	server, regionEntry, err := resolveProvisionServer(serverFlag, regionCode, mode, r.Server)
	if err != nil {
		return err
	}

	if mode == RegionModeAnonymous {
		fmt.Fprintf(os.Stderr, "Anonymous mode: a quick-start workspace hosted by TiDB. Your files are tenant-isolated and encrypted in transit. For a workspace tied to your own account (recommended for important data), sign up at drive9.ai. Details: drive9.ai/security\n")
	}

	cfg := loadConfig()

	if name == "" {
		name = randomName()
	}

	if _, exists := cfg.Contexts[name]; exists {
		return fmt.Errorf("context %q already exists; use a different name", name)
	}

	body, err := provisionRequestBody(publicKey, privateKey, tidbCloudSpendingLimit)
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
		return fmt.Errorf("provision failed (HTTP %d): %s", resp.StatusCode, createProvisionErrorHint(errResp.Error))
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
	RegionModeAnonymous       = "anonymous"
	RegionModeStarterLegacy   = "tidb_cloud_starter"
	RegionModeTiDBCloudNative = "tidb_cloud_native"

	ModeLabelAnonymous = "Anonymous"
	ModeLabelTiDBCloud = "TiDBCloud"
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
	return `usage: drive9 create [flags]

provision a new tenant and register the returned API key as a local owner context.

flags:
  --name NAME                     context name (default: auto-generated 7-char name)
  --region-code CODE              provisioning region code; use "drive9 region list" to see available regions
  --server URL                    override the server URL (bypasses region manifest lookup)
  --tidbcloud-public-key KEY      TiDB Cloud public key (required for TiDBCloud Mode)
  --tidbcloud-private-key KEY     TiDB Cloud private key (required for TiDBCloud Mode)
  --tidbcloud-spending-limit N    TiDB Cloud Cluster Spending Limit; must be non-negative (default: 0 in TiDBCloud Mode)
  --json                          output result as JSON

examples:
  # provision an Anonymous tenant using the default region
  drive9 create

  # provision a TiDBCloud tenant in ap-southeast-1
  drive9 create --region-code aws-ap-southeast-1 \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key> \
    --tidbcloud-spending-limit 10000

  # provision directly against a known server
  drive9 create --server http://127.0.0.1:9009

  # list available regions
  drive9 region list

note: Anonymous mode is a quick-start workspace hosted by TiDB — tenant-isolated, encrypted in transit. Sign up at drive9.ai for your own account workspace. Details: drive9.ai/security`
}

func provisionModeForCredentials(publicKey, privateKey string) string {
	if strings.TrimSpace(publicKey) != "" || strings.TrimSpace(privateKey) != "" {
		return RegionModeTiDBCloudNative
	}
	return RegionModeAnonymous
}

func provisionRequestBody(publicKey, privateKey string, tidbCloudSpendingLimit *int64) (io.Reader, error) {
	if publicKey == "" && privateKey == "" && tidbCloudSpendingLimit == nil {
		return nil, nil
	}
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("TiDBCloud Mode requires both public and private keys")
	}
	body := map[string]any{
		"public_key":  publicKey,
		"private_key": privateKey,
	}
	if tidbCloudSpendingLimit != nil {
		body["tidbcloud_spending_limit"] = *tidbCloudSpendingLimit
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(raw), nil
}

func createProvisionErrorHint(msg string) string {
	trimmed := strings.TrimSpace(msg)
	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, "free quota") || strings.Contains(lower, "free cluster") || strings.Contains(lower, "usage quota") {
		if !strings.Contains(lower, "--tidbcloud-spending-limit") {
			return trimmed + "; retry with --tidbcloud-spending-limit N to set a TiDB Cloud Cluster Spending Limit"
		}
	}
	return trimmed
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
	requestedMode := strings.TrimSpace(mode)
	canonicalMode := canonicalRegionMode(mode)
	var matches []RegionManifestEntry
	for _, entry := range entries {
		if strings.TrimSpace(entry.RegionCode) == regionCode && canonicalRegionMode(entry.Mode) == canonicalMode {
			matches = append(matches, entry)
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("region %q does not support mode %q; run `drive9 region list`", regionCode, requestedMode)
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("region %q mode %q matches multiple servers; pass --server explicitly", regionCode, requestedMode)
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
	skipConfirm := false

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
		case "-y", "--yes":
			skipConfirm = true
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
	var bodyBytes []byte
	if body != nil {
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return fmt.Errorf("read request body: %w", err)
		}
	}

	if !skipConfirm {
		if !isTerminal(os.Stdin) {
			return fmt.Errorf("refusing to delete without confirmation in non-interactive mode; pass -y to confirm")
		}
		fmt.Fprintf(os.Stderr, "WARNING: this will permanently delete the current tenant,\n")
		fmt.Fprintf(os.Stderr, "including its TiDB cluster, database, API keys, and all stored files.\n")
		fmt.Fprint(os.Stderr, "Continue? [y/N]: ")
		var answer string
		if _, err := fmt.Fscanln(os.Stdin, &answer); err != nil {
			return fmt.Errorf("delete cancelled")
		}
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "y", "yes":
		default:
			return fmt.Errorf("delete cancelled")
		}
	}

	var bodyReader io.Reader
	if bodyBytes != nil {
		bodyReader = bytes.NewReader(bodyBytes)
	}
	c := client.New(server, apiKey)

	kind, err := tenantKind(c)
	if err != nil {
		return fmt.Errorf("cannot determine tenant kind: %w", err)
	}
	switch kind {
	case "fork":
		return deleteForkAfterLiveRejection(server, apiKey, bodyBytes, asJSON)
	case "live":
	default:
		return fmt.Errorf("unknown tenant kind %q; expected \"fork\" or \"live\"", kind)
	}

	resp, err := c.RawDelete("/v1/tenant", bodyReader)
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
		if ctx == nil {
			continue
		}
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
	return `usage: drive9 delete [flags]

delete the current tenant. The tenant's TiDB Cloud cluster, database, and API
keys are removed. For TiDBCloud Mode, TiDB Cloud credentials must be provided.

flags:
  --server URL                    server URL (default: active context server)
  --api-key KEY                   owner API key (default: active context API key)
  --tidbcloud-public-key KEY      TiDB Cloud public key (required for TiDBCloud Mode)
  --tidbcloud-private-key KEY     TiDB Cloud private key (required for TiDBCloud Mode)
  --json                          output result as JSON
  -y, --yes                       skip confirmation prompt

examples:
  # delete the active context's tenant
  drive9 delete

  # delete a TiDBCloud tenant using explicit credentials
  drive9 delete --server https://api.drive9.ai \
    --api-key drive9_xxx \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}

func deprovisionRequestBody(publicKey, privateKey string) (io.Reader, error) {
	if publicKey == "" && privateKey == "" {
		return nil, nil
	}
	if publicKey == "" || privateKey == "" {
		return nil, fmt.Errorf("TiDBCloud Mode requires both public and private keys for delete")
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

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func tenantKind(c *client.Client) (string, error) {
	resp, err := c.RawGet("/v1/status")
	if err != nil {
		return "", fmt.Errorf("status API request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read status response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status API returned HTTP %d", resp.StatusCode)
	}
	var status struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(raw, &status)
	if status.Kind == "" {
		return "", fmt.Errorf("server does not report tenant kind; upgrade server or contact administrator")
	}
	return status.Kind, nil
}

func deleteForkAfterLiveRejection(server, apiKey string, bodyBytes []byte, asJSON bool) error {
	var bodyReader io.Reader
	if bodyBytes != nil {
		bodyReader = bytes.NewReader(bodyBytes)
	}
	c := client.New(server, apiKey)
	resp, err := c.RawDelete("/v1/fork", bodyReader)
	if err != nil {
		return fmt.Errorf("fork delete failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	rawResp, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read fork delete response: %w", err)
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
		return fmt.Errorf("fork delete failed (HTTP %d): %s", resp.StatusCode, msg)
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
	fmt.Printf("fork delete accepted (status: %s)\n", result.Status)
	return nil
}
