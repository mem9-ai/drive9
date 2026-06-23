package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Quota queries or updates tenant quota configuration.
func Quota(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", quotaUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, quotaUsage())
		return nil
	case "get":
		return quotaGet(args[1:])
	case "set":
		return quotaSet(args[1:])
	default:
		return fmt.Errorf("unknown quota command %q\n%s", args[0], quotaUsage())
	}
}

func quotaGet(args []string) error {
	serverFlag := ""
	serverGiven := false
	regionCodeFlag := ""
	regionCodeGiven := false
	apiKeyFlag := ""
	apiKeyGiven := false
	tenantID := ""
	tenantIDGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	asJSON := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, quotaGetUsage())
			return nil
		case "--server":
			if i+1 >= len(args) {
				return fmt.Errorf("--server requires an argument")
			}
			i++
			serverFlag = args[i]
			serverGiven = true
		case "--region-code":
			if i+1 >= len(args) {
				return fmt.Errorf("--region-code requires an argument")
			}
			i++
			regionCodeFlag = args[i]
			regionCodeGiven = true
		case "--api-key":
			if i+1 >= len(args) {
				return fmt.Errorf("--api-key requires an argument")
			}
			i++
			apiKeyFlag = args[i]
			apiKeyGiven = true
		case "--tenant-id":
			if i+1 >= len(args) {
				return fmt.Errorf("--tenant-id requires an argument")
			}
			i++
			tenantID = args[i]
			tenantIDGiven = true
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
			return fmt.Errorf("unknown flag %q\n%s", args[i], quotaGetUsage())
		}
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(regionCodeFlag), regionCodeGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("api-key", strings.TrimSpace(apiKeyFlag), apiKeyGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tenant-id", strings.TrimSpace(tenantID), tenantIDGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return err
	}

	r := ResolveCredentials()
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
	tenantID = strings.TrimSpace(tenantID)

	var out *client.QuotaResponse
	if tenantIDGiven {
		server, err := quotaServer(serverFlag, regionCodeFlag, r.Server, true)
		if err != nil {
			return err
		}
		req, err := quotaCredentialRequest(tenantID, publicKey, privateKey)
		if err != nil {
			return err
		}
		out, err = client.New(server, "").QueryQuotaWithCredentials(context.Background(), req)
		if err != nil {
			return quotaAPIError("query quota", err)
		}
	} else {
		if publicKeyGiven || privateKeyGiven {
			return fmt.Errorf("--tenant-id is required when using TiDB Cloud credentials")
		}
		server, err := quotaServer(serverFlag, regionCodeFlag, r.Server, false)
		if err != nil {
			return err
		}
		apiKey := strings.TrimSpace(apiKeyFlag)
		if apiKey == "" && r.Kind == CredentialOwner {
			apiKey = r.APIKey
		}
		if apiKey == "" {
			return fmt.Errorf("quota get requires an owner API key, or --tenant-id with TiDB Cloud credentials")
		}
		out, err = client.New(server, apiKey).GetQuota(context.Background())
		if err != nil {
			return quotaAPIError("query quota", err)
		}
	}
	return printQuotaCLIResponse(out, asJSON)
}

func quotaSet(args []string) error {
	serverFlag := ""
	serverGiven := false
	regionCodeFlag := ""
	regionCodeGiven := false
	tenantID := ""
	tenantIDGiven := false
	publicKeyFlag := ""
	publicKeyGiven := false
	privateKeyFlag := ""
	privateKeyGiven := false
	var maxStorageBytes *int64
	var maxMediaLLMFiles *int64
	var maxMonthlyCostMC *int64
	asJSON := false

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, quotaSetUsage())
			return nil
		case "--server":
			if i+1 >= len(args) {
				return fmt.Errorf("--server requires an argument")
			}
			i++
			serverFlag = args[i]
			serverGiven = true
		case "--region-code":
			if i+1 >= len(args) {
				return fmt.Errorf("--region-code requires an argument")
			}
			i++
			regionCodeFlag = args[i]
			regionCodeGiven = true
		case "--tenant-id":
			if i+1 >= len(args) {
				return fmt.Errorf("--tenant-id requires an argument")
			}
			i++
			tenantID = args[i]
			tenantIDGiven = true
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
		case "--max-storage-bytes":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-storage-bytes requires an argument")
			}
			i++
			v, err := parseQuotaInt64Flag("--max-storage-bytes", args[i])
			if err != nil {
				return err
			}
			maxStorageBytes = &v
		case "--max-media-llm-files":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-media-llm-files requires an argument")
			}
			i++
			v, err := parseQuotaInt64Flag("--max-media-llm-files", args[i])
			if err != nil {
				return err
			}
			maxMediaLLMFiles = &v
		case "--max-monthly-cost-mc":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-monthly-cost-mc requires an argument")
			}
			i++
			v, err := parseQuotaInt64Flag("--max-monthly-cost-mc", args[i])
			if err != nil {
				return err
			}
			maxMonthlyCostMC = &v
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("unknown flag %q\n%s", args[i], quotaSetUsage())
		}
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(regionCodeFlag), regionCodeGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tenant-id", strings.TrimSpace(tenantID), tenantIDGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return err
	}
	if maxStorageBytes == nil && maxMediaLLMFiles == nil && maxMonthlyCostMC == nil {
		return fmt.Errorf("quota set requires at least one quota flag")
	}

	r := ResolveCredentials()
	envPublicKey := consumeEnv(EnvTiDBCloudPublicKey)
	envPrivateKey := consumeEnv(EnvTiDBCloudPrivateKey)
	server, err := quotaServer(serverFlag, regionCodeFlag, r.Server, true)
	if err != nil {
		return err
	}
	publicKey := strings.TrimSpace(publicKeyFlag)
	if publicKey == "" {
		publicKey = envPublicKey
	}
	privateKey := strings.TrimSpace(privateKeyFlag)
	if privateKey == "" {
		privateKey = envPrivateKey
	}
	cred, err := quotaCredentialRequest(strings.TrimSpace(tenantID), publicKey, privateKey)
	if err != nil {
		return err
	}
	out, err := client.New(server, "").SetQuotaWithCredentials(context.Background(), client.QuotaSetRequest{
		TenantID:         cred.TenantID,
		PublicKey:        cred.PublicKey,
		PrivateKey:       cred.PrivateKey,
		MaxStorageBytes:  maxStorageBytes,
		MaxMediaLLMFiles: maxMediaLLMFiles,
		MaxMonthlyCostMC: maxMonthlyCostMC,
	})
	if err != nil {
		return quotaAPIError("set quota", err)
	}
	return printQuotaCLIResponse(out, asJSON)
}

func quotaServer(serverFlag, regionCodeFlag, fallbackServer string, includeEnvRegion bool) (string, error) {
	regionCode := strings.TrimSpace(regionCodeFlag)
	if strings.TrimSpace(serverFlag) == "" && regionCode == "" && includeEnvRegion {
		regionCode = readTrimEnv(EnvRegionCode)
	}
	server, _, err := resolveProvisionServer(serverFlag, regionCode, RegionModeTiDBCloudNative, fallbackServer)
	if err != nil {
		return "", err
	}
	return server, nil
}

func quotaAPIError(action string, err error) error {
	var statusErr *client.StatusError
	if errors.As(err, &statusErr) {
		msg := strings.TrimSpace(statusErr.Message)
		if msg == "" {
			msg = http.StatusText(statusErr.StatusCode)
		}
		if msg == "" {
			msg = fmt.Sprintf("HTTP %d", statusErr.StatusCode)
		}
		return fmt.Errorf("%s failed (HTTP %d): %s", action, statusErr.StatusCode, msg)
	}
	return fmt.Errorf("%s failed: %w", action, err)
}

func quotaCredentialRequest(tenantID, publicKey, privateKey string) (client.QuotaCredentialRequest, error) {
	tenantID = strings.TrimSpace(tenantID)
	publicKey = strings.TrimSpace(publicKey)
	privateKey = strings.TrimSpace(privateKey)
	if tenantID == "" {
		return client.QuotaCredentialRequest{}, fmt.Errorf("--tenant-id is required")
	}
	if publicKey == "" || privateKey == "" {
		return client.QuotaCredentialRequest{}, fmt.Errorf("TiDB Cloud credentials are required; pass --tidbcloud-public-key and --tidbcloud-private-key or set %s/%s", EnvTiDBCloudPublicKey, EnvTiDBCloudPrivateKey)
	}
	return client.QuotaCredentialRequest{
		TenantID:   tenantID,
		PublicKey:  publicKey,
		PrivateKey: privateKey,
	}, nil
}

func parseQuotaInt64Flag(name, raw string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
}

func printQuotaCLIResponse(out *client.QuotaResponse, asJSON bool) error {
	if out == nil {
		return fmt.Errorf("quota response is empty")
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	fmt.Printf("tenant: %s\n", out.TenantID)
	fmt.Printf("provider: %s\n", out.Provider)
	fmt.Printf("status: %s\n", out.Status)
	fmt.Printf("supports_update: %t\n", out.SupportsUpdate)
	fmt.Printf("config: max_storage_bytes=%d max_media_llm_files=%d max_monthly_cost_mc=%d\n",
		out.Config.MaxStorageBytes, out.Config.MaxMediaLLMFiles, out.Config.MaxMonthlyCostMC)
	fmt.Printf("usage: storage_bytes=%d reserved_bytes=%d media_file_count=%d monthly_cost_mc=%d\n",
		out.Usage.StorageBytes, out.Usage.ReservedBytes, out.Usage.MediaFileCount, out.Usage.MonthlyCostMC)
	return nil
}

func quotaUsage() string {
	return `usage: drive9 quota <get|set> [flags]

query or set tenant quota.

commands:
  get    query quota with an owner API key, or with TiDB Cloud credentials and tenant id
  set    set TiDBCloud mode quota with TiDB Cloud credentials and tenant id`
}

func quotaGetUsage() string {
	return `usage: drive9 quota get [flags]

query quota. Without --tenant-id, uses the active owner API key. With
--tenant-id, uses TiDB Cloud credentials and does not require a Drive9 API key.

flags:
  --server URL                    server URL (default: active context server)
  --region-code CODE              TiDBCloud mode region code; ignored when --server is set
  --api-key KEY                   owner API key for current-tenant query
  --tenant-id ID                  drive9 tenant id for TiDB Cloud credential query
  --tidbcloud-public-key KEY      TiDB Cloud public key
  --tidbcloud-private-key KEY     TiDB Cloud private key
  --json                          output result as JSON`
}

func quotaSetUsage() string {
	return `usage: drive9 quota set [flags]

set quota for TiDBCloud mode tenants only. This requires TiDB Cloud credentials
and a drive9 tenant id. Drive9 tenant API keys are not accepted as
authorization for quota updates.

flags:
  --server URL                    server URL (default: active context server)
  --region-code CODE              TiDBCloud mode region code; ignored when --server is set
  --tenant-id ID                  drive9 tenant id
  --tidbcloud-public-key KEY      TiDB Cloud public key
  --tidbcloud-private-key KEY     TiDB Cloud private key
  --max-storage-bytes BYTES       max confirmed+reserved storage bytes
  --max-media-llm-files N         max media files eligible for LLM processing
  --max-monthly-cost-mc N         max monthly LLM cost in millicents; 0 disables
  --json                          output result as JSON`
}
