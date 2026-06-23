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

	server, err := quotaServer(serverFlag, regionCodeFlag, r.Server, true)
	if err != nil {
		return err
	}
	req, err := quotaRequest(tenantID, publicKey, privateKey)
	if err != nil {
		return err
	}
	out, err := client.New(server, "").GetQuota(context.Background(), req)
	if err != nil {
		return quotaAPIError("query quota", err)
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
	var maxStorageSize *int64
	var maxFileSize *int64
	var maxFileCount *int64
	var tidbCloudSpendingLimit *int64
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
		case "--max-storage-size":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-storage-size requires an argument")
			}
			i++
			v, err := parsePositiveQuotaInt64Flag("--max-storage-size", args[i])
			if err != nil {
				return err
			}
			maxStorageSize = &v
		case "--max-file-size":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-file-size requires an argument")
			}
			i++
			v, err := parsePositiveQuotaInt64Flag("--max-file-size", args[i])
			if err != nil {
				return err
			}
			maxFileSize = &v
		case "--max-file-count":
			if i+1 >= len(args) {
				return fmt.Errorf("--max-file-count requires an argument")
			}
			i++
			v, err := parseNonNegativeQuotaInt64Flag("--max-file-count", args[i])
			if err != nil {
				return err
			}
			maxFileCount = &v
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
	if maxStorageSize == nil && maxFileSize == nil && maxFileCount == nil && tidbCloudSpendingLimit == nil {
		return fmt.Errorf("quota set requires --max-storage-size, --max-file-size, --max-file-count, or --tidbcloud-spending-limit")
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
	cred, err := quotaRequest(strings.TrimSpace(tenantID), publicKey, privateKey)
	if err != nil {
		return err
	}
	out, err := client.New(server, "").SetQuota(context.Background(), client.QuotaSetRequest{
		TenantID:               cred.TenantID,
		PublicKey:              cred.PublicKey,
		PrivateKey:             cred.PrivateKey,
		MaxStorageSize:         maxStorageSize,
		MaxFileSize:            maxFileSize,
		MaxFileCount:           maxFileCount,
		TiDBCloudSpendingLimit: tidbCloudSpendingLimit,
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

func quotaRequest(tenantID, publicKey, privateKey string) (client.QuotaRequest, error) {
	tenantID = strings.TrimSpace(tenantID)
	publicKey = strings.TrimSpace(publicKey)
	privateKey = strings.TrimSpace(privateKey)
	if tenantID == "" {
		return client.QuotaRequest{}, fmt.Errorf("--tenant-id is required")
	}
	if publicKey == "" || privateKey == "" {
		return client.QuotaRequest{}, fmt.Errorf("TiDB Cloud credentials are required; pass --tidbcloud-public-key and --tidbcloud-private-key or set %s/%s", EnvTiDBCloudPublicKey, EnvTiDBCloudPrivateKey)
	}
	return client.QuotaRequest{
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

func parsePositiveQuotaInt64Flag(name, raw string) (int64, error) {
	value, err := parseQuotaInt64Flag(name, raw)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return value, nil
}

func parseNonNegativeQuotaInt64Flag(name, raw string) (int64, error) {
	value, err := parseQuotaInt64Flag(name, raw)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must be non-negative", name)
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
	configParts := []string{
		fmt.Sprintf("max_storage_size=%dMi", out.Config.MaxStorageSize),
		fmt.Sprintf("max_file_size=%dMi", out.Config.MaxFileSize),
		fmt.Sprintf("max_file_count=%d", out.Config.MaxFileCount),
	}
	if out.Config.TiDBCloudSpendingLimit != nil {
		configParts = append(configParts, fmt.Sprintf("tidbcloud_spending_limit=%d", *out.Config.TiDBCloudSpendingLimit))
	}
	fmt.Printf("config: %s\n", strings.Join(configParts, " "))
	fmt.Printf("usage: storage_bytes=%d reserved_bytes=%d file_count=%d\n", out.Usage.StorageBytes, out.Usage.ReservedBytes, out.Usage.FileCount)
	return nil
}

func quotaUsage() string {
	return `usage: drive9 quota <get|set> [flags]

query or set tenant quota.

commands:
  get    query TiDBCloud mode quota with TiDB Cloud credentials and tenant id
  set    set TiDBCloud mode quota with TiDB Cloud credentials and tenant id`
}

func quotaGetUsage() string {
	return `usage: drive9 quota get [flags]

query TiDBCloud mode quota with TiDB Cloud credentials and a drive9 tenant id.
Drive9 tenant API keys are not accepted for quota reads.

flags:
  --server URL                    server URL (default: active context server)
  --region-code CODE              TiDBCloud mode region code; ignored when --server is set
  --tenant-id ID                  drive9 tenant id for quota query
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
  --max-storage-size Mi           max confirmed+reserved storage size in Mi
  --max-file-size Mi              max single file size in Mi; must not exceed server DRIVE9_MAX_UPLOAD_BYTES
  --max-file-count N              max confirmed file count; 0 means unlimited
  --tidbcloud-spending-limit N    TiDB Cloud Cluster Spending Limit; must be non-negative; see https://docs.pingcap.com/tidbcloud/manage-serverless-spend-limit
  --json                          output result as JSON`
}
