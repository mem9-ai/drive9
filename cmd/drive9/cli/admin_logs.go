package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// The server owns these bounds; the CLI mirrors them so an over-limit flag
// fails locally instead of costing a round-trip.
const (
	adminLogMaxTimeRange = 30 * time.Minute
	adminLogMaxLimit     = 1000
)

// adminTenantLogs implements `drive9 admin tenant logs`, which queries one
// tenant's logs through the admin API.
func adminTenantLogs(args []string) error {
	req, server, asJSON, err := parseAdminTenantLogsCommand(args)
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	out, err := client.New(server, "").AdminGetTenantLogs(context.Background(), req)
	if err != nil {
		return quotaAPIError("query tenant logs", err)
	}
	if out == nil {
		return fmt.Errorf("admin tenant logs response is empty")
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	return printAdminTenantLogs(out)
}

func parseAdminTenantLogsCommand(args []string) (client.AdminTenantLogsRequest, string, bool, error) {
	usage := adminTenantLogsUsage()
	var (
		serverFlag      string
		serverGiven     bool
		regionCodeFlag  string
		regionCodeGiven bool
		publicKeyFlag   string
		publicKeyGiven  bool
		privateKeyFlag  string
		privateKeyGiven bool
		tenantID        string
		tenantGiven     bool
		keyword         string
		timeRange       string
		limit           int
		asJSON          bool
	)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, usage)
			return client.AdminTenantLogsRequest{}, "", false, errHelpRequested{}
		case "--server":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--server requires an argument")
			}
			i++
			serverFlag = args[i]
			serverGiven = true
		case "--region-code":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--region-code requires an argument")
			}
			i++
			regionCodeFlag = args[i]
			regionCodeGiven = true
		case "--tenant-id":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--tenant-id requires an argument")
			}
			i++
			tenantID = args[i]
			tenantGiven = true
		case "--tidbcloud-public-key":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--tidbcloud-public-key requires an argument")
			}
			i++
			publicKeyFlag = args[i]
			publicKeyGiven = true
		case "--tidbcloud-private-key":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--tidbcloud-private-key requires an argument")
			}
			i++
			privateKeyFlag = args[i]
			privateKeyGiven = true
		case "--keyword":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--keyword requires an argument")
			}
			i++
			keyword = args[i]
		case "--time-range":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--time-range requires an argument")
			}
			i++
			timeRange = strings.TrimSpace(args[i])
			duration, err := time.ParseDuration(timeRange)
			if err != nil {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--time-range %q is invalid: use a duration such as 20m", args[i])
			}
			if duration <= 0 {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--time-range must be positive")
			}
			if duration > adminLogMaxTimeRange {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--time-range must be at most %s", adminLogMaxTimeRange)
			}
		case "--limit":
			if i+1 >= len(args) {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--limit requires an argument")
			}
			i++
			v, err := strconv.Atoi(strings.TrimSpace(args[i]))
			if err != nil || v <= 0 {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--limit must be a positive integer")
			}
			if v > adminLogMaxLimit {
				return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("--limit must be at most %d", adminLogMaxLimit)
			}
			limit = v
		case "--json":
			asJSON = true
		default:
			return client.AdminTenantLogsRequest{}, "", false, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
		}
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(serverFlag), serverGiven); err != nil {
		return client.AdminTenantLogsRequest{}, "", false, err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(regionCodeFlag), regionCodeGiven); err != nil {
		return client.AdminTenantLogsRequest{}, "", false, err
	}
	if err := rejectEmptyFlag("tenant-id", strings.TrimSpace(tenantID), tenantGiven); err != nil {
		return client.AdminTenantLogsRequest{}, "", false, err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(publicKeyFlag), publicKeyGiven); err != nil {
		return client.AdminTenantLogsRequest{}, "", false, err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(privateKeyFlag), privateKeyGiven); err != nil {
		return client.AdminTenantLogsRequest{}, "", false, err
	}
	r := ResolveCredentials()
	server, err := quotaServer(serverFlag, regionCodeFlag, r.Server, true)
	if err != nil {
		return client.AdminTenantLogsRequest{}, "", false, err
	}
	publicKey, privateKey := adminTiDBCloudKeys(publicKeyFlag, privateKeyFlag)
	req, err := quotaRequest(strings.TrimSpace(tenantID), publicKey, privateKey)
	if err != nil {
		return client.AdminTenantLogsRequest{}, "", false, err
	}
	return client.AdminTenantLogsRequest{
		TenantID:   req.TenantID,
		PublicKey:  req.PublicKey,
		PrivateKey: req.PrivateKey,
		Keyword:    keyword,
		TimeRange:  timeRange,
		Limit:      limit,
	}, server, asJSON, nil
}

func printAdminTenantLogs(out *client.AdminTenantLogsResponse) error {
	if out.Count == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "no log entries for tenant %s in the last %s\n", out.TenantID, out.TimeRange)
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TIMESTAMP\tLINE")
	for _, entry := range out.Entries {
		_, _ = fmt.Fprintf(w, "%s\t%s\n", entry.Timestamp.UTC().Format(time.RFC3339Nano), entry.Line)
	}
	return w.Flush()
}

func adminTenantLogsUsage() string {
	return `usage: drive9 admin tenant logs --tenant-id ID [flags]

query one tenant's logs, newest first. The server scopes the query to this
deployment's environment, the API key's organization and the tenant itself, so
callers only choose the tenant, the keyword and the time window.

flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               TiDBCloud Mode region code; ignored when --server is set
  --tenant-id ID                   drive9 tenant id
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --keyword TEXT                   only return lines containing this text
  --time-range DURATION            lookback window such as 20m (default: 5m, max: 30m)
  --limit N                        maximum entries to return (default: 100, max: 1000)
  --json                           output the raw response as JSON

examples:
  drive9 admin tenant logs --tenant-id <tenant-id> --time-range 20m \
    --keyword "connection timed out" \
    --tidbcloud-public-key <public-key> \
    --tidbcloud-private-key <private-key>`
}
