package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/mem9-ai/drive9/pkg/client"
)

func adminTenantEmbeddingConfig(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminTenantEmbeddingConfigUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminTenantEmbeddingConfigUsage())
		return nil
	case "get":
		return adminTenantEmbeddingConfigGet(args[1:])
	case "set":
		return adminTenantEmbeddingConfigSet(args[1:])
	default:
		return fmt.Errorf("unknown admin tenant embedding-config command %q\n%s", args[0], adminTenantEmbeddingConfigUsage())
	}
}

type adminTenantEmbeddingConfigFlags struct {
	serverFlag      string
	serverGiven     bool
	regionCodeFlag  string
	regionCodeGiven bool
	tenantID        string
	tenantIDGiven   bool
	publicKeyFlag   string
	publicKeyGiven  bool
	privateKeyFlag  string
	privateKeyGiven bool
	asJSON          bool
	enabled         *bool
	apiBase         *string
	apiKey          *string
	model           *string
}

func parseAdminTenantEmbeddingConfigFlags(args []string, usage string, allowConfig bool) (adminTenantEmbeddingConfigFlags, error) {
	var flags adminTenantEmbeddingConfigFlags
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "-help", "--help", "help":
			_, _ = fmt.Fprintln(os.Stdout, usage)
			return flags, errHelpRequested{}
		case "--server":
			value, next, err := nextAdminExtractFlag(args, i, "--server")
			if err != nil {
				return flags, err
			}
			flags.serverFlag, flags.serverGiven, i = value, true, next
		case "--region-code":
			value, next, err := nextAdminExtractFlag(args, i, "--region-code")
			if err != nil {
				return flags, err
			}
			flags.regionCodeFlag, flags.regionCodeGiven, i = value, true, next
		case "--tenant-id":
			value, next, err := nextAdminExtractFlag(args, i, "--tenant-id")
			if err != nil {
				return flags, err
			}
			flags.tenantID, flags.tenantIDGiven, i = value, true, next
		case "--tidbcloud-public-key":
			value, next, err := nextAdminExtractFlag(args, i, "--tidbcloud-public-key")
			if err != nil {
				return flags, err
			}
			flags.publicKeyFlag, flags.publicKeyGiven, i = value, true, next
		case "--tidbcloud-private-key":
			value, next, err := nextAdminExtractFlag(args, i, "--tidbcloud-private-key")
			if err != nil {
				return flags, err
			}
			flags.privateKeyFlag, flags.privateKeyGiven, i = value, true, next
		case "--json":
			flags.asJSON = true
		case "--enabled":
			if !allowConfig {
				return flags, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
			}
			value, next, err := nextAdminExtractFlag(args, i, "--enabled")
			if err != nil {
				return flags, err
			}
			parsed, err := parseAdminExtractBool(value)
			if err != nil {
				return flags, err
			}
			flags.enabled, i = &parsed, next
		case "--api-base", "--api-key", "--model":
			if !allowConfig {
				return flags, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
			}
			name := args[i]
			value, next, err := nextAdminExtractFlag(args, i, name)
			if err != nil {
				return flags, err
			}
			switch name {
			case "--api-base":
				flags.apiBase = &value
			case "--api-key":
				flags.apiKey = &value
			case "--model":
				flags.model = &value
			}
			i = next
		default:
			return flags, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
		}
	}
	if err := rejectEmptyFlag("server", strings.TrimSpace(flags.serverFlag), flags.serverGiven); err != nil {
		return flags, err
	}
	if err := rejectEmptyFlag("region-code", strings.TrimSpace(flags.regionCodeFlag), flags.regionCodeGiven); err != nil {
		return flags, err
	}
	if err := rejectEmptyFlag("tenant-id", strings.TrimSpace(flags.tenantID), flags.tenantIDGiven); err != nil {
		return flags, err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(flags.publicKeyFlag), flags.publicKeyGiven); err != nil {
		return flags, err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(flags.privateKeyFlag), flags.privateKeyGiven); err != nil {
		return flags, err
	}
	flags.tenantID = strings.TrimSpace(flags.tenantID)
	if flags.tenantID == "" {
		return flags, fmt.Errorf("--tenant-id is required")
	}
	if !allowConfig {
		return flags, nil
	}
	if flags.enabled == nil {
		return flags, fmt.Errorf("--enabled is required")
	}
	if *flags.enabled {
		if flags.apiBase == nil || flags.apiKey == nil || flags.model == nil {
			return flags, fmt.Errorf("--api-base, --api-key, and --model are required when enabling")
		}
		if strings.TrimSpace(*flags.apiBase) == "" || strings.TrimSpace(*flags.apiKey) == "" || strings.TrimSpace(*flags.model) == "" {
			return flags, fmt.Errorf("--api-base, --api-key, and --model must not be empty when enabling")
		}
		return flags, nil
	}
	if flags.apiBase != nil || flags.apiKey != nil || flags.model != nil {
		return flags, fmt.Errorf("--api-base, --api-key, and --model must be omitted when disabling")
	}
	return flags, nil
}

func (f adminTenantEmbeddingConfigFlags) requestIdentity() (client.QuotaRequest, error) {
	publicKey, privateKey := adminTiDBCloudKeys(f.publicKeyFlag, f.privateKeyFlag)
	return quotaRequest(f.tenantID, publicKey, privateKey)
}

func (f adminTenantEmbeddingConfigFlags) server() (string, error) {
	r := ResolveCredentials()
	return quotaServer(f.serverFlag, f.regionCodeFlag, r.Server, true)
}

func adminTenantEmbeddingConfigGet(args []string) error {
	flags, err := parseAdminTenantEmbeddingConfigFlags(args, adminTenantEmbeddingConfigGetUsage(), false)
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	server, err := flags.server()
	if err != nil {
		return err
	}
	identity, err := flags.requestIdentity()
	if err != nil {
		return err
	}
	out, err := client.New(server, "").AdminGetTenantEmbeddingConfig(context.Background(), client.AdminTenantEmbeddingConfigGetRequest{
		TenantID: identity.TenantID, PublicKey: identity.PublicKey, PrivateKey: identity.PrivateKey,
	})
	if err != nil {
		return quotaAPIError("get tenant embedding config", err)
	}
	return printAdminTenantEmbeddingConfigResponse(out, flags.asJSON)
}

func adminTenantEmbeddingConfigSet(args []string) error {
	flags, err := parseAdminTenantEmbeddingConfigFlags(args, adminTenantEmbeddingConfigSetUsage(), true)
	if err != nil {
		if errors.Is(err, errHelpRequested{}) {
			return nil
		}
		return err
	}
	server, err := flags.server()
	if err != nil {
		return err
	}
	identity, err := flags.requestIdentity()
	if err != nil {
		return err
	}
	out, err := client.New(server, "").AdminSetTenantEmbeddingConfig(context.Background(), client.AdminTenantEmbeddingConfigSetRequest{
		TenantID: identity.TenantID, PublicKey: identity.PublicKey, PrivateKey: identity.PrivateKey,
		Enabled: *flags.enabled, APIBase: flags.apiBase, APIKey: flags.apiKey, Model: flags.model,
	})
	if err != nil {
		return quotaAPIError("set tenant embedding config", err)
	}
	return printAdminTenantEmbeddingConfigResponse(out, flags.asJSON)
}

func printAdminTenantEmbeddingConfigResponse(out *client.AdminTenantEmbeddingConfig, asJSON bool) error {
	if out == nil {
		return fmt.Errorf("tenant embedding config response is empty")
	}
	redacted := *out
	if out.APIKey != nil {
		apiKey := redactProviderAPIKey(*out.APIKey)
		redacted.APIKey = &apiKey
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(&redacted)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ENABLED\tSOURCE\tAPI_BASE\tAPI_KEY\tMODEL\tGENERATION\tUPDATED_AT")
	_, _ = fmt.Fprintf(w, "%t\t%s\t%s\t%s\t%s\t%s\t%s\n", redacted.Enabled, emptyAsDash(redacted.Source), optionalExtractString(redacted.APIBase), optionalExtractString(redacted.APIKey), optionalExtractString(redacted.Model), optionalEmbeddingGeneration(redacted.Generation), optionalExtractTime(redacted.UpdatedAt))
	return w.Flush()
}

func optionalEmbeddingGeneration(generation uint64) string {
	if generation == 0 {
		return "-"
	}
	return fmt.Sprintf("%d", generation)
}

func adminTenantEmbeddingConfigUsage() string {
	return `usage: drive9 admin tenant embedding-config <get|set> [flags]

commands:
  get [flags]                       show one tenant's embedding config
  set [flags]                       replace one tenant's embedding config

set is a full replacement. --enabled is required. Enabling also requires
--api-base, --api-key, and --model; disabling requires them to be omitted.

set flags:
  --enabled true|false
  --api-base URL
  --api-key KEY
  --model MODEL`
}

func adminTenantEmbeddingConfigGetUsage() string {
	return `usage: drive9 admin tenant embedding-config get [flags]

flags:
  --server URL
  --region-code CODE
  --tenant-id ID                   required
  --tidbcloud-public-key KEY
  --tidbcloud-private-key KEY
  --json                           output result as JSON`
}

func adminTenantEmbeddingConfigSetUsage() string {
	return `usage: drive9 admin tenant embedding-config set [flags]

replace one tenant's embedding configuration. This is a full replacement;
provider completeness and live validation are performed by the server.

flags:
  --server URL
  --region-code CODE
  --tenant-id ID                   required
  --enabled true|false             required
  --api-base URL                   required when enabling; omit when disabling
  --api-key KEY                    required when enabling; omit when disabling (sensitive)
  --model MODEL                    required when enabling; omit when disabling
  --tidbcloud-public-key KEY
  --tidbcloud-private-key KEY
  --json                           output result as JSON`
}
