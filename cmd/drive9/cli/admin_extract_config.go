package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func adminTenantExtractConfig(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", adminTenantExtractConfigUsage())
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprintln(os.Stdout, adminTenantExtractConfigUsage())
		return nil
	case "get":
		return adminTenantExtractConfigGet(args[1:])
	case "set":
		return adminTenantExtractConfigSet(args[1:])
	default:
		return fmt.Errorf("unknown admin tenant extract-config command %q\n%s", args[0], adminTenantExtractConfigUsage())
	}
}

type adminTenantExtractConfigFlags struct {
	serverFlag       string
	serverGiven      bool
	regionCodeFlag   string
	regionCodeGiven  bool
	tenantID         string
	tenantIDGiven    bool
	mediaType        string
	mediaTypeGiven   bool
	publicKeyFlag    string
	publicKeyGiven   bool
	privateKeyFlag   string
	privateKeyGiven  bool
	asJSON           bool
	enabled          *bool
	apiBase          *string
	apiKey           *string
	model            *string
	prompt           *string
	configFieldCount int
}

func parseAdminTenantExtractConfigFlags(args []string, usage string, allowConfig bool) (adminTenantExtractConfigFlags, error) {
	var flags adminTenantExtractConfigFlags
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
		case "--media-type":
			value, next, err := nextAdminExtractFlag(args, i, "--media-type")
			if err != nil {
				return flags, err
			}
			flags.mediaType, flags.mediaTypeGiven, i = value, true, next
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
			flags.configFieldCount++
		case "--api-base":
			if !allowConfig {
				return flags, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
			}
			value, next, err := nextAdminExtractFlag(args, i, "--api-base")
			if err != nil {
				return flags, err
			}
			flags.apiBase, i = &value, next
			flags.configFieldCount++
		case "--api-key":
			if !allowConfig {
				return flags, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
			}
			value, next, err := nextAdminExtractFlag(args, i, "--api-key")
			if err != nil {
				return flags, err
			}
			flags.apiKey, i = &value, next
			flags.configFieldCount++
		case "--model":
			if !allowConfig {
				return flags, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
			}
			value, next, err := nextAdminExtractFlag(args, i, "--model")
			if err != nil {
				return flags, err
			}
			flags.model, i = &value, next
			flags.configFieldCount++
		case "--prompt":
			if !allowConfig {
				return flags, fmt.Errorf("unknown flag %q\n%s", args[i], usage)
			}
			value, next, err := nextAdminExtractFlag(args, i, "--prompt")
			if err != nil {
				return flags, err
			}
			flags.prompt, i = &value, next
			flags.configFieldCount++
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
	if err := rejectEmptyFlag("media-type", strings.TrimSpace(flags.mediaType), flags.mediaTypeGiven); err != nil {
		return flags, err
	}
	if err := rejectEmptyFlag("tidbcloud-public-key", strings.TrimSpace(flags.publicKeyFlag), flags.publicKeyGiven); err != nil {
		return flags, err
	}
	if err := rejectEmptyFlag("tidbcloud-private-key", strings.TrimSpace(flags.privateKeyFlag), flags.privateKeyGiven); err != nil {
		return flags, err
	}
	if strings.TrimSpace(flags.tenantID) == "" {
		return flags, fmt.Errorf("--tenant-id is required")
	}
	if strings.TrimSpace(flags.mediaType) == "" {
		return flags, fmt.Errorf("--media-type is required")
	}
	flags.tenantID = strings.TrimSpace(flags.tenantID)
	flags.mediaType = strings.TrimSpace(flags.mediaType)
	if allowConfig && flags.configFieldCount == 0 {
		return flags, fmt.Errorf("at least one extract config field is required")
	}
	return flags, nil
}

func nextAdminExtractFlag(args []string, index int, name string) (string, int, error) {
	if index+1 >= len(args) {
		return "", index, fmt.Errorf("%s requires an argument", name)
	}
	return args[index+1], index + 1, nil
}

func parseAdminExtractBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("--enabled must be true or false")
	}
}

func (f adminTenantExtractConfigFlags) client(server string) *client.Client {
	return client.New(server, "")
}

func (f adminTenantExtractConfigFlags) credentials() (string, string) {
	return adminTiDBCloudKeys(f.publicKeyFlag, f.privateKeyFlag)
}

func (f adminTenantExtractConfigFlags) requestIdentity() (client.QuotaRequest, error) {
	publicKey, privateKey := f.credentials()
	return quotaRequest(strings.TrimSpace(f.tenantID), publicKey, privateKey)
}

func (f adminTenantExtractConfigFlags) server() (string, error) {
	r := ResolveCredentials()
	return quotaServer(f.serverFlag, f.regionCodeFlag, r.Server, true)
}

func adminTenantExtractConfigGet(args []string) error {
	flags, err := parseAdminTenantExtractConfigFlags(args, adminTenantExtractConfigGetUsage(), false)
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
	out, err := flags.client(server).AdminGetTenantExtractConfig(context.Background(), client.AdminTenantExtractConfigGetRequest{
		TenantID: identity.TenantID, MediaType: client.ExtractMediaType(strings.TrimSpace(flags.mediaType)), PublicKey: identity.PublicKey, PrivateKey: identity.PrivateKey,
	})
	if err != nil {
		return quotaAPIError("get tenant extract config", err)
	}
	return printAdminTenantExtractConfigResponse(out, flags.mediaType, flags.asJSON)
}

func adminTenantExtractConfigSet(args []string) error {
	flags, err := parseAdminTenantExtractConfigFlags(args, adminTenantExtractConfigSetUsage(), true)
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
	out, err := flags.client(server).AdminSetTenantExtractConfig(context.Background(), client.AdminTenantExtractConfigSetRequest{
		TenantID: identity.TenantID, MediaType: client.ExtractMediaType(strings.TrimSpace(flags.mediaType)), PublicKey: identity.PublicKey, PrivateKey: identity.PrivateKey,
		Enabled: flags.enabled, APIBase: flags.apiBase, APIKey: flags.apiKey, Model: flags.model, Prompt: flags.prompt,
	})
	if err != nil {
		return quotaAPIError("set tenant extract config", err)
	}
	return printAdminTenantExtractConfigResponse(out, flags.mediaType, flags.asJSON)
}

func printAdminTenantExtractConfigResponse(out *client.AdminTenantExtractConfig, mediaType string, asJSON bool) error {
	if out == nil {
		return fmt.Errorf("tenant extract config response is empty")
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "MEDIA_TYPE\tENABLED\tSOURCE\tAPI_BASE\tAPI_KEY\tMODEL\tPROMPT\tUPDATED_AT")
	_, _ = fmt.Fprintf(w, "%s\t%t\t%s\t%s\t%s\t%s\t%s\t%s\n", mediaType, out.Enabled, emptyAsDash(out.Source), optionalExtractString(out.APIBase), optionalExtractString(out.APIKey), optionalExtractString(out.Model), optionalExtractString(out.Prompt), optionalExtractTime(out.UpdatedAt))
	return w.Flush()
}

func optionalExtractString(value *string) string {
	if value == nil {
		return "-"
	}
	return emptyAsDash(*value)
}

func optionalExtractTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "-"
	}
	return value.Format(time.RFC3339)
}

func adminTenantExtractConfigUsage() string {
	return `usage: drive9 admin tenant extract-config <get|set> [flags]

commands:
  get [flags]                       show one tenant's extract config
  set [flags]                       partially update one tenant's extract config

common flags:
  --server URL                     server URL (default: active context server)
  --region-code CODE               region code; ignored when --server is set
  --tenant-id ID                   drive9 tenant id
  --media-type TYPE                extract media type (image, audio, video, text, or a future type)
  --tidbcloud-public-key KEY       TiDB Cloud public key
  --tidbcloud-private-key KEY      TiDB Cloud private key
  --json                           output result as JSON

set flags:
  --enabled true|false             enable or disable this media type
  --api-base URL                   provider base URL
  --api-key KEY                    provider API key (sensitive)
  --model MODEL                    provider model name
  --prompt TEXT                    custom extraction prompt; empty clears the override`
}

func adminTenantExtractConfigGetUsage() string {
	return `usage: drive9 admin tenant extract-config get [flags]

show one tenant's effective extract configuration.

flags:
  --server URL
  --region-code CODE
  --tenant-id ID                   required
  --media-type TYPE                required
  --tidbcloud-public-key KEY
  --tidbcloud-private-key KEY
  --json                           output result as JSON`
}

func adminTenantExtractConfigSetUsage() string {
	return `usage: drive9 admin tenant extract-config set [flags]

partially update one tenant's extract configuration. At least one config field
is required; provider completeness and live validation are performed by the server.

flags:
  --server URL
  --region-code CODE
  --tenant-id ID                   required
  --media-type TYPE                required
  --enabled true|false
  --api-base URL
  --api-key KEY                    sensitive
  --model MODEL
  --prompt TEXT                    empty clears the custom prompt
  --tidbcloud-public-key KEY
  --tidbcloud-private-key KEY
  --json                           output result as JSON`
}
