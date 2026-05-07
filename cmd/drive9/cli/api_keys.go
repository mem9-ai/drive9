package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

const defaultManagedAPIKeyName = "default"

// APIKeyCmd dispatches tenant API key management subcommands.
func APIKeyCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage drive9 api-key <ls|create|get|rm>")
	}
	switch args[0] {
	case "ls":
		return apiKeyList(args[1:])
	case "create":
		return apiKeyCreate(args[1:])
	case "get":
		return apiKeyGet(args[1:])
	case "rm":
		return apiKeyDelete(args[1:])
	case "-h", "--help", "help":
		return fmt.Errorf("usage drive9 api-key <ls|create|get|rm>")
	default:
		return fmt.Errorf("unknown api-key command %q", args[0])
	}
}

func apiKeyList(args []string) error {
	asJSON := false
	for _, arg := range args {
		switch arg {
		case "--json":
			asJSON = true
		default:
			return fmt.Errorf("usage drive9 api-key ls [--json]")
		}
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	keys, err := c.ListTenantAPIKeys(context.Background())
	if err != nil {
		return err
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].KeyName < keys[j].KeyName
	})
	if asJSON {
		return writeJSON(map[string]any{"keys": keys})
	}
	printTenantAPIKeyTable(keys)
	return nil
}

func apiKeyCreate(args []string) error {
	name, asJSON, err := parseAPIKeyNameAndJSONFlag(args, "usage drive9 api-key create <name> [--json]")
	if err != nil {
		return err
	}
	if err := validateTenantAPIKeyNameForCreate(name); err != nil {
		return err
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	key, err := c.CreateTenantAPIKey(context.Background(), name)
	if err != nil {
		return err
	}
	if asJSON {
		return writeJSON(key)
	}
	printTenantAPIKey(key)
	return nil
}

func apiKeyGet(args []string) error {
	name, asJSON, err := parseAPIKeyNameAndJSONFlag(args, "usage drive9 api-key get <name> [--json]")
	if err != nil {
		return err
	}
	if err := validateTenantAPIKeyName(name); err != nil {
		return err
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	key, err := c.GetTenantAPIKey(context.Background(), name)
	if err != nil {
		return err
	}
	if asJSON {
		return writeJSON(key)
	}
	printTenantAPIKey(key)
	return nil
}

func apiKeyDelete(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage drive9 api-key rm <name>")
	}
	if err := validateTenantAPIKeyName(args[0]); err != nil {
		return err
	}
	c, err := newVaultManagementClientFromEnv()
	if err != nil {
		return err
	}
	return c.DeleteTenantAPIKey(context.Background(), args[0])
}

func parseAPIKeyNameAndJSONFlag(args []string, usage string) (string, bool, error) {
	var (
		name   string
		asJSON bool
	)
	for _, arg := range args {
		switch arg {
		case "--json":
			asJSON = true
		default:
			if strings.HasPrefix(arg, "--") {
				return "", false, fmt.Errorf("unknown flag %q", arg)
			}
			if name != "" {
				return "", false, fmt.Errorf("%s", usage)
			}
			name = arg
		}
	}
	if name == "" {
		return "", false, fmt.Errorf("%s", usage)
	}
	return name, asJSON, nil
}

func validateTenantAPIKeyName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("api key name is required")
	}
	if len(name) > 64 {
		return fmt.Errorf("api key name must be <= 64 characters")
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf("api key name must not contain /")
	}
	return nil
}

func validateTenantAPIKeyNameForCreate(name string) error {
	if err := validateTenantAPIKeyName(name); err != nil {
		return err
	}
	if name == defaultManagedAPIKeyName {
		return fmt.Errorf("api key name %q is reserved", defaultManagedAPIKeyName)
	}
	return nil
}

func printTenantAPIKeyTable(keys []client.TenantAPIKeySummary) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tSTATUS\tISSUED_AT\tREVOKED_AT")
	for _, key := range keys {
		revokedAt := ""
		if key.RevokedAt != nil {
			revokedAt = key.RevokedAt.Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			key.KeyName,
			key.Status,
			key.IssuedAt.Format(time.RFC3339),
			revokedAt,
		)
	}
	_ = w.Flush()
}

func printTenantAPIKey(key *client.TenantAPIKey) {
	_, _ = fmt.Fprintf(os.Stdout, "api_key=%s\n", key.APIKey)
	_, _ = fmt.Fprintf(os.Stdout, "key_id=%s\n", key.KeyID)
	_, _ = fmt.Fprintf(os.Stdout, "key_name=%s\n", key.KeyName)
	_, _ = fmt.Fprintf(os.Stdout, "status=%s\n", key.Status)
	if !key.IssuedAt.IsZero() {
		_, _ = fmt.Fprintf(os.Stdout, "issued_at=%s\n", key.IssuedAt.Format(time.RFC3339))
	}
	if key.RevokedAt != nil {
		_, _ = fmt.Fprintf(os.Stdout, "revoked_at=%s\n", key.RevokedAt.Format(time.RFC3339))
	}
}
