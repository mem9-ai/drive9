//go:build !windows

package cli

import (
	"fmt"
	"os"
)

func scrubMountCredentialEnvironment() error {
	for _, name := range []string{"DRIVE9_API_KEY", "DRIVE9_VAULT_TOKEN", "DRIVE9_SERVER"} {
		if err := os.Unsetenv(name); err != nil {
			return fmt.Errorf("unset %s: %w", name, err)
		}
	}
	return nil
}
