package cli

import "os"

// withEnsureCredentialEnv temporarily installs stored mount credentials into
// the process environment for a mount operation.
func withEnsureCredentialEnv(server, apiKey, token string, fn func() error) error {
	if server == "" && apiKey == "" && token == "" {
		return fn()
	}
	type snap struct {
		key string
		val string
		ok  bool
	}
	keys := []string{EnvServer, EnvAPIKey, EnvVaultToken}
	prev := make([]snap, 0, len(keys))
	for _, k := range keys {
		v, ok := os.LookupEnv(k)
		prev = append(prev, snap{key: k, val: v, ok: ok})
	}
	defer func() {
		for _, s := range prev {
			if s.ok {
				_ = os.Setenv(s.key, s.val)
			} else {
				_ = os.Unsetenv(s.key)
			}
		}
	}()
	if server != "" {
		_ = os.Setenv(EnvServer, server)
	}
	if token != "" {
		_ = os.Setenv(EnvVaultToken, token)
		_ = os.Unsetenv(EnvAPIKey)
	} else if apiKey != "" {
		_ = os.Setenv(EnvAPIKey, apiKey)
		_ = os.Unsetenv(EnvVaultToken)
	}
	return fn()
}
