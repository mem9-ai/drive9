package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/mem9-ai/dat9/pkg/client"
)

func Auth(_ *client.Client, args []string) error {
	if len(args) < 1 {
		return showAuth()
	}
	apiKey := args[0]
	return saveCredential("api_key", apiKey)
}

func showAuth() error {
	key := loadCredential("api_key")
	if key == "" {
		fmt.Fprintln(os.Stderr, "no API key configured")
		fmt.Fprintln(os.Stderr, "usage: dat9 auth <api-key>")
		return nil
	}
	masked := key[:8] + "..." + key[len(key)-4:]
	fmt.Printf("api_key = %s\n", masked)
	fmt.Printf("credentials: %s\n", credentialsPath())
	return nil
}

func saveCredential(key, value string) error {
	dir := configDir()
	if dir == "" {
		return fmt.Errorf("cannot determine home directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	path := credentialsPath()
	existing := make(map[string]string)

	if data, err := os.ReadFile(path); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if ok {
				existing[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
	}
	existing[key] = fmt.Sprintf("%q", value)

	var lines []string
	for k, v := range existing {
		lines = append(lines, fmt.Sprintf("%s = %s", k, v))
	}
	content := strings.Join(lines, "\n") + "\n"

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write credentials: %w", err)
	}
	fmt.Printf("API key saved to %s\n", path)
	return nil
}
