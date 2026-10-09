package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type runtimeExitError struct{ code int }

func (e *runtimeExitError) Error() string {
	return fmt.Sprintf("remote process exited with status %d", e.code)
}
func (e *runtimeExitError) ExitCode() int { return e.code }

// Exec runs one remote process against a persistent Drive9 workspace. It
// streams bytes directly to the local stdout/stderr and never retries.
func Exec(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return execWithContext(ctx, args)
}

func execWithContext(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	workspace := fs.String("workspace", "/", "Drive9 workspace root")
	layerID := fs.String("layer", "", "exact Drive9 LayerFS layer ID")
	cwd := fs.String("cwd", ".", "working directory relative to the workspace root")
	readOnly := fs.Bool("read-only", false, "mount the workspace read-only")
	persistence := fs.String("persistence", client.RuntimePersistenceFullRoot, "required persistence scope: full_root or workspace")
	timeout := fs.Duration("timeout", 0, "execution timeout (server default when omitted)")
	var environment stringListFlag
	fs.Var(&environment, "env", "environment entry KEY=VALUE (repeatable)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: drive9 exec [flags] -- command [arg...]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return errors.New("command is required")
	}
	root, err := runtimeWorkspaceRoot(*workspace)
	if err != nil {
		return err
	}
	if err := validateRuntimeLayerID(*layerID); err != nil {
		return err
	}
	env, err := runtimeEnvironment(environment)
	if err != nil {
		return err
	}
	if *timeout < 0 || *timeout > time.Hour {
		return errors.New("--timeout must be between 0 and 1h")
	}
	if *persistence != client.RuntimePersistenceFullRoot && *persistence != client.RuntimePersistenceWorkspace {
		return errors.New("--persistence must be full_root or workspace")
	}
	if *layerID != "" && *persistence == client.RuntimePersistenceFullRoot {
		return errors.New("--layer requires --persistence=workspace; full_root LayerFS views are not supported")
	}

	request := client.ExecRequest{
		Argv: append([]string(nil), fs.Args()...), Cwd: *cwd, Env: env,
		Workspace: client.RuntimeWorkspace{Root: root, LayerID: *layerID, ReadOnly: *readOnly, Persistence: *persistence},
	}
	if *timeout > 0 {
		request.TimeoutMS = timeout.Milliseconds()
	}
	result, err := NewFromEnv().Exec(ctx, request, client.ExecOptions{Stdout: os.Stdout, Stderr: os.Stderr})
	if err != nil {
		if client.IsRuntimeOutcomeUnknown(err) {
			return fmt.Errorf("%w; the process may have run and was not retried", err)
		}
		return err
	}
	if result.ExitCode != 0 {
		return &runtimeExitError{code: result.ExitCode}
	}
	return nil
}

func validateRuntimeLayerID(value string) error {
	if value == "" {
		return nil
	}
	if strings.TrimSpace(value) != value || len(value) > 64 || strings.ContainsAny(value, ":\x00") {
		return errors.New("--layer must be an exact valid LayerFS layer ID")
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return errors.New("--layer must be an exact valid LayerFS layer ID")
		}
	}
	return nil
}

func runtimeWorkspaceRoot(raw string) (string, error) {
	raw = strings.TrimSpace(strings.TrimPrefix(raw, ":"))
	if raw == "" || !strings.HasPrefix(raw, "/") || path.Clean(raw) != raw || strings.ContainsRune(raw, '\x00') {
		return "", errors.New("--workspace must be a canonical absolute Drive9 path")
	}
	return raw, nil
}

func runtimeEnvironment(entries []string) (map[string]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !validRuntimeEnvironmentKey(key) || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("invalid --env entry %q", entry)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate --env key %q", key)
		}
		result[key] = value
	}
	return result, nil
}

func validRuntimeEnvironmentKey(key string) bool {
	if key == "" {
		return false
	}
	for index, char := range key {
		if char == '_' || char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || index > 0 && char >= '0' && char <= '9' {
			continue
		}
		return false
	}
	return true
}
