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
	"sync"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Exit statuses for outcomes that are not the remote command's own status.
// They follow shell conventions (timeout(1), SIGINT, ssh).
const (
	execExitTimedOut = 124
	execExitCanceled = 130
	execExitUnknown  = 255
)

const execUsage = `usage: drive9 exec [flags] [--] command [arg...]
       drive9 exec [flags] 'shell command line'

Run a command in a sandbox with your workspace mounted at /workspace.
Output streams live; the exit status is the command's.

flags:
  -w, --workspace :/PATH   Drive9 directory mounted at /workspace (default :/)
  -C, --cwd DIR            working directory, relative to /workspace (default /workspace)
  -e, --env KEY[=VALUE]    set an environment variable; KEY alone copies it from here
  -t, --timeout DURATION   stop the command after DURATION (default: server default)
      --layer ID           use a LayerFS layer view of the workspace
      --read-only          mount the workspace read-only
  -v, --verbose            show the sandbox, working directory, and duration

examples:
  drive9 exec npm test
  drive9 exec -w :/repo 'npm ci && npm test'
  drive9 exec -e CI=1 -e GITHUB_TOKEN -t 20m -- make -j8 test
`

// execExitError carries the process exit status without printing anything:
// the remote command already wrote its own diagnostics.
type execExitError struct{ code int }

func (e *execExitError) Error() string    { return fmt.Sprintf("exit status %d", e.code) }
func (e *execExitError) ExitCode() int    { return e.code }
func (e *execExitError) SilentExit() bool { return true }

// execStatusError is a drive9-side failure with a specific exit status.
type execStatusError struct {
	code    int
	message string
}

func (e *execStatusError) Error() string { return e.message }
func (e *execStatusError) ExitCode() int { return e.code }

type execOptions struct {
	workspace string
	layer     string
	cwd       string
	readOnly  bool
	timeout   time.Duration
	verbose   bool
	env       []string
	command   []string
}

// Exec runs one command in a sandbox against a Drive9 workspace. It never
// retries: a command whose outcome is unknown may already have run.
func Exec(args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	var execID execIDBox
	go func() {
		if _, ok := <-signals; !ok {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, "drive9: canceling… (press Ctrl-C again to detach)")
		cancel()
		if _, ok := <-signals; !ok {
			return
		}
		if id := execID.get(); id != "" {
			_, _ = fmt.Fprintf(os.Stderr, "drive9: detached; %s may still be running\n", id)
		}
		os.Exit(execExitCanceled)
	}()
	return runExec(ctx, args, &execID, os.Stdout, os.Stderr)
}

type execIDBox struct {
	mu sync.Mutex
	id string
}

func (b *execIDBox) set(id string) { b.mu.Lock(); b.id = id; b.mu.Unlock() }
func (b *execIDBox) get() string   { b.mu.Lock(); defer b.mu.Unlock(); return b.id }

func runExec(ctx context.Context, args []string, execID *execIDBox, stdout, stderr io.Writer) error {
	options, err := parseExecArgs(args)
	if err != nil {
		return err
	}
	request, err := options.request()
	if err != nil {
		return err
	}
	started := time.Now()
	result, err := NewFromEnv().Exec(ctx, request, client.ExecOptions{
		Stdout: stdout,
		Stderr: stderr,
		OnStarted: func(info client.ExecStarted) error {
			execID.set(info.ExecutionID)
			if options.verbose {
				_, _ = fmt.Fprintf(stderr, "drive9: running in %s sandbox at %s (%s)\n", sandboxLabel(info.Sandbox), info.Cwd, info.ExecutionID)
			}
			return nil
		},
	})
	if err != nil {
		return execError(err, execID.get())
	}
	if options.verbose {
		_, _ = fmt.Fprintf(stderr, "drive9: exit %d after %s\n", result.ExitCode, time.Since(started).Round(100*time.Millisecond))
	}
	if result.ExitCode != 0 {
		return &execExitError{code: result.ExitCode}
	}
	return nil
}

func sandboxLabel(sandbox string) string {
	if sandbox == client.RuntimeSandboxWarm {
		return "a warm"
	}
	return "a new"
}

func execError(err error, executionID string) error {
	var (
		busy     *client.RuntimeBusyError
		timeout  *client.RuntimeTimeoutError
		canceled *client.RuntimeCanceledError
		start    *client.RuntimeStartError
	)
	switch {
	case errors.Is(err, client.ErrRuntimeNotEnabled):
		return errors.New("remote exec is not enabled on this server")
	case errors.As(err, &busy):
		return &execStatusError{code: 1, message: fmt.Sprintf("%s; retry in %s", busy.Message, busy.RetryAfter)}
	case errors.As(err, &timeout):
		return &execStatusError{code: execExitTimedOut, message: "command timed out and was stopped"}
	case errors.As(err, &canceled):
		return &execStatusError{code: execExitCanceled, message: "command canceled"}
	case client.IsRuntimeOutcomeUnknown(err):
		message := "lost track of the command; it may have finished. Check the workspace before running it again"
		if executionID != "" {
			message += " (" + executionID + ")"
		}
		return &execStatusError{code: execExitUnknown, message: message}
	case errors.As(err, &start):
		return errors.New(start.Message)
	}
	return err
}

func parseExecArgs(args []string) (execOptions, error) {
	var options execOptions
	var env stringListFlag
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() { _, _ = fmt.Fprint(os.Stderr, execUsage) }
	for _, name := range []string{"w", "workspace"} {
		fs.StringVar(&options.workspace, name, ":/", "")
	}
	for _, name := range []string{"C", "cwd"} {
		fs.StringVar(&options.cwd, name, "", "")
	}
	for _, name := range []string{"t", "timeout"} {
		fs.DurationVar(&options.timeout, name, 0, "")
	}
	for _, name := range []string{"v", "verbose"} {
		fs.BoolVar(&options.verbose, name, false, "")
	}
	fs.Var(&env, "e", "")
	fs.Var(&env, "env", "")
	fs.StringVar(&options.layer, "layer", "", "")
	fs.BoolVar(&options.readOnly, "read-only", false, "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.Usage()
			return execOptions{}, &execExitError{code: 0}
		}
		return execOptions{}, fmt.Errorf("%w (see drive9 exec --help)", err)
	}
	options.env = env
	options.command = fs.Args()
	if len(options.command) == 0 {
		return execOptions{}, errors.New("missing command (see drive9 exec --help)")
	}
	if options.timeout < 0 {
		return execOptions{}, errors.New("--timeout must be positive")
	}
	return options, nil
}

func (options execOptions) request() (client.ExecRequest, error) {
	root, err := runtimeWorkspaceRoot(options.workspace)
	if err != nil {
		return client.ExecRequest{}, err
	}
	if err := validateRuntimeLayerID(options.layer); err != nil {
		return client.ExecRequest{}, err
	}
	env, err := runtimeEnvironment(options.env)
	if err != nil {
		return client.ExecRequest{}, err
	}
	request := client.ExecRequest{
		Cwd: options.cwd, Env: env,
		Workspace: client.RuntimeWorkspace{Root: root, LayerID: options.layer, ReadOnly: options.readOnly},
	}
	if options.timeout > 0 {
		request.TimeoutMS = options.timeout.Milliseconds()
	}
	if len(options.command) == 1 && looksLikeShellLine(options.command[0]) {
		request.Command = options.command[0]
	} else {
		request.Argv = append([]string(nil), options.command...)
	}
	return request, nil
}

// looksLikeShellLine reports whether a single argument is meant for a shell,
// as in `drive9 exec 'npm ci && npm test'`.
func looksLikeShellLine(argument string) bool {
	return strings.ContainsAny(argument, " \t\n|&;<>()$`\\\"'*?[]#~")
}

func validateRuntimeLayerID(value string) error {
	if value == "" {
		return nil
	}
	if strings.TrimSpace(value) != value || len(value) > 64 || strings.ContainsAny(value, ":\x00") {
		return errors.New("--layer must be a LayerFS layer ID")
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return errors.New("--layer must be a LayerFS layer ID")
		}
	}
	return nil
}

func runtimeWorkspaceRoot(raw string) (string, error) {
	raw = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), ":"))
	if raw == "" {
		return "/", nil
	}
	if !strings.HasPrefix(raw, "/") || strings.ContainsRune(raw, '\x00') {
		return "", fmt.Errorf("--workspace must be a Drive9 path such as :/repo, got %q", raw)
	}
	return path.Clean(raw), nil
}

// runtimeEnvironment parses KEY=VALUE entries. A bare KEY copies the value
// from the local environment, like `docker run -e KEY`.
func runtimeEnvironment(entries []string) (map[string]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, hasValue := strings.Cut(entry, "=")
		if !validRuntimeEnvironmentKey(key) || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("invalid --env %q: use KEY=VALUE or KEY", entry)
		}
		if !hasValue {
			local, ok := os.LookupEnv(key)
			if !ok {
				return nil, fmt.Errorf("--env %s: %s is not set in your environment", key, key)
			}
			value = local
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("--env %s is set twice", key)
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
