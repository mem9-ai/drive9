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

	"golang.org/x/term"

	"github.com/mem9-ai/drive9/pkg/client"
)

// Exit statuses for outcomes that are not the remote command's own status.
// They follow shell conventions (timeout(1), SIGINT, ssh).
const (
	execExitTimedOut = 124
	execExitCanceled = 130
	execExitUnknown  = 255
)

// execProgressDelay is how long a command may take to start before the CLI
// tells the user it is starting a sandbox.
const execProgressDelay = 700 * time.Millisecond

const execUsage = `usage: drive9 exec [flags] [--] command [arg...]
       drive9 exec [flags] 'shell command line'

Run a command in a sandbox with your drive9 drive mounted at /workspace.
Output streams live; the exit status is the command's.

Inside a local drive9 mount, the command runs in the matching directory,
after your local changes are uploaded.

flags:
  -w, --workspace PATH     Drive9 directory to run in, e.g. repo (default: where
                           you are in a local mount, else the drive root)
  -C, --cwd DIR            subdirectory of the workspace to run in
  -e, --env KEY[=VALUE]    set an environment variable; KEY alone copies it from here
  -t, --timeout DURATION   stop the command after DURATION (default: server default)
      --fresh              start in a new sandbox instead of reusing the warm one
      --layer ID           use a LayerFS layer view of the workspace
      --read-only          mount the workspace read-only
  -v, --verbose            show the sandbox, working directory, and duration

examples:
  drive9 exec npm test
  drive9 exec -w repo 'npm ci && npm test'
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
	workspace    string
	workspaceSet bool
	layer        string
	cwd          string
	cwdSet       bool
	readOnly     bool
	fresh        bool
	timeout      time.Duration
	verbose      bool
	env          []string
	command      []string
}

// execRuntime is everything `drive9 exec` takes from its surroundings.
type execRuntime struct {
	stdout, stderr       io.Writer
	stdoutTTY, stderrTTY bool
	dir                  string
	lookupEnv            func(string) (string, bool)
	server               func() string
	findMount            func(string) (*localMount, error)
	syncMount            func(context.Context, *localMount) error
	client               func() *client.Client
	execID               *execIDBox
}

func defaultExecRuntime(execID *execIDBox) execRuntime {
	dir, _ := os.Getwd()
	return execRuntime{
		stdout: os.Stdout, stderr: os.Stderr,
		stdoutTTY: term.IsTerminal(int(os.Stdout.Fd())), stderrTTY: term.IsTerminal(int(os.Stderr.Fd())),
		dir: dir, lookupEnv: os.LookupEnv, server: func() string { return ResolveCredentials().Server },
		findMount: findLocalMount, syncMount: syncLocalMount,
		client: NewFromEnv, execID: execID,
	}
}

// Exec runs one command in a sandbox against a Drive9 workspace. It never
// retries: a command whose outcome is unknown may already have run.
func Exec(args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	execID := &execIDBox{}
	go func() {
		if _, ok := <-signals; !ok {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, "\ndrive9: canceling… (press Ctrl-C again to detach)")
		cancel()
		if _, ok := <-signals; !ok {
			return
		}
		if id := execID.get(); id != "" {
			_, _ = fmt.Fprintf(os.Stderr, "drive9: detached; %s may still be running\n", id)
		}
		os.Exit(execExitCanceled)
	}()
	return runExec(ctx, args, defaultExecRuntime(execID))
}

type execIDBox struct {
	mu sync.Mutex
	id string
}

func (b *execIDBox) set(id string) { b.mu.Lock(); b.id = id; b.mu.Unlock() }
func (b *execIDBox) get() string   { b.mu.Lock(); defer b.mu.Unlock(); return b.id }

func runExec(ctx context.Context, args []string, rt execRuntime) error {
	if rt.execID == nil {
		rt.execID = &execIDBox{}
	}
	options, err := parseExecArgs(args)
	if err != nil {
		return err
	}
	if err := resolveExecWorkspace(ctx, &options, rt); err != nil {
		return err
	}
	request, err := options.request(rt)
	if err != nil {
		return err
	}
	progress := newExecProgress(rt.stderr, rt.stderrTTY && !options.verbose)
	defer progress.stop()
	started := time.Now()
	result, err := rt.client().Exec(ctx, request, client.ExecOptions{
		Stdout: rt.stdout,
		Stderr: rt.stderr,
		OnStarted: func(info client.ExecStarted) error {
			progress.stop()
			rt.execID.set(info.ExecutionID)
			if options.verbose {
				_, _ = fmt.Fprintf(rt.stderr, "drive9: running in %s sandbox at %s (%s, ready in %s)\n",
					sandboxLabel(info.Sandbox), info.Cwd, info.ExecutionID, time.Since(started).Round(100*time.Millisecond))
			}
			return nil
		},
	})
	progress.stop()
	if err != nil {
		return execError(err, rt.execID.get())
	}
	if options.verbose {
		_, _ = fmt.Fprintf(rt.stderr, "drive9: exit %d after %s\n", result.ExitCode, time.Since(started).Round(100*time.Millisecond))
	}
	if result.ExitCode != 0 {
		return &execExitError{code: result.ExitCode}
	}
	return nil
}

// resolveExecWorkspace picks the workspace when -w is not given: the local
// drive9 mount the user is in (after uploading pending writes), else ":/".
func resolveExecWorkspace(ctx context.Context, options *execOptions, rt execRuntime) error {
	if options.workspaceSet || rt.findMount == nil || rt.dir == "" {
		return nil
	}
	mount, err := rt.findMount(rt.dir)
	if err != nil || mount == nil {
		return nil
	}
	if server := currentExecServer(rt); server != "" && mount.Server != "" && strings.TrimRight(server, "/") != strings.TrimRight(mount.Server, "/") {
		// The mount belongs to another server; do not guess.
		return nil
	}
	if rt.syncMount != nil {
		progress := newExecProgressMessage(rt.stderr, rt.stderrTTY && !options.verbose, "drive9: uploading local changes…")
		err := rt.syncMount(ctx, mount)
		progress.stop()
		if err != nil {
			return fmt.Errorf("could not upload local changes in %s: %v (pass -w to run against the remote workspace as it is)", mount.MountPoint, err)
		}
	}
	options.workspace = mount.workspace()
	if options.layer == "" {
		options.layer = mount.Layer
	}
	if !options.cwdSet {
		options.cwd = mount.remoteCwd()
	}
	if options.verbose {
		_, _ = fmt.Fprintf(rt.stderr, "drive9: using workspace :%s from %s\n", options.workspace, mount.MountPoint)
	}
	return nil
}

func currentExecServer(rt execRuntime) string {
	if rt.server == nil {
		return ""
	}
	return rt.server()
}

func sandboxLabel(sandbox string) string {
	if sandbox == client.RuntimeSandboxWarm {
		return "a warm"
	}
	return "a new"
}

// execProgress shows a transient one-line status on a terminal while the
// sandbox starts, and erases it before any command output appears.
type execProgress struct {
	mu      sync.Mutex
	out     io.Writer
	timer   *time.Timer
	shown   bool
	stopped bool
}

func newExecProgress(out io.Writer, enabled bool) *execProgress {
	return newExecProgressMessage(out, enabled, "drive9: starting sandbox…")
}

func newExecProgressMessage(out io.Writer, enabled bool, message string) *execProgress {
	progress := &execProgress{out: out}
	if !enabled {
		progress.stopped = true
		return progress
	}
	progress.timer = time.AfterFunc(execProgressDelay, func() {
		progress.mu.Lock()
		defer progress.mu.Unlock()
		if !progress.stopped {
			_, _ = fmt.Fprint(progress.out, message)
			progress.shown = true
		}
	})
	return progress
}

func (p *execProgress) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.stopped = true
	if p.timer != nil {
		p.timer.Stop()
	}
	if p.shown {
		_, _ = fmt.Fprint(p.out, "\r\033[K")
	}
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
	fs.BoolVar(&options.fresh, "fresh", false, "")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.Usage()
			return execOptions{}, &execExitError{code: 0}
		}
		return execOptions{}, fmt.Errorf("%w (see drive9 exec --help)", err)
	}
	options.workspaceSet = flagProvided(fs, "w") || flagProvided(fs, "workspace")
	options.cwdSet = flagProvided(fs, "C") || flagProvided(fs, "cwd")
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

func (options execOptions) request(rt execRuntime) (client.ExecRequest, error) {
	root, err := runtimeWorkspaceRoot(options.workspace)
	if err != nil {
		return client.ExecRequest{}, err
	}
	if err := validateRuntimeLayerID(options.layer); err != nil {
		return client.ExecRequest{}, err
	}
	lookup := rt.lookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	env, err := runtimeEnvironment(options.env, lookup)
	if err != nil {
		return client.ExecRequest{}, err
	}
	env = withColorEnvironment(env, rt.stdoutTTY, lookup)
	request := client.ExecRequest{
		Cwd: options.cwd, Env: env, Fresh: options.fresh,
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

// withColorEnvironment keeps colored output when the user is watching a
// terminal: the remote process has no TTY, so most tools would turn color off.
// NO_COLOR on this side wins, and explicit -e values are never overridden.
func withColorEnvironment(env map[string]string, stdoutTTY bool, lookup func(string) (string, bool)) map[string]string {
	if !stdoutTTY {
		return env
	}
	set := func(key, value string) {
		if env == nil {
			env = map[string]string{}
		}
		if _, exists := env[key]; !exists {
			env[key] = value
		}
	}
	if _, noColor := lookup("NO_COLOR"); noColor {
		set("NO_COLOR", "1")
		return env
	}
	set("FORCE_COLOR", "1")
	set("CLICOLOR_FORCE", "1")
	term := "xterm-256color"
	if local, ok := lookup("TERM"); ok && local != "" && local != "dumb" {
		term = local
	}
	set("TERM", term)
	return env
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

// runtimeWorkspaceRoot accepts ":/repo", "/repo", and "repo".
func runtimeWorkspaceRoot(raw string) (string, error) {
	raw = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), ":"))
	if strings.ContainsRune(raw, '\x00') || strings.HasPrefix(raw, "..") {
		return "", fmt.Errorf("--workspace must be a Drive9 path such as :/repo, got %q", raw)
	}
	return path.Clean("/" + raw), nil
}

// runtimeEnvironment parses KEY=VALUE entries. A bare KEY copies the value
// from the local environment, like `docker run -e KEY`.
func runtimeEnvironment(entries []string, lookup func(string) (string, bool)) (map[string]string, error) {
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
			local, ok := lookup(key)
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
