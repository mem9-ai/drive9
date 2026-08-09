//go:build !windows

package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/mountsupervisor"
)

// runMountSupervise is the hidden supervisor entrypoint:
// drive9 mount supervise --state ... -- [worker args...]
func runMountSupervise(args []string) error {
	fs := flag.NewFlagSet("mount supervise", flag.ContinueOnError)
	mountPoint := fs.String("mountpoint", "", "mount point")
	logPath := fs.String("log", "", "log file path")
	maxRestarts := fs.Int("max-restarts", 5, "max restarts in restart window")
	restartWindow := fs.Duration("restart-window", 10*time.Minute, "restart budget window")
	healthInterval := fs.Duration("health-interval", 10*time.Second, "health probe interval")
	healthTimeout := fs.Duration("health-timeout", 5*time.Second, "health probe timeout")
	healthFailures := fs.Int("health-failures", 3, "consecutive health failures before restart")
	stopTimeout := fs.Duration("stop-timeout", 60*time.Second, "SIGTERM to SIGKILL grace")
	backoffMax := fs.Duration("restart-backoff-max", 30*time.Second, "max restart backoff")
	alertWebhook := fs.String("alert-webhook", "", "optional alert webhook URL")
	alertFile := fs.String("alert-file", "", "optional alert file path")
	server := fs.String("server", "", "server for status metadata")
	remoteRoot := fs.String("remote-root", "", "remote root for status metadata")
	profile := fs.String("profile", "", "profile for status metadata")
	localRootMeta := fs.String("local-root-meta", "", "local root for pack metadata")
	packPathsJSON := fs.String("pack-paths-json", "", "JSON array of pack paths")
	sanitizedJSON := fs.String("sanitized-args-json", "", "JSON array of sanitized args")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	workerArgs := fs.Args()
	if strings.TrimSpace(*mountPoint) == "" {
		return fmt.Errorf("drive9 mount supervise: --mountpoint is required")
	}
	if len(workerArgs) == 0 {
		return fmt.Errorf("drive9 mount supervise: worker args required after flags")
	}
	// Normalize: worker args should start with "mount".
	if workerArgs[0] != "mount" {
		workerArgs = append([]string{"mount"}, workerArgs...)
	}

	var sanitized []string
	if s := strings.TrimSpace(*sanitizedJSON); s != "" {
		if err := json.Unmarshal([]byte(s), &sanitized); err != nil {
			return fmt.Errorf("drive9 mount supervise: parse sanitized-args-json: %w", err)
		}
	}
	var packPaths []string
	if s := strings.TrimSpace(*packPathsJSON); s != "" {
		if err := json.Unmarshal([]byte(s), &packPaths); err != nil {
			return fmt.Errorf("drive9 mount supervise: parse pack-paths-json: %w", err)
		}
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("drive9 mount supervise: locate executable: %w", err)
	}

	var logFile *os.File
	// Default: keep process stderr/stdout so --supervise-foreground is visible.
	// Background mode already redirects parent fds to the log file, so only tee
	// when those still point at a terminal (or non-log destination).
	stdout, stderr := io.Writer(os.Stdout), io.Writer(os.Stderr)
	if lp := strings.TrimSpace(*logPath); lp != "" {
		if err := os.MkdirAll(filepath.Dir(lp), 0o700); err != nil {
			return err
		}
		logFile, err = os.OpenFile(lp, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		defer func() { _ = logFile.Close() }()
		// Simple size rotation: if > 10MB, rotate once.
		if info, err := logFile.Stat(); err == nil && info.Size() > 10<<20 {
			_ = logFile.Close()
			_ = os.Rename(lp, lp+".1")
			logFile, err = os.OpenFile(lp, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
		}
		_, _ = fmt.Fprintf(logFile, "\n--- drive9 mount supervisor start %s ---\n", time.Now().Format(time.RFC3339))
		// If stdout/stderr are already the log (background detach), don't MultiWriter
		// (would duplicate every line). Tee only when they look like a TTY/pipe.
		if fileIsTerminal(os.Stderr) || fileIsTerminal(os.Stdout) {
			stdout = io.MultiWriter(os.Stdout, logFile)
			stderr = io.MultiWriter(os.Stderr, logFile)
		} else {
			stdout, stderr = logFile, logFile
		}
	}

	// Stale stop tokens from a prior SIGKILL-path umount must not block remount.
	_ = mountstate.ClearStopToken(*mountPoint)

	return mountsupervisor.Run(mountsupervisor.Config{
		MountPoint:     *mountPoint,
		Executable:     exe,
		WorkerArgs:     workerArgs,
		Env:            os.Environ(),
		LogPath:        *logPath,
		Stdout:         stdout,
		Stderr:         stderr,
		MaxRestarts:    *maxRestarts,
		RestartWindow:  *restartWindow,
		HealthInterval: *healthInterval,
		HealthTimeout:  *healthTimeout,
		HealthFailures: *healthFailures,
		StopTimeout:    *stopTimeout,
		BackoffMax:     *backoffMax,
		AlertWebhook:   firstNonEmpty(*alertWebhook, os.Getenv("DRIVE9_MOUNT_ALERT_WEBHOOK")),
		AlertFile:      *alertFile,
		SanitizedArgs:  sanitized,
		Server:         *server,
		RemoteRoot:     *remoteRoot,
		Profile:        *profile,
		LocalRoot:      *localRootMeta,
		PackPaths:      packPaths,
	})
}

func runMountStatus(args []string) error {
	fs := flag.NewFlagSet("mount status", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit JSON")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 mount status [--json] <mountpoint>")
	}
	mp := fs.Arg(0)
	snap := mountsupervisor.CollectStatus(mp)
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(snap)
	}
	fmt.Printf("mountpoint: %s\n", snap.MountPoint)
	fmt.Printf("state: %s\n", snap.State)
	fmt.Printf("healthy: %v\n", snap.Healthy)
	fmt.Printf("supervised: %v\n", snap.Supervised)
	if snap.SupervisorPID > 0 {
		fmt.Printf("supervisor_pid: %d\n", snap.SupervisorPID)
	}
	if snap.WorkerPID > 0 {
		fmt.Printf("worker_pid: %d\n", snap.WorkerPID)
	}
	fmt.Printf("restarts: %d\n", snap.Restarts)
	if snap.LastExitReason != "" {
		fmt.Printf("last_exit: code=%d reason=%s\n", snap.LastExitCode, snap.LastExitReason)
	}
	if snap.LastHealth != "" {
		fmt.Printf("last_health: %s", snap.LastHealth)
		if snap.LastHealthErr != "" {
			fmt.Printf(" (%s)", snap.LastHealthErr)
		}
		fmt.Println()
	}
	if snap.ProbeError != "" {
		fmt.Printf("probe_error: %s\n", snap.ProbeError)
	}
	if snap.LogPath != "" {
		fmt.Printf("log: %s\n", snap.LogPath)
	}
	if !snap.Healthy {
		return cliExitError{code: 1, msg: "mount is not healthy"}
	}
	return nil
}

func runMountHealth(args []string) error {
	fs := flag.NewFlagSet("mount health", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 mount health <mountpoint>")
	}
	mp := fs.Arg(0)
	snap := mountsupervisor.CollectStatus(mp)
	if snap.Healthy {
		fmt.Println("ok")
		return nil
	}
	msg := snap.ProbeError
	if msg == "" {
		msg = "unhealthy"
	}
	fmt.Fprintln(os.Stderr, msg)
	return cliExitError{code: 1, msg: msg}
}

func runMountEnsure(args []string) error {
	fs := flag.NewFlagSet("mount ensure", flag.ContinueOnError)
	reset := fs.Bool("reset", false, "reset circuit breaker before ensure")
	restart := fs.Bool("restart", false, "force restart even if currently healthy")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drive9 mount ensure [--reset] [--restart] <mountpoint>")
	}
	mp := fs.Arg(0)
	if *reset {
		_ = mountsupervisor.ResetCircuit(mp)
	}
	snap := mountsupervisor.CollectStatus(mp)
	if snap.Healthy && !*restart {
		fmt.Fprintf(os.Stderr, "drive9: mount ensure: already healthy at %s\n", mp)
		return nil
	}
	// Clean stale mount if needed.
	if cleaned, err := mountsupervisor.EnsureClean(mp); err != nil {
		return fmt.Errorf("drive9 mount ensure: clean: %w", err)
	} else if cleaned {
		fmt.Fprintf(os.Stderr, "drive9: mount ensure: cleaned stale mount at %s\n", mp)
	}

	// If supervisor still alive and not restart, nothing more.
	if !*restart && snap.SupervisorPID > 0 && processAliveImpl(snap.SupervisorPID) {
		// Wait briefly for self-heal.
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) {
			s2 := mountsupervisor.CollectStatus(mp)
			if s2.Healthy {
				fmt.Fprintf(os.Stderr, "drive9: mount ensure: recovered under existing supervisor\n")
				return nil
			}
			time.Sleep(time.Second)
		}
	}

	// Capture stored args + credentials BEFORE stop clears process state.
	var sanitized []string
	var storedServer, storedAPIKey, storedToken string
	if st, _, err := mountstate.ReadSupervisorState(mp); err == nil && len(st.Args) > 0 {
		sanitized = st.Args
		if st.Server != "" {
			storedServer = st.Server
		}
	}
	if ps, _, err := mountstate.ReadProcessState(mp); err == nil {
		if len(sanitized) == 0 && len(ps.Args) > 0 {
			sanitized = ps.Args
		}
		if storedServer == "" {
			storedServer = ps.Server
		}
		storedAPIKey = ps.APIKey
		storedToken = ps.Token
	}
	if len(sanitized) == 0 {
		return fmt.Errorf("drive9 mount ensure: no stored mount args for %s; run drive9 mount explicitly", mp)
	}
	// Ensure must not re-enter a blocking --supervise-foreground process.
	sanitized = stripEnsureBlockingFlags(sanitized)

	// Stop any existing supervisor/worker first (graceful), verifying identity.
	_ = mountstate.WriteStopToken(mp, "ensure")
	var workerCreation uint64
	var workerPID int
	if sst, _, err := mountstate.ReadSupervisorState(mp); err == nil {
		workerCreation = sst.WorkerCreation
		if sst.WorkerPID > 0 {
			workerPID = sst.WorkerPID
		}
	}
	if ps, _, err := mountstate.ReadProcessState(mp); err == nil {
		if workerPID == 0 && ps.WorkerPID > 0 {
			workerPID = ps.WorkerPID
		}
		// Unsupervised / dead-supervisor leftover: worker may be recorded as PID.
		if workerPID == 0 && ps.PID > 0 && ps.Role != mountstate.RoleSupervisor {
			workerPID = ps.PID
			workerCreation = ps.CreationTime
		}
	}
	if snap.SupervisorPID > 0 {
		var creation uint64
		if ps, _, err := mountstate.ReadProcessState(mp); err == nil {
			if ps.SupervisorPID == snap.SupervisorPID {
				creation = ps.SupervisorCreationTime
				if creation == 0 {
					creation = ps.CreationTime
				}
			}
		}
		if sst, _, err := mountstate.ReadSupervisorState(mp); err == nil && sst.PID == snap.SupervisorPID {
			creation = sst.CreationTime
		}
		if processMatchesIdentity(snap.SupervisorPID, creation) {
			_ = terminateProcessGraceful(snap.SupervisorPID, 30*time.Second)
		}
	}
	// Always attempt worker stop when identity matches (covers dead supervisor +
	// orphan worker, and avoids PID-reuse kill without creation check).
	if workerPID == 0 {
		workerPID = snap.WorkerPID
	}
	if workerPID > 0 && processMatchesIdentity(workerPID, workerCreation) {
		_ = terminateProcessGraceful(workerPID, 30*time.Second)
	}
	_ = mountstate.ClearStopToken(mp)
	_ = mountstate.ClearSupervisorState(mp)
	_ = os.Remove(mountstate.PIDFilePath(mp))
	_, _ = mountsupervisor.EnsureClean(mp)

	// Replay original mount principal via env only — never put --api-key on
	// argv (would land in process listings / sanitized args / systemd units).
	// --server is non-secret and may be injected as a flag for clarity.
	sanitized = injectEnsureServerFlag(sanitized, storedServer)
	return withEnsureCredentialEnv(storedServer, storedAPIKey, storedToken, func() error {
		return fsMountCmdWithBackground(sanitized, true)
	})
}

// injectEnsureServerFlag prepends --server from stored process state when missing.
func injectEnsureServerFlag(args []string, server string) []string {
	out := append([]string(nil), args...)
	if server != "" && !argsHasFlag(out, "--server") {
		out = append([]string{"--server", server}, out...)
	}
	return out
}

func argsHasFlag(args []string, name string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == name {
			return true
		}
		if strings.HasPrefix(a, name+"=") {
			return true
		}
	}
	return false
}

// withEnsureCredentialEnv temporarily installs stored mount credentials into
// the process environment for remount (needed for vault tokens which have no flag).
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

func stripEnsureBlockingFlags(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--supervise-foreground" || a == "--no-supervise" || a == "--foreground" || a == "--supervised":
			continue
		case strings.HasPrefix(a, "--supervise-foreground=") || strings.HasPrefix(a, "--no-supervise=") ||
			strings.HasPrefix(a, "--foreground=") || strings.HasPrefix(a, "--supervised="):
			continue
		default:
			out = append(out, a)
		}
	}
	return out
}

func runMountSystemdUnit(args []string) error {
	// Peel only systemd-unit-specific flags; pass the rest through as mount args
	// so `drive9 mount systemd-unit --mode=fuse :/repo /mnt/d9` works without
	// requiring an undocumented `--` separator.
	install, unitName, rest, err := peelSystemdUnitFlags(args)
	if err != nil {
		return err
	}
	if len(rest) == 0 {
		return fmt.Errorf("usage: drive9 mount systemd-unit [--install] [--name name] [mount flags] <mountpoint>")
	}
	mountPoint := rest[len(rest)-1]
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	// Build ExecStart with --supervise-foreground.
	startArgs := []string{exe, "mount", "--supervise-foreground"}
	startArgs = append(startArgs, rest...)
	// Quote simply; escape % for systemd (%% is a literal percent).
	execStart := systemdEscapePercents(shellJoin(startArgs))
	execStop := systemdEscapePercents(shellJoin([]string{exe, "umount", "--timeout", "60s", mountPoint}))
	desc := systemdEscapePercents(fmt.Sprintf("drive9 FUSE mount for %s", mountPoint))
	unit := fmt.Sprintf(`[Unit]
Description=%s
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%s
ExecStop=%s
Restart=on-failure
RestartSec=2
KillMode=mixed
TimeoutStopSec=70

[Install]
WantedBy=default.target
`, desc, execStart, execStop)

	if !install {
		fmt.Print(unit)
		return nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	dir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := validateSystemdUnitName(unitName); err != nil {
		return err
	}
	path := filepath.Join(dir, unitName+".service")
	// 0600: unit may embed --api-key or other secrets in ExecStart.
	if err := os.WriteFile(path, []byte(unit), 0o600); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s\n", path)
	return nil
}

// peelSystemdUnitFlags extracts --install/--name and leaves mount flags intact.
func peelSystemdUnitFlags(args []string) (install bool, name string, rest []string, err error) {
	name = "drive9-mount"
	rest = make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			rest = append(rest, args[i+1:]...)
			return install, name, rest, nil
		case a == "--install":
			install = true
		case a == "--install=true" || a == "--install=1":
			install = true
		case a == "--install=false" || a == "--install=0":
			install = false
		case strings.HasPrefix(a, "--install="):
			return false, "", nil, fmt.Errorf("drive9 mount systemd-unit: invalid --install value")
		case a == "--name":
			if i+1 >= len(args) {
				return false, "", nil, fmt.Errorf("drive9 mount systemd-unit: --name requires a value")
			}
			i++
			name = args[i]
		case strings.HasPrefix(a, "--name="):
			name = strings.TrimPrefix(a, "--name=")
			if name == "" {
				return false, "", nil, fmt.Errorf("drive9 mount systemd-unit: --name requires a value")
			}
		case a == "-h" || a == "--help":
			return false, "", nil, fmt.Errorf("usage: drive9 mount systemd-unit [--install] [--name name] [mount flags] <mountpoint>")
		default:
			rest = append(rest, a)
		}
	}
	if err := validateSystemdUnitName(name); err != nil {
		return false, "", nil, err
	}
	return install, name, rest, nil
}

func validateSystemdUnitName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("drive9 mount systemd-unit: --name requires a value")
	}
	if strings.ContainsAny(name, `/\`) || strings.Contains(name, "..") || filepath.IsAbs(name) {
		return fmt.Errorf("drive9 mount systemd-unit: --name must be a simple unit basename, got %q", name)
	}
	if name != filepath.Base(name) {
		return fmt.Errorf("drive9 mount systemd-unit: --name must be a simple unit basename, got %q", name)
	}
	return nil
}

func systemdEscapePercents(s string) string {
	return strings.ReplaceAll(s, "%", "%%")
}

func shellJoin(parts []string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" || strings.ContainsAny(p, " \t\"'\\") {
			out = append(out, "'"+strings.ReplaceAll(p, "'", `'\''`)+"'")
		} else {
			out = append(out, p)
		}
	}
	return strings.Join(out, " ")
}

type cliExitError struct {
	code int
	msg  string
}

func (e cliExitError) Error() string  { return e.msg }
func (e cliExitError) ExitCode() int { return e.code }

func fileIsTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
