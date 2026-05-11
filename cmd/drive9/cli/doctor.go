package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type doctorStatus string

const (
	doctorPass doctorStatus = "PASS"
	doctorFail doctorStatus = "FAIL"
)

type doctorCheck struct {
	name   string
	status doctorStatus
	detail string
	fix    string
}

type doctorFailure struct {
	failed int
}

func (e doctorFailure) Error() string {
	if e.failed == 1 {
		return "doctor fuse found 1 failing check"
	}
	return fmt.Sprintf("doctor fuse found %d failing checks", e.failed)
}

func (e doctorFailure) ExitCode() int { return 1 }

type doctorDeps struct {
	goos               string
	goarch             string
	stdout             io.Writer
	lookupPath         func(string) (string, error)
	stat               func(string) (os.FileInfo, error)
	readFile           func(string) ([]byte, error)
	mkdirAll           func(string, os.FileMode) error
	writeFile          func(string, []byte, os.FileMode) error
	remove             func(string) error
	access             func(string, uint32) error
	currentUser        func() (*user.User, error)
	getgroups          func() ([]int, error)
	commandOutput      func(context.Context, string, ...string) ([]byte, error)
	resolveCredentials func() ResolvedCredentials
	httpGet            func(context.Context, string, string) (int, error)
	isMountpoint       func(string) (bool, error)
}

func defaultDoctorDeps() doctorDeps {
	return doctorDeps{
		goos:        runtime.GOOS,
		goarch:      runtime.GOARCH,
		stdout:      os.Stdout,
		lookupPath:  exec.LookPath,
		stat:        os.Stat,
		readFile:    os.ReadFile,
		mkdirAll:    os.MkdirAll,
		writeFile:   os.WriteFile,
		remove:      os.Remove,
		access:      doctorAccess,
		currentUser: user.Current,
		getgroups:   os.Getgroups,
		commandOutput: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).CombinedOutput()
		},
		resolveCredentials: ResolveCredentials,
		httpGet:            doctorHTTPGet,
		isMountpoint:       isMountpoint,
	}
}

// Doctor handles the `drive9 doctor` command.
func Doctor(args []string) error {
	return runDoctor(args, defaultDoctorDeps())
}

func runDoctor(args []string, deps doctorDeps) error {
	if deps.stdout == nil {
		deps.stdout = io.Discard
	}
	if len(args) == 0 {
		return errors.New(doctorUsage())
	}
	switch args[0] {
	case "fuse":
		return runDoctorFuse(args[1:], deps)
	case "-h", "-help", "--help", "help":
		_, _ = fmt.Fprint(deps.stdout, doctorUsage())
		return nil
	default:
		return fmt.Errorf("unknown doctor command %q\n%s", args[0], doctorUsage())
	}
}

func doctorUsage() string {
	return `usage: drive9 doctor <fuse>

commands:
  fuse    diagnose local FUSE prerequisites for drive9 mount
`
}

func runDoctorFuse(args []string, deps doctorDeps) error {
	if deps.stdout == nil {
		deps.stdout = io.Discard
	}

	fs := flag.NewFlagSet("doctor fuse", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mountpoint := fs.String("mountpoint", "/mnt/drive9", "mountpoint path to inspect")
	cacheDir := fs.String("cache-dir", defaultDoctorCacheDir(), "cache directory to inspect")
	server := fs.String("server", "", "drive9 server URL to check")
	timeout := fs.Duration("timeout", 3*time.Second, "server connectivity timeout")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("usage: drive9 doctor fuse [--mountpoint path] [--cache-dir path] [--server url] [--timeout duration]")
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: drive9 doctor fuse [--mountpoint path] [--cache-dir path] [--server url] [--timeout duration]")
	}
	if *timeout <= 0 {
		return fmt.Errorf("doctor fuse: --timeout must be > 0")
	}

	if deps.goos == "windows" {
		checks := []doctorCheck{
			doctorOSKernelCheck(context.Background(), deps),
			{
				name:   "fuse support",
				status: doctorFail,
				detail: "drive9 FUSE mounts are not supported on Windows",
				fix:    "use `drive9 mount` without `--mode=fuse` on Windows (the default path uses WebDAV), or use Linux/macOS for FUSE mounts",
			},
		}

		_, _ = fmt.Fprintln(deps.stdout, "drive9 doctor fuse")
		for _, check := range checks {
			_, _ = fmt.Fprintf(deps.stdout, "%s %s: %s\n", check.status, check.name, check.detail)
			if strings.TrimSpace(check.fix) != "" {
				_, _ = fmt.Fprintf(deps.stdout, "  fix: %s\n", check.fix)
			}
		}
		return doctorFailure{failed: 1}
	}

	creds := deps.resolveCredentials()
	if strings.TrimSpace(*server) != "" {
		creds.Server = strings.TrimSpace(*server)
		creds.ServerSource = "flag:--server"
	}

	checks := []doctorCheck{
		doctorOSKernelCheck(context.Background(), deps),
		doctorEnvironmentCheck(deps),
		doctorCurrentUserCheck(deps),
		doctorFuseDeviceCheck(deps),
		doctorFuseBinaryCheck(deps),
		doctorUnmountHelperCheck(deps, *mountpoint),
		doctorFuseConfCheck(deps),
		doctorMountpointCheck(deps, *mountpoint),
		doctorCacheDirCheck(deps, *cacheDir),
		doctorCredentialsCheck(creds),
		doctorServerCheck(context.Background(), deps, creds, *timeout),
	}

	_, _ = fmt.Fprintln(deps.stdout, "drive9 doctor fuse")
	failures := 0
	for _, check := range checks {
		_, _ = fmt.Fprintf(deps.stdout, "%s %s: %s\n", check.status, check.name, check.detail)
		if check.status == doctorFail {
			failures++
			if strings.TrimSpace(check.fix) != "" {
				_, _ = fmt.Fprintf(deps.stdout, "  fix: %s\n", check.fix)
			}
		}
	}
	if failures > 0 {
		return doctorFailure{failed: failures}
	}
	return nil
}

func defaultDoctorCacheDir() string {
	if dir, err := os.UserCacheDir(); err == nil && dir != "" {
		return filepath.Join(dir, "drive9")
	}
	return filepath.Join(os.TempDir(), "drive9-cache")
}

func doctorOSKernelCheck(ctx context.Context, deps doctorDeps) doctorCheck {
	detail := deps.goos + "/" + deps.goarch
	if deps.commandOutput != nil {
		if out, err := deps.commandOutput(ctx, "uname", "-a"); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				detail += " " + s
			}
		}
	}
	return doctorCheck{name: "os/kernel", status: doctorPass, detail: detail}
}

func doctorEnvironmentCheck(deps doctorDeps) doctorCheck {
	var detected []string
	if deps.stat != nil {
		if _, err := deps.stat("/.dockerenv"); err == nil {
			detected = append(detected, "docker")
		}
		if _, err := deps.stat("/run/.containerenv"); err == nil {
			detected = append(detected, "container")
		}
	}
	if deps.readFile != nil {
		if b, err := deps.readFile("/proc/1/cgroup"); err == nil {
			s := strings.ToLower(string(b))
			for _, marker := range []string{"docker", "kubepods", "containerd"} {
				if strings.Contains(s, marker) {
					detected = appendIfMissing(detected, marker)
				}
			}
		}
	}
	for _, key := range []string{"E2B_SANDBOX_ID", "E2B_API_KEY"} {
		if os.Getenv(key) != "" {
			detected = appendIfMissing(detected, "e2b")
		}
	}
	if len(detected) == 0 {
		return doctorCheck{name: "environment", status: doctorPass, detail: "no container or E2B marker detected"}
	}
	return doctorCheck{name: "environment", status: doctorPass, detail: strings.Join(detected, ", ") + " detected"}
}

func doctorCurrentUserCheck(deps doctorDeps) doctorCheck {
	if deps.currentUser == nil {
		return doctorCheck{name: "current user", status: doctorPass, detail: "not inspected"}
	}
	u, err := deps.currentUser()
	if err != nil {
		return doctorCheck{name: "current user", status: doctorFail, detail: err.Error(), fix: "ensure the current user exists in passwd/NSS"}
	}
	detail := u.Username
	if u.Uid != "" {
		detail += " uid=" + u.Uid
	}
	if deps.getgroups != nil {
		if groups, err := deps.getgroups(); err == nil {
			detail += " groups=" + strconv.Itoa(len(groups))
		}
	}
	return doctorCheck{name: "current user", status: doctorPass, detail: detail}
}

func doctorFuseDeviceCheck(deps doctorDeps) doctorCheck {
	if deps.goos != "linux" {
		return doctorCheck{name: "/dev/fuse", status: doctorPass, detail: "not required on " + deps.goos}
	}
	info, err := deps.stat("/dev/fuse")
	if err != nil {
		return doctorCheck{name: "/dev/fuse", status: doctorFail, detail: err.Error(), fix: "run in an environment with /dev/fuse exposed, or enable FUSE device access for the container/sandbox"}
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return doctorCheck{name: "/dev/fuse", status: doctorFail, detail: "exists but is not a character device", fix: "recreate /dev/fuse using the host FUSE device"}
	}
	if deps.access != nil {
		if err := deps.access("/dev/fuse", 0x6); err != nil {
			return doctorCheck{name: "/dev/fuse", status: doctorFail, detail: "current user cannot read/write /dev/fuse: " + err.Error(), fix: "add the user to the fuse group, adjust /dev/fuse permissions, or run the sandbox with FUSE privileges"}
		}
	}
	return doctorCheck{name: "/dev/fuse", status: doctorPass, detail: "character device is present and accessible"}
}

func doctorFuseBinaryCheck(deps doctorDeps) doctorCheck {
	switch deps.goos {
	case "linux":
		for _, name := range []string{"fusermount3", "fusermount"} {
			if path, err := deps.lookupPath(name); err == nil {
				return doctorCheck{name: "fusermount", status: doctorPass, detail: name + " at " + path}
			}
		}
		return doctorCheck{name: "fusermount", status: doctorFail, detail: "fusermount3 not found in PATH", fix: "install fuse3 (for example: apt-get install -y fuse3)"}
	case "darwin":
		for _, name := range []string{"mount_macfuse", "mount_fusefs"} {
			if path, err := deps.lookupPath(name); err == nil {
				return doctorCheck{name: "fuse binary", status: doctorPass, detail: name + " at " + path}
			}
		}
		return doctorCheck{name: "fuse binary", status: doctorFail, detail: "macFUSE mount helper not found in PATH", fix: "install macFUSE and ensure its mount helper is available"}
	default:
		return doctorCheck{name: "fuse binary", status: doctorFail, detail: "unsupported OS " + deps.goos, fix: "run drive9 mount on Linux or macOS with FUSE installed"}
	}
}

func doctorUnmountHelperCheck(deps doctorDeps, mountpoint string) doctorCheck {
	argv, err := umountArgv(deps.goos, deps.lookupPath, mountpoint)
	if err != nil {
		return doctorCheck{name: "unmount helper", status: doctorFail, detail: err.Error(), fix: "install fuse3 or ensure umount is available in PATH"}
	}
	return doctorCheck{name: "unmount helper", status: doctorPass, detail: strings.Join(argv, " ")}
}

func doctorFuseConfCheck(deps doctorDeps) doctorCheck {
	if deps.goos != "linux" {
		return doctorCheck{name: "/etc/fuse.conf user_allow_other", status: doctorPass, detail: "not required on " + deps.goos}
	}
	b, err := deps.readFile("/etc/fuse.conf")
	if err != nil {
		return doctorCheck{name: "/etc/fuse.conf user_allow_other", status: doctorFail, detail: err.Error(), fix: "create /etc/fuse.conf with a line containing user_allow_other if you need --allow-other mounts"}
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "user_allow_other" {
			return doctorCheck{name: "/etc/fuse.conf user_allow_other", status: doctorPass, detail: "enabled"}
		}
	}
	return doctorCheck{name: "/etc/fuse.conf user_allow_other", status: doctorFail, detail: "user_allow_other is not enabled", fix: "add `user_allow_other` to /etc/fuse.conf before using drive9 mount --allow-other"}
}

func doctorMountpointCheck(deps doctorDeps, mountpoint string) doctorCheck {
	mountpoint = strings.TrimSpace(mountpoint)
	if mountpoint == "" {
		return doctorCheck{name: "mountpoint", status: doctorFail, detail: "empty path", fix: "pass --mountpoint <path>"}
	}
	info, err := deps.stat(mountpoint)
	if err != nil {
		return doctorCheck{name: "mountpoint", status: doctorFail, detail: err.Error(), fix: "create the mountpoint directory: mkdir -p " + mountpoint}
	}
	if !info.IsDir() {
		return doctorCheck{name: "mountpoint", status: doctorFail, detail: "path is not a directory", fix: "choose or create an empty directory for the mountpoint"}
	}
	if deps.isMountpoint != nil {
		mounted, err := deps.isMountpoint(mountpoint)
		if err != nil {
			return doctorCheck{name: "mountpoint", status: doctorFail, detail: err.Error(), fix: "check mountpoint permissions and parent directory state"}
		}
		if mounted {
			return doctorCheck{name: "mountpoint", status: doctorPass, detail: mountpoint + " is already mounted"}
		}
	}
	return doctorCheck{name: "mountpoint", status: doctorPass, detail: mountpoint + " is an available directory"}
}

func doctorCacheDirCheck(deps doctorDeps, dir string) doctorCheck {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return doctorCheck{name: "cache dir", status: doctorFail, detail: "empty path", fix: "pass --cache-dir <path>"}
	}
	if err := deps.mkdirAll(dir, 0o700); err != nil {
		return doctorCheck{name: "cache dir", status: doctorFail, detail: err.Error(), fix: "choose a writable cache directory or fix permissions"}
	}
	probe := filepath.Join(dir, ".drive9-doctor-write-test")
	if err := deps.writeFile(probe, []byte("ok\n"), 0o600); err != nil {
		return doctorCheck{name: "cache dir", status: doctorFail, detail: err.Error(), fix: "choose a writable cache directory or fix permissions"}
	}
	if err := deps.remove(probe); err != nil {
		return doctorCheck{name: "cache dir", status: doctorFail, detail: err.Error(), fix: "remove stale files or fix cache directory permissions"}
	}
	return doctorCheck{name: "cache dir", status: doctorPass, detail: dir + " is writable"}
}

func doctorCredentialsCheck(creds ResolvedCredentials) doctorCheck {
	switch creds.Kind {
	case CredentialOwner:
		return doctorCheck{name: "credentials", status: doctorPass, detail: "owner credential from " + creds.CredSource}
	case CredentialDelegated:
		return doctorCheck{name: "credentials", status: doctorPass, detail: "delegated credential from " + creds.CredSource}
	default:
		return doctorCheck{name: "credentials", status: doctorFail, detail: "no drive9 credential resolved", fix: "set DRIVE9_API_KEY or DRIVE9_VAULT_TOKEN, or configure a context with drive9 ctx add/import/use"}
	}
}

func doctorServerCheck(ctx context.Context, deps doctorDeps, creds ResolvedCredentials, timeout time.Duration) doctorCheck {
	server := strings.TrimSpace(creds.Server)
	if server == "" {
		return doctorCheck{name: "server connectivity", status: doctorFail, detail: "empty server URL", fix: "set DRIVE9_SERVER or pass --server <url>"}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	status, err := deps.httpGet(ctx, strings.TrimRight(server, "/")+"/healthz", "")
	if err != nil {
		return doctorCheck{name: "server connectivity", status: doctorFail, detail: err.Error(), fix: "check DRIVE9_SERVER, DNS, TLS, and network access"}
	}
	if status < 200 || status >= 300 {
		return doctorCheck{name: "server connectivity", status: doctorFail, detail: fmt.Sprintf("%s /healthz returned HTTP %d", server, status), fix: "check that the drive9 server is running and reachable"}
	}
	return doctorCheck{name: "server connectivity", status: doctorPass, detail: fmt.Sprintf("%s /healthz returned HTTP %d (%s)", server, status, creds.ServerSource)}
}

func doctorHTTPGet(ctx context.Context, url string, bearer string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, nil
}

func appendIfMissing(items []string, item string) []string {
	for _, existing := range items {
		if existing == item {
			return items
		}
	}
	return append(items, item)
}
