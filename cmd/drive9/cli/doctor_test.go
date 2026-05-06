package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/user"
	"strings"
	"testing"
	"time"
)

type doctorFakeInfo struct {
	name string
	mode os.FileMode
}

func (i doctorFakeInfo) Name() string       { return i.name }
func (i doctorFakeInfo) Size() int64        { return 0 }
func (i doctorFakeInfo) Mode() os.FileMode  { return i.mode }
func (i doctorFakeInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (i doctorFakeInfo) IsDir() bool        { return i.mode.IsDir() }
func (i doctorFakeInfo) Sys() any           { return nil }

func doctorTestDeps(out *bytes.Buffer) doctorDeps {
	return doctorDeps{
		goos:   "linux",
		goarch: "amd64",
		stdout: out,
		lookupPath: func(name string) (string, error) {
			if name == "fusermount3" {
				return "/usr/bin/fusermount3", nil
			}
			return "", os.ErrNotExist
		},
		stat: func(path string) (os.FileInfo, error) {
			switch path {
			case "/dev/fuse":
				return doctorFakeInfo{name: "fuse", mode: os.ModeCharDevice | 0o666}, nil
			case "/mnt/drive9":
				return doctorFakeInfo{name: "drive9", mode: os.ModeDir | 0o755}, nil
			case "/.dockerenv", "/run/.containerenv":
				return nil, os.ErrNotExist
			default:
				return nil, os.ErrNotExist
			}
		},
		readFile: func(path string) ([]byte, error) {
			switch path {
			case "/etc/fuse.conf":
				return []byte("# comment\nuser_allow_other\n"), nil
			case "/proc/1/cgroup":
				return []byte("0::/user.slice\n"), nil
			default:
				return nil, os.ErrNotExist
			}
		},
		mkdirAll: func(string, os.FileMode) error { return nil },
		writeFile: func(string, []byte, os.FileMode) error {
			return nil
		},
		remove: func(string) error { return nil },
		access: func(string, uint32) error { return nil },
		currentUser: func() (*user.User, error) {
			return &user.User{Username: "tester", Uid: "1000"}, nil
		},
		getgroups: func() ([]int, error) { return []int{20, 1000}, nil },
		commandOutput: func(context.Context, string, ...string) ([]byte, error) {
			return []byte("Linux test 6.0\n"), nil
		},
		resolveCredentials: func() ResolvedCredentials {
			return ResolvedCredentials{
				Kind:         CredentialOwner,
				Server:       "https://api.test",
				APIKey:       "d9_test",
				CredSource:   "env:DRIVE9_API_KEY",
				ServerSource: "env:DRIVE9_SERVER",
			}
		},
		httpGet: func(context.Context, string, string) (int, error) {
			return 200, nil
		},
		isMountpoint: func(string) (bool, error) { return false, nil },
	}
}

func TestRunDoctorFuseAllPass(t *testing.T) {
	var out bytes.Buffer
	err := runDoctor([]string{"fuse"}, doctorTestDeps(&out))
	if err != nil {
		t.Fatalf("runDoctor: %v\n%s", err, out.String())
	}
	for _, want := range []string{
		"drive9 doctor fuse",
		"PASS os/kernel:",
		"PASS /dev/fuse:",
		"PASS fusermount:",
		"PASS unmount helper:",
		"PASS /etc/fuse.conf user_allow_other:",
		"PASS mountpoint:",
		"PASS cache dir:",
		"PASS credentials:",
		"PASS server connectivity:",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunDoctorFuseMissingDeviceFailsWithFix(t *testing.T) {
	var out bytes.Buffer
	deps := doctorTestDeps(&out)
	deps.stat = func(path string) (os.FileInfo, error) {
		switch path {
		case "/dev/fuse":
			return nil, os.ErrNotExist
		case "/mnt/drive9":
			return doctorFakeInfo{name: "drive9", mode: os.ModeDir | 0o755}, nil
		default:
			return nil, os.ErrNotExist
		}
	}
	err := runDoctor([]string{"fuse"}, deps)
	if err == nil {
		t.Fatal("expected missing /dev/fuse to fail")
	}
	var failure doctorFailure
	if !errors.As(err, &failure) || failure.failed != 1 {
		t.Fatalf("err = %#v, want one doctorFailure", err)
	}
	if !strings.Contains(out.String(), "FAIL /dev/fuse:") || !strings.Contains(out.String(), "fix: run in an environment with /dev/fuse exposed") {
		t.Fatalf("output missing /dev/fuse failure and fix:\n%s", out.String())
	}
}

func TestRunDoctorFuseMissingCredentialsFails(t *testing.T) {
	var out bytes.Buffer
	deps := doctorTestDeps(&out)
	deps.resolveCredentials = func() ResolvedCredentials {
		return ResolvedCredentials{Kind: CredentialNone, Server: "https://api.test", ServerSource: "default"}
	}
	err := runDoctor([]string{"fuse"}, deps)
	if err == nil {
		t.Fatal("expected missing credentials to fail")
	}
	if !strings.Contains(out.String(), "FAIL credentials: no drive9 credential resolved") {
		t.Fatalf("output missing credentials failure:\n%s", out.String())
	}
}

func TestRunDoctorFuseServerFailureFails(t *testing.T) {
	var out bytes.Buffer
	deps := doctorTestDeps(&out)
	deps.httpGet = func(context.Context, string, string) (int, error) {
		return 503, nil
	}
	err := runDoctor([]string{"fuse"}, deps)
	if err == nil {
		t.Fatal("expected server failure to fail")
	}
	if !strings.Contains(out.String(), "FAIL server connectivity:") || !strings.Contains(out.String(), "HTTP 503") {
		t.Fatalf("output missing server failure:\n%s", out.String())
	}
}

func TestRunDoctorFuseMountpointFileFails(t *testing.T) {
	var out bytes.Buffer
	deps := doctorTestDeps(&out)
	deps.stat = func(path string) (os.FileInfo, error) {
		switch path {
		case "/dev/fuse":
			return doctorFakeInfo{name: "fuse", mode: os.ModeCharDevice | 0o666}, nil
		case "/mnt/drive9":
			return doctorFakeInfo{name: "drive9", mode: 0o644}, nil
		default:
			return nil, os.ErrNotExist
		}
	}
	err := runDoctor([]string{"fuse"}, deps)
	if err == nil {
		t.Fatal("expected file mountpoint to fail")
	}
	if !strings.Contains(out.String(), "FAIL mountpoint: path is not a directory") {
		t.Fatalf("output missing mountpoint failure:\n%s", out.String())
	}
}

func TestRunDoctorFuseDarwinDoesNotRequireLinuxFuseConf(t *testing.T) {
	var out bytes.Buffer
	deps := doctorTestDeps(&out)
	deps.goos = "darwin"
	deps.lookupPath = func(name string) (string, error) {
		if name == "mount_macfuse" {
			return "/usr/local/bin/mount_macfuse", nil
		}
		return "", os.ErrNotExist
	}
	err := runDoctor([]string{"fuse"}, deps)
	if err != nil {
		t.Fatalf("runDoctor: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "PASS /etc/fuse.conf user_allow_other: not required on darwin") {
		t.Fatalf("output missing darwin fuse.conf pass:\n%s", out.String())
	}
}

func TestRunDoctorUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"unknown"},
		{"fuse", "extra"},
		{"fuse", "--timeout", "0s"},
	} {
		var out bytes.Buffer
		if err := runDoctor(args, doctorTestDeps(&out)); err == nil {
			t.Fatalf("runDoctor(%v) = nil, want error", args)
		}
	}
}
