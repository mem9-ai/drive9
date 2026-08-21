package cli

import (
	"runtime"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestValidateObjectMount(t *testing.T) {
	if err := validateObjectMount("linux", "fuse", "", "", ""); err != nil {
		t.Fatalf("linux fuse: %v", err)
	}
	if err := validateObjectMount("windows", "fuse", "", "", ""); err == nil {
		t.Fatal("windows should fail")
	}
	if err := validateObjectMount("linux", "webdav", "", "", ""); err == nil || !strings.Contains(err.Error(), "webdav") {
		t.Fatalf("webdav err=%v", err)
	}
	if err := validateObjectMount("darwin", "auto", "", "", ""); err == nil || !strings.Contains(err.Error(), "--mode=fuse") {
		t.Fatalf("darwin auto err=%v", err)
	}
	if err := validateObjectMount("darwin", "fuse", "", "", ""); err != nil {
		t.Fatalf("darwin fuse: %v", err)
	}
	if err := validateObjectMount("linux", "fuse", "layer-1", "", ""); err == nil {
		t.Fatal("layer should fail")
	}
	if err := validateObjectMount("linux", "fuse", "", "cp1", ""); err == nil {
		t.Fatal("checkpoint should fail")
	}
	if err := validateObjectMount("linux", "fuse", "", "", "coding-agent"); err == nil {
		t.Fatal("profile should fail")
	}
	if err := validateObjectMount("linux", "fuse", "", "", "none"); err != nil {
		t.Fatalf("profile=none: %v", err)
	}
}

func TestMountObjectStartsSupervisedByDefault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("object mounts are unsupported on Windows")
	}
	oldStartMountBackground := startMountBackground
	oldStartSupervised := startMountSupervisedBackground
	t.Cleanup(func() {
		startMountBackground = oldStartMountBackground
		startMountSupervisedBackground = oldStartSupervised
	})

	var gotSupervised mountSuperviseStartRequest
	var gotLegacy mountBackgroundRequest
	startMountSupervisedBackground = func(req mountSuperviseStartRequest) error {
		gotSupervised = req
		return nil
	}
	startMountBackground = func(req mountBackgroundRequest) error {
		gotLegacy = req
		return nil
	}

	mountPoint := t.TempDir()
	err := MountCmd([]string{
		"--mode", "fuse",
		"s3://bucket/prefix/",
		mountPoint,
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if gotSupervised.MountPoint != mountPoint {
		t.Fatalf("MountPoint = %q, want %q", gotSupervised.MountPoint, mountPoint)
	}
	if gotSupervised.RemoteRoot != "s3://bucket/prefix/" {
		t.Fatalf("RemoteRoot = %q", gotSupervised.RemoteRoot)
	}
	if gotSupervised.MountKind != mountstate.MountKindObject {
		t.Fatalf("MountKind = %q, want %q", gotSupervised.MountKind, mountstate.MountKindObject)
	}
	if gotSupervised.Profile != "none" {
		t.Fatalf("Profile = %q, want none", gotSupervised.Profile)
	}
	if gotLegacy.MountPoint != "" {
		t.Fatal("legacy startMountBackground should not run for default supervised object mount")
	}
}

func TestMountObjectNoSuperviseUsesLegacyBackground(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("object mounts are unsupported on Windows")
	}
	oldStartMountBackground := startMountBackground
	oldStartSupervised := startMountSupervisedBackground
	t.Cleanup(func() {
		startMountBackground = oldStartMountBackground
		startMountSupervisedBackground = oldStartSupervised
	})

	var got mountBackgroundRequest
	startMountBackground = func(req mountBackgroundRequest) error {
		got = req
		return nil
	}
	startMountSupervisedBackground = func(req mountSuperviseStartRequest) error {
		t.Fatal("supervised path should not run with --no-supervise")
		return nil
	}

	mountPoint := t.TempDir()
	err := MountCmd([]string{
		"--mode", "fuse",
		"--no-supervise",
		"s3://bucket/prefix/",
		mountPoint,
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if got.MountPoint != mountPoint {
		t.Fatalf("MountPoint = %q, want %q", got.MountPoint, mountPoint)
	}
}

func TestMountObjectForegroundPassesCacheDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("object mounts are unsupported on Windows")
	}
	old := mountObjectStore
	t.Cleanup(func() { mountObjectStore = old })

	var gotCache string
	mountObjectStore = func(loc *Location, mountPoint, cacheDir string, readOnly, debug, supervised, allowOther bool) error {
		if loc.Raw != "s3://bucket/prefix/" {
			t.Fatalf("loc = %q", loc.Raw)
		}
		gotCache = cacheDir
		return nil
	}

	err := MountCmd([]string{
		"--foreground",
		"--mode", "fuse",
		"--cache-dir", "/tmp/drive9-obj-cache",
		"s3://bucket/prefix/",
		t.TempDir(),
	})
	if err != nil {
		t.Fatalf("MountCmd: %v", err)
	}
	if gotCache != "/tmp/drive9-obj-cache" {
		t.Fatalf("cacheDir = %q", gotCache)
	}
}

func TestSuperviseCommandArgsIncludesObjectMountKind(t *testing.T) {
	args, err := superviseCommandArgs(mountSuperviseStartRequest{
		MountPoint:   "/mnt/obj",
		RemoteRoot:   "s3://bucket/prefix/",
		MountKind:    mountstate.MountKindObject,
		OriginalArgs: []string{"s3://bucket/prefix/", "/mnt/obj"},
	}, "/tmp/drive9-obj.log")
	if err != nil {
		t.Fatal(err)
	}
	kind := ""
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--mount-kind" {
			kind = args[i+1]
		}
	}
	if kind != mountstate.MountKindObject {
		t.Fatalf("supervise args %v: --mount-kind=%q", args, kind)
	}
}
