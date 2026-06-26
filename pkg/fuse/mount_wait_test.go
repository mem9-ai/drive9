package fuse

import (
	"errors"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func TestProbeMountPointReadySucceedsOnUsableDir(t *testing.T) {
	dir := t.TempDir()
	if err := probeMountPointReady(dir); err != nil {
		t.Fatalf("probeMountPointReady(%q) = %v, want nil", dir, err)
	}
}

func TestProbeMountPointReadyFailsOnMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if err := probeMountPointReady(missing); err == nil {
		t.Fatal("probeMountPointReady(missing) = nil, want error")
	}
}

func TestProbeMountPointReadyRejectsEmptyMountPoint(t *testing.T) {
	if err := probeMountPointReady("   "); err == nil {
		t.Fatal("probeMountPointReady(blank) = nil, want error")
	}
}

func TestServeWaitMountThenStartWatchersStartsWatchersAfterWaitMount(t *testing.T) {
	var events []string
	watchersStarted := false

	err := serveWaitMountThenStartWatchers(func() {}, func() error {
		if watchersStarted {
			t.Fatal("watchers started before WaitMount returned")
		}
		events = append(events, "wait_mount")
		return nil
	}, func() {
		events = append(events, "watchers")
		watchersStarted = true
	}, nil)

	if err != nil {
		t.Fatalf("serveWaitMountThenStartWatchers() error = %v, want nil", err)
	}
	if !watchersStarted {
		t.Fatal("watchers were not started")
	}
	if want := []string{"wait_mount", "watchers"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestServeWaitMountThenStartWatchersStartsWatchersAfterAcceptedWaitMountError(t *testing.T) {
	waitErr := errors.New("permission denied")
	var events []string

	err := serveWaitMountThenStartWatchers(func() {}, func() error {
		events = append(events, "wait_mount")
		return waitErr
	}, func() {
		events = append(events, "watchers")
	}, func(err error) error {
		if !errors.Is(err, waitErr) {
			t.Fatalf("waitMount error = %v, want %v", err, waitErr)
		}
		events = append(events, "handler")
		return nil
	})

	if err != nil {
		t.Fatalf("serveWaitMountThenStartWatchers() error = %v, want nil", err)
	}
	if want := []string{"wait_mount", "handler", "watchers"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestServeWaitMountThenStartWatchersDoesNotStartWatchersAfterRejectedWaitMountError(t *testing.T) {
	waitErr := errors.New("permission denied")
	wantErr := errors.New("mount not ready")
	watchersStarted := false

	err := serveWaitMountThenStartWatchers(func() {}, func() error {
		return waitErr
	}, func() {
		watchersStarted = true
	}, func(err error) error {
		if !errors.Is(err, waitErr) {
			t.Fatalf("waitMount error = %v, want %v", err, waitErr)
		}
		return wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("serveWaitMountThenStartWatchers() error = %v, want %v", err, wantErr)
	}
	if watchersStarted {
		t.Fatal("watchers started after rejected WaitMount error")
	}
}

func TestShouldContinueAfterWaitMountPermissionErrorWhenProbePasses(t *testing.T) {
	called := false
	ok, probeErr := shouldContinueAfterWaitMountPermissionError(syscall.EACCES, "/mnt/drive9", func(mountPoint string) error {
		called = true
		if mountPoint != "/mnt/drive9" {
			t.Fatalf("mountPoint = %q, want /mnt/drive9", mountPoint)
		}
		return nil
	})

	if !ok {
		t.Fatal("ok = false, want true")
	}
	if probeErr != nil {
		t.Fatalf("probeErr = %v, want nil", probeErr)
	}
	if !called {
		t.Fatal("probe was not called")
	}
}

func TestShouldContinueAfterWaitMountPermissionErrorWhenProbeFails(t *testing.T) {
	wantErr := errors.New("not ready")

	ok, probeErr := shouldContinueAfterWaitMountPermissionError(syscall.EPERM, "/mnt/drive9", func(string) error {
		return wantErr
	})

	if ok {
		t.Fatal("ok = true, want false")
	}
	if !errors.Is(probeErr, wantErr) {
		t.Fatalf("probeErr = %v, want %v", probeErr, wantErr)
	}
}

func TestShouldContinueAfterWaitMountPermissionErrorIgnoresNonPermissionErrors(t *testing.T) {
	called := false

	ok, probeErr := shouldContinueAfterWaitMountPermissionError(errors.New("transport endpoint is not connected"), "/mnt/drive9", func(string) error {
		called = true
		return nil
	})

	if ok {
		t.Fatal("ok = true, want false")
	}
	if probeErr != nil {
		t.Fatalf("probeErr = %v, want nil", probeErr)
	}
	if called {
		t.Fatal("probe should not be called for non-permission errors")
	}
}

func TestIsWaitMountPermissionErrorRecognizesWrappedAndTextErrors(t *testing.T) {
	for _, err := range []error{
		syscall.EPERM,
		syscall.EACCES,
		&osPathError{err: syscall.EACCES},
		errors.New("permission denied"),
		errors.New("operation not permitted"),
	} {
		if !isWaitMountPermissionError(err) {
			t.Fatalf("isWaitMountPermissionError(%v) = false, want true", err)
		}
	}
}

type osPathError struct {
	err error
}

func (e *osPathError) Error() string {
	return e.err.Error()
}

func (e *osPathError) Unwrap() error {
	return e.err
}
