package fuse

import (
	"errors"
	"syscall"
	"testing"
)

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
