package tidbcloud

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsAuthError_Nil(t *testing.T) {
	status, ok := IsAuthError(nil)
	if ok {
		t.Fatalf("expected ok=false for nil error, got status=%d", status)
	}
}

func TestIsAuthError_Missing(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", ErrAuthMissing)
	status, ok := IsAuthError(err)
	if !ok {
		t.Fatal("expected ok=true for ErrAuthMissing")
	}
	if status != 401 {
		t.Fatalf("got status %d, want 401", status)
	}
}

func TestIsAuthError_Forbidden(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", ErrAuthForbidden)
	status, ok := IsAuthError(err)
	if !ok {
		t.Fatal("expected ok=true for ErrAuthForbidden")
	}
	if status != 403 {
		t.Fatalf("got status %d, want 403", status)
	}
}

func TestIsAuthError_OtherError(t *testing.T) {
	err := errors.New("some other error")
	_, ok := IsAuthError(err)
	if ok {
		t.Fatal("expected ok=false for unrelated error")
	}
}

func TestIsNotFound_ClusterNotFound(t *testing.T) {
	err := fmt.Errorf("get cluster 123: %w", ErrClusterNotFound)
	if !IsNotFound(err) {
		t.Fatal("expected true for ErrClusterNotFound")
	}
}

func TestIsNotFound_OtherError(t *testing.T) {
	if IsNotFound(errors.New("something else")) {
		t.Fatal("expected false for unrelated error")
	}
}

func TestIsNotFound_Nil(t *testing.T) {
	if IsNotFound(nil) {
		t.Fatal("expected false for nil")
	}
}
