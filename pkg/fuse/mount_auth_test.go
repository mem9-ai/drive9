package fuse

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestValidateRemoteRootAuthorizationErrorsArePermanent(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, http.StatusText(status), status)
			}))
			defer ts.Close()

			err := validateRemoteRoot(client.NewWithToken(ts.URL, "scoped"), "/")
			assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
			if !strings.Contains(err.Error(), "authorization denied") {
				t.Fatalf("error = %q, want authorization detail", err)
			}
		})
	}
}

func TestValidateRemoteRootNonRootForbiddenSkipsListFallback(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer ts.Close()

	err := validateRemoteRoot(client.NewWithToken(ts.URL, "scoped"), "/team")
	assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
	if got := calls.Load(); got != 1 {
		t.Fatalf("remote validation calls = %d, want 1 without list fallback", got)
	}
}

func TestValidateRemoteRootNotFoundRemainsPermanent(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	defer ts.Close()

	err := validateRemoteRoot(client.New(ts.URL, "owner"), "/missing")
	assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("error = %q, want missing-root detail", err)
	}
}

func TestValidateRemoteRootServiceUnavailableRemainsTransient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	err := validateRemoteRoot(client.New(ts.URL, "owner"), "/")
	assertMountExit(t, err, ExitStartupTransient, ExitReasonStartupTransient)
}

func assertMountExit(t *testing.T, err error, code int, reason MountExitReason) {
	t.Helper()
	var exitErr *MountExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error = %T %v, want *MountExitError", err, err)
	}
	if exitErr.Code != code || exitErr.Reason != reason {
		t.Fatalf("mount exit = code %d reason %s, want code %d reason %s", exitErr.Code, exitErr.Reason, code, reason)
	}
}
