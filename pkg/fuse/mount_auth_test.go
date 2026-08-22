package fuse

import (
	"context"
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

			err := validateRemoteRoot(context.Background(), client.NewWithToken(ts.URL, "scoped"), "/")
			assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
			if !strings.Contains(err.Error(), "authorization denied") {
				t.Fatalf("error = %q, want authorization detail", err)
			}
		})
	}
}

func TestValidateRemoteRootNonRootForbiddenFallsBackToList(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method == http.MethodHead {
			http.Error(w, "read forbidden", http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer ts.Close()

	err := validateRemoteRoot(context.Background(), client.NewWithToken(ts.URL, "scoped"), "/team")
	if err != nil {
		t.Fatalf("validateRemoteRoot: %v, want list-only mount accepted", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("remote validation calls = %d, want HEAD plus List", got)
	}
}

func TestValidateRemoteRootNonRootUnauthorizedDoesNotFallbackToList(t *testing.T) {
	var statCalls atomic.Int32
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			statCalls.Add(1)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		case http.MethodGet:
			listCalls.Add(1)
			_, _ = w.Write([]byte(`{"entries":[]}`))
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	err := validateRemoteRoot(context.Background(), client.NewWithToken(ts.URL, "scoped"), "/team")
	assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
	if got := statCalls.Load(); got != 1 {
		t.Fatalf("Stat calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("List calls = %d, want 0 after Stat 401", got)
	}
}

func TestValidateRemoteRootNonRootTransientStatDoesNotFallbackToList(t *testing.T) {
	var statCalls atomic.Int32
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			statCalls.Add(1)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		case http.MethodGet:
			listCalls.Add(1)
			_, _ = w.Write([]byte(`{"entries":[]}`))
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	err := validateRemoteRoot(context.Background(), client.New(ts.URL, "owner"), "/team")
	assertMountExit(t, err, ExitStartupTransient, ExitReasonStartupTransient)
	if got := statCalls.Load(); got != 1 {
		t.Fatalf("Stat calls = %d, want 1", got)
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("List calls = %d, want 0 after transient Stat error", got)
	}
}

func TestValidateRemoteRootNonRootUnsupportedStatFallsBackToList(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusMethodNotAllowed,
		http.StatusNotImplemented,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method == http.MethodHead {
					http.Error(w, http.StatusText(status), status)
					return
				}
				_, _ = w.Write([]byte(`{"entries":[]}`))
			}))
			defer ts.Close()

			err := validateRemoteRoot(context.Background(), client.New(ts.URL, "owner"), "/team")
			if err != nil {
				t.Fatalf("validateRemoteRoot: %v, want compatibility fallback", err)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("remote validation calls = %d, want HEAD plus List", got)
			}
		})
	}
}

func TestValidateRemoteRootNonRootForbiddenListDeniedIsPermanent(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer ts.Close()

	err := validateRemoteRoot(context.Background(), client.NewWithToken(ts.URL, "scoped"), "/team")
	assertMountExit(t, err, ExitStartupPermanent, ExitReasonStartupPermanent)
	if got := calls.Load(); got != 2 {
		t.Fatalf("remote validation calls = %d, want HEAD plus denied List", got)
	}
}

func TestValidateRemoteRootNotFoundRemainsPermanent(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	defer ts.Close()

	err := validateRemoteRoot(context.Background(), client.New(ts.URL, "owner"), "/missing")
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

	err := validateRemoteRoot(context.Background(), client.New(ts.URL, "owner"), "/")
	assertMountExit(t, err, ExitStartupTransient, ExitReasonStartupTransient)
}

func TestValidateRemoteRootHonorsCanceledContext(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := validateRemoteRoot(ctx, client.New(ts.URL, "owner"), "/")
	assertMountExit(t, err, ExitStartupTransient, ExitReasonStartupTransient)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
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
