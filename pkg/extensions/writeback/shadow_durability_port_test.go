package writeback

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// Ports of v1's close_sync_upload_test.go shadow-remote-durability rows: a
// remote-durable shadow upload is generation-fenced — a stale generation is
// rejected without touching the network, and a missing generation never
// uploads at all. (The close-sync flush-tier tests around them exercise
// Dat9FS internals whose chain-level equivalents are covered by
// writeback_test.go's close-sync and fsync suites.)

func TestShadowRemoteDurabilityRejectsStaleGeneration(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	t.Cleanup(ts.Close)

	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	const path = "/stale.txt"
	if err := shadow.WriteFull(path, []byte("close-sync content"), 0); err != nil {
		t.Fatal(err)
	}
	gen := shadow.ActiveGeneration(path)
	if _, err := shadow.WriteAt(path, 0, []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	_, err = uploadFromShadowRemote(context.Background(), client.New(ts.URL, ""), shadow, path, path, 0, gen, shadowUploadRemoteDurable)
	// staging wraps its own (unexported) stale sentinel; both layers reject
	// before any network I/O.
	if err == nil || !staleGeneration(err) || calls.Load() != 0 {
		t.Fatalf("stale upload: err=%v remote calls=%d", err, calls.Load())
	}
}

func TestShadowRemoteDurabilityRejectsMissingGeneration(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	t.Cleanup(ts.Close)

	shadow, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	const path = "/missing-gen.txt"
	if err := shadow.WriteFull(path, []byte("close-sync content"), 0); err != nil {
		t.Fatal(err)
	}
	_, err = uploadFromShadowRemote(context.Background(), client.New(ts.URL, ""), shadow, path, path, 0, 0, shadowUploadRemoteDurable)
	if !errors.Is(err, errInvalidShadowUpload) || calls.Load() != 0 {
		t.Fatalf("unfenced upload: err=%v calls=%d", err, calls.Load())
	}
}

// staleGeneration reports either layer's stale-payload sentinel.
func staleGeneration(err error) bool {
	return errors.Is(err, errCommitPayloadStale) ||
		err != nil && strings.Contains(err.Error(), "generation is stale")
}
