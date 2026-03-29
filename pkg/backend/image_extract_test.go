package backend

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

type staticImageExtractor struct {
	text string
}

func (e *staticImageExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, error) {
	return e.text, nil
}

type gatedImageExtractor struct {
	started chan struct{}
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func (e *gatedImageExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	if call == 1 {
		select {
		case e.started <- struct{}{}:
		default:
		}
		select {
		case <-e.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return "old caption", nil
	}
	return "new caption", nil
}

func TestAsyncImageExtractUpdatesContentText(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})

	if _, err := b.Write("/img/a.png", []byte("fake-png"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	got := waitForContentText(t, b, "/img/a.png", 2*time.Second)
	if !strings.Contains(got, "cat on sofa") {
		t.Fatalf("unexpected extracted text: %q", got)
	}
}

func TestAsyncImageExtractSkipsStaleChecksum(t *testing.T) {
	extractor := &gatedImageExtractor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})

	if _, err := b.Write("/img/b.png", []byte("first-image-bytes"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first extraction did not start")
	}

	if _, err := b.Write("/img/b.png", []byte("second-image-bytes"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	close(extractor.release)

	got := waitForContentText(t, b, "/img/b.png", 3*time.Second)
	if got != "new caption" {
		t.Fatalf("expected final extracted text %q, got %q", "new caption", got)
	}
}

func waitForContentText(t *testing.T, b *Dat9Backend, path string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nf, err := b.store.Stat(backgroundWithTrace(), path)
		if err == nil && nf.File != nil {
			if strings.TrimSpace(nf.File.ContentText) != "" {
				return nf.File.ContentText
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	nf, err := b.store.Stat(backgroundWithTrace(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("file not found while waiting for extracted text: path=%s err=%v", path, err)
	}
	t.Fatalf("timed out waiting for extracted text, last value=%q", nf.File.ContentText)
	return ""
}
