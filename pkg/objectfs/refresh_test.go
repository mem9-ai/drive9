package objectfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
)

func TestSessionRefreshWait(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	got := sessionRefreshWait(now.Add(time.Hour), now)
	if got != 45*time.Minute {
		t.Fatalf("1h ttl wait=%s want 45m", got)
	}
	got = sessionRefreshWait(now.Add(12*time.Hour), now)
	if got != 12*time.Hour-15*time.Minute {
		t.Fatalf("12h ttl wait=%s", got)
	}
	got = sessionRefreshWait(now.Add(-time.Second), now)
	if got != sessionRefreshMinWait {
		t.Fatalf("expired wait=%s", got)
	}
}

func TestParseSessionExpiry(t *testing.T) {
	got := ParseSessionExpiry("2026-08-24T13:00:00Z")
	if got.UTC().Format(time.RFC3339) != "2026-08-24T13:00:00Z" {
		t.Fatalf("got %s", got)
	}
	if !ParseSessionExpiry("").IsZero() || !ParseSessionExpiry("nope").IsZero() {
		t.Fatal("empty/invalid should be zero")
	}
}

func TestEffectiveSessionExpiryDefault(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	got := effectiveSessionExpiry(time.Time{}, now)
	if !got.Equal(now.Add(sessionDefaultTTL)) {
		t.Fatalf("got %s", got)
	}
}

func TestSessionFsReplaceSwitchesListing(t *testing.T) {
	configfile.Install()
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir1, "a.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir2, "a.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	f1, err := fs.NewFs(ctx, dir1)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := fs.NewFs(ctx, dir2)
	if err != nil {
		t.Fatal(err)
	}
	s := newSessionFs(f1, "drive9-object", "test")
	obj, err := s.NewObject(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := readObj(t, ctx, obj); got != "one" {
		t.Fatalf("got %q", got)
	}
	s.replace(f2)
	obj, err = s.NewObject(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := readObj(t, ctx, obj); got != "two" {
		t.Fatalf("got %q", got)
	}
}

func readObj(t *testing.T, ctx context.Context, obj fs.Object) string {
	t.Helper()
	rc, err := obj.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(bytes.TrimSpace(b))
}
