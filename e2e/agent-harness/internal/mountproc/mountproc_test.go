package mountproc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWaitMountedReturnsLastCheckError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := WaitMounted(ctx, "/mnt/test", func(string) (bool, error) {
		return false, errors.New("mount table unavailable")
	})
	if !errors.Is(err, ErrMountTimeout) {
		t.Fatalf("err = %v, want ErrMountTimeout", err)
	}
	if !strings.Contains(err.Error(), "mount table unavailable") {
		t.Fatalf("err = %v, want last check error", err)
	}
}

func TestSyncModeToDurability(t *testing.T) {
	for _, tc := range []struct {
		syncMode string
		want     string
	}{
		{syncMode: "auto", want: "auto"},
		{syncMode: "interactive", want: "interactive"},
		{syncMode: "strict", want: "fsync"},
	} {
		got, err := syncModeToDurability(tc.syncMode)
		if err != nil {
			t.Fatalf("syncModeToDurability(%q): %v", tc.syncMode, err)
		}
		if got != tc.want {
			t.Fatalf("syncModeToDurability(%q) = %q, want %q", tc.syncMode, got, tc.want)
		}
	}
}
