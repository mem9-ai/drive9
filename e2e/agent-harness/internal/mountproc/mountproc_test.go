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
