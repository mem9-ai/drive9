package layer

import (
	"context"
	"io"
	"strings"
	"testing"
)

// lazyLargeBase streams the retained prefix of a >inlineMax truncation; a
// nil kept-prefix must zero-fill nothing (review: 8 MiB shrink produced 7
// MiB of zeros).
func TestLazyLargeBaseTruncateStreamsPrefix(t *testing.T) {
	payload := strings.Repeat("A", 64)
	var got strings.Builder
	r := &truncateReaderAt{kept: nil, keptLen: 40, size: 48, ctx: context.Background(),
		base: func(ctx context.Context, off, length int64) (io.Reader, error) {
			return strings.NewReader(payload[off : off+length]), nil
		}}
	buf := make([]byte, 48)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 48 {
		t.Fatalf("n=%d want 48", n)
	}
	got.Write(buf[:40])
	if got.String() != strings.Repeat("A", 40) {
		t.Fatalf("retained prefix corrupted: %q", got.String()[:8])
	}
	for i := 40; i < 48; i++ {
		if buf[i] != 0 {
			t.Fatalf("extension byte %d not zero", i)
		}
	}
}
