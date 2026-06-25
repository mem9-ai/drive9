package metrics

import (
	"os"
	"testing"
)

func TestParseProcStat(t *testing.T) {
	// Synthetic /proc/self/stat line. comm "(dr ive9)" deliberately contains a
	// space and parens to exercise the after-last-')' parsing. utime=200,
	// stime=100 (-> (200+100)/100 = 3.0s), vsize=1048576, rss=10 pages.
	line := "1234 (dr ive9) R 1 1234 1234 0 -1 0 0 0 0 0 200 100 0 0 20 0 1 0 5000 1048576 10 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
	ps, ok := parseProcStat(line)
	if !ok {
		t.Fatalf("parseProcStat returned ok=false for valid line")
	}
	if ps.cpuSeconds != 3.0 {
		t.Fatalf("cpuSeconds = %v, want 3.0", ps.cpuSeconds)
	}
	if ps.virtualBytes != 1048576 {
		t.Fatalf("virtualBytes = %v, want 1048576", ps.virtualBytes)
	}
	wantRSS := 10.0 * float64(os.Getpagesize())
	if ps.residentBytes != wantRSS {
		t.Fatalf("residentBytes = %v, want %v", ps.residentBytes, wantRSS)
	}
}

func TestParseProcStatMalformed(t *testing.T) {
	if _, ok := parseProcStat("garbage-without-paren"); ok {
		t.Fatalf("expected ok=false for line without ')'")
	}
	if _, ok := parseProcStat("1 (x) R 1 2 3"); ok {
		t.Fatalf("expected ok=false for truncated line")
	}
}
