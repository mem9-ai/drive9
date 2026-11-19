package membudget

import (
	"errors"
	"testing"
)

func TestParseCgroupLimit(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
		ok   bool
	}{
		{"max", 0, false},
		{"max\n", 0, false},
		{"", 0, false},
		{"  1073741824\n", 1073741824, true},
		{"9223372036854771712", 0, false}, // cgroup v1 "unlimited"
		{"9223372036854775807", 0, false}, // int64 ceiling
		{"0", 0, false},
		{"-5", 0, false},
		{"garbage", 0, false},
	} {
		got, ok := parseCgroupLimit(tc.raw)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("parseCgroupLimit(%q) = (%d,%t), want (%d,%t)", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

func TestAutoLimitUsesTightestAvailableLimit(t *testing.T) {
	oldFiles, oldRead := cgroupLimitFiles, readLimitFile
	t.Cleanup(func() { cgroupLimitFiles, readLimitFile = oldFiles, oldRead })

	cgroupLimitFiles = []string{"/v2/memory.max", "/v1/memory.limit_in_bytes"}
	readLimitFile = func(path string) ([]byte, error) {
		switch path {
		case "/v2/memory.max":
			return []byte("max\n"), nil
		case "/v1/memory.limit_in_bytes":
			return []byte("1073741824\n"), nil
		}
		return nil, errors.New("not found")
	}
	limit, source := AutoLimit()
	fraction := autoLimitFraction // runtime value: the product is fractional
	want := int64(float64(int64(1<<30)) * fraction)
	if limit != want {
		t.Fatalf("limit = %d, want %d (60%% of 1GiB)", limit, want)
	}
	if source != "/v1/memory.limit_in_bytes" {
		t.Fatalf("source = %q", source)
	}
}

func TestAutoLimitDisabledWithoutACgroupLimit(t *testing.T) {
	oldFiles, oldRead := cgroupLimitFiles, readLimitFile
	t.Cleanup(func() { cgroupLimitFiles, readLimitFile = oldFiles, oldRead })

	cgroupLimitFiles = []string{"/v2/memory.max"}
	readLimitFile = func(string) ([]byte, error) { return []byte("max"), nil }
	if limit, source := AutoLimit(); limit != 0 || source != "" {
		t.Fatalf("AutoLimit = (%d,%q), want disabled", limit, source)
	}
	readLimitFile = func(string) ([]byte, error) { return nil, errors.New("missing") }
	if limit, _ := AutoLimit(); limit != 0 {
		t.Fatalf("AutoLimit with no files = %d, want 0", limit)
	}
}
