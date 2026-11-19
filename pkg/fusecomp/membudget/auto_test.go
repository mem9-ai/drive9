package membudget

import (
	"os"
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

// fakeCgroup drives AutoLimit through a fake hierarchy.
func fakeCgroup(t *testing.T, files map[string]string) {
	t.Helper()
	oldRead, oldCgroup, oldMountinfo := readLimitFile, procSelfCgroup, procSelfMountinfo
	t.Cleanup(func() { readLimitFile, procSelfCgroup, procSelfMountinfo = oldRead, oldCgroup, oldMountinfo })
	readLimitFile = func(path string) ([]byte, error) {
		if v, ok := files[path]; ok {
			return []byte(v), nil
		}
		return nil, os.ErrNotExist
	}
}

func TestAutoLimitResolvesTheProcessCgroupAndAncestors(t *testing.T) {
	// The process sits in a child cgroup whose own memory.max is "max"; the
	// limit lives on the parent. A root-only reader would find nothing.
	fakeCgroup(t, map[string]string{
		"/proc/self/cgroup":                         "0::/service/session\n",
		"/proc/self/mountinfo":                      "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
		"/sys/fs/cgroup/service/session/memory.max": "max\n",
		"/sys/fs/cgroup/service/memory.max":         "1073741824\n",
		"/sys/fs/cgroup/memory.max":                 "max\n",
	})
	limit, source := AutoLimit()
	fraction := autoLimitFraction
	want := int64(float64(int64(1<<30)) * fraction)
	if limit != want {
		t.Fatalf("limit = %d, want %d", limit, want)
	}
	if source != "/sys/fs/cgroup/service/memory.max" {
		t.Fatalf("source = %q", source)
	}
}

func TestAutoLimitTakesTheTightestAncestor(t *testing.T) {
	fakeCgroup(t, map[string]string{
		"/proc/self/cgroup":             "0::/a/b\n",
		"/proc/self/mountinfo":          "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
		"/sys/fs/cgroup/a/b/memory.max": "4294967296\n",
		"/sys/fs/cgroup/a/memory.max":   "1073741824\n",
		"/sys/fs/cgroup/memory.max":     "max\n",
	})
	fraction := autoLimitFraction
	limit, _ := AutoLimit()
	if want := int64(float64(int64(1<<30)) * fraction); limit != want {
		t.Fatalf("limit = %d, want the tightest ancestor %d", limit, want)
	}
}

func TestAutoLimitLegacyHierarchy(t *testing.T) {
	fakeCgroup(t, map[string]string{
		"/proc/self/cgroup":    "11:memory:/docker/abc\n",
		"/proc/self/mountinfo": "1 0 0:1 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
		"/sys/fs/cgroup/memory/docker/abc/memory/memory.limit_in_bytes": "2147483648\n",
	})
	fraction := autoLimitFraction
	limit, source := AutoLimit()
	if want := int64(float64(int64(2<<30)) * fraction); limit != want {
		t.Fatalf("limit = %d, want %d", limit, want)
	}
	if source != "/sys/fs/cgroup/memory/docker/abc/memory/memory.limit_in_bytes" {
		t.Fatalf("source = %q", source)
	}
}

func TestAutoLimitDisabledWithoutALimit(t *testing.T) {
	fakeCgroup(t, map[string]string{
		"/proc/self/cgroup":         "0::/\n",
		"/proc/self/mountinfo":      "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
		"/sys/fs/cgroup/memory.max": "max\n",
	})
	if limit, source := AutoLimit(); limit != 0 || source != "" {
		t.Fatalf("AutoLimit = (%d,%q), want disabled", limit, source)
	}
	// No hierarchy at all (bare host / macOS).
	fakeCgroup(t, map[string]string{})
	if limit, _ := AutoLimit(); limit != 0 {
		t.Fatalf("AutoLimit without /proc = %d, want 0", limit)
	}
}

func TestAutoLimitContainerRootMount(t *testing.T) {
	// A container whose cgroup namespace mounts the cgroup2 fs at "/".
	fakeCgroup(t, map[string]string{
		"/proc/self/cgroup":    "0::/\n",
		"/proc/self/mountinfo": "1 0 0:1 / / rw - cgroup2 cgroup rw\n",
		"/memory.max":          "536870912\n",
	})
	fraction := autoLimitFraction
	limit, source := AutoLimit()
	if want := int64(float64(int64(512<<20)) * fraction); limit != want {
		t.Fatalf("limit = %d, want %d", limit, want)
	}
	if source != "/memory.max" {
		t.Fatalf("source = %q", source)
	}
}
