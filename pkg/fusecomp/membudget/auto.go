package membudget

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// autoLimitFraction is the share of the container memory limit a mount may
// budget for its own caches and buffers. The remainder is headroom for the Go
// runtime, the kernel FUSE queues and the rest of the process.
const autoLimitFraction = 0.6

// Filesystem/process locations. They are variables so tests can drive the
// hierarchy without a container.
var (
	readLimitFile     = os.ReadFile
	procSelfCgroup    = "/proc/self/cgroup"
	procSelfMountinfo = "/proc/self/mountinfo"
)

// AutoLimit derives the memory budget a mount should use when the operator did
// not configure one: autoLimitFraction of the container's memory limit. Memory
// limits are hierarchical, so the process's own cgroup is resolved through
// /proc/self/cgroup and the cgroup mount, and the tightest finite limit on the
// path to the mount root wins.
//
// It returns 0 (budget disabled) when the process is not cgroup-limited, which
// is the case on a bare host — there the operator states a limit explicitly
// rather than inheriting an arbitrary default. The returned source names the
// file the limit came from, for the mount log.
func AutoLimit() (limit int64, source string) {
	for _, resolve := range []func() (int64, string){cgroupV2Limit, cgroupV1Limit} {
		if value, src := resolve(); value > 0 {
			budget := int64(float64(value) * autoLimitFraction)
			if budget > 0 {
				return budget, src
			}
		}
	}
	return 0, ""
}

// cgroupV2Limit resolves the unified hierarchy: /proc/self/cgroup names the
// process's cgroup, /proc/self/mountinfo the cgroup2 mount point, and the
// tightest memory.max from that cgroup up to the mount root applies.
func cgroupV2Limit() (int64, string) {
	membership, ok := cgroupMembership("")
	if !ok {
		return 0, ""
	}
	mount, ok := cgroupMountPoint("cgroup2", "")
	if !ok {
		return 0, ""
	}
	return walkMemoryLimits(mount.resolve(membership), mount.point, "memory.max")
}

// cgroupV1Limit resolves the legacy hierarchy: the memory controller path from
// /proc/self/cgroup and the cgroup1 mount whose super options carry "memory".
func cgroupV1Limit() (int64, string) {
	membership, ok := cgroupMembership("memory")
	if !ok {
		return 0, ""
	}
	mount, ok := cgroupMountPoint("cgroup", "memory")
	if !ok {
		return 0, ""
	}
	// The mount root /proc/self/mountinfo reports for the memory controller IS
	// the controller root on a real v1 system (`/sys/fs/cgroup/memory`), so the
	// limit file sits directly in the cgroup directory. Hybrid single-root
	// layouts expose it one directory down; both are probed per level.
	return walkMemoryLimits(mount.resolve(membership), mount.point,
		"memory.limit_in_bytes", "memory/memory.limit_in_bytes")
}

// cgroupMembership returns the process's cgroup path for a controller ("" for
// the unified v2 hierarchy) from /proc/self/cgroup.
func cgroupMembership(controller string) (string, bool) {
	raw, err := readLimitFile(procSelfCgroup)
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) != 3 {
			continue
		}
		if controller == "" {
			if parts[0] != "0" || parts[1] != "" {
				continue
			}
		} else if !superOptionsContain(parts[1], controller) {
			// The v1 controller field is a comma-separated list
			// ("7:cpu,memory:/job"), not a single name.
			continue
		}
		rel := strings.TrimSpace(parts[2])
		if rel == "" {
			rel = "/"
		}
		if !strings.HasPrefix(rel, "/") {
			rel = "/" + rel
		}
		return rel, true
	}
	return "", false
}

// cgroupMount describes a cgroup filesystem mount: where it appears in the
// namespace and which subtree of the hierarchy it exposes.
type cgroupMount struct {
	point string // mountinfo field 5
	root  string // mountinfo field 4: the mounted subtree
}

// resolve maps a cgroup membership path (/proc/self/cgroup) to a directory under
// the mount point, honouring a subtree mount (root "/tenant", membership
// "/tenant/job" -> <point>/job).
func (m cgroupMount) resolve(membership string) string {
	rel := strings.TrimPrefix(membership, "/")
	if root := strings.Trim(strings.TrimSpace(m.root), "/"); root != "" {
		if rel == root {
			rel = ""
		} else if trimmed, ok := strings.CutPrefix(rel, root+"/"); ok {
			rel = trimmed
		} else {
			// A membership outside the mounted subtree cannot be resolved
			// through this mount: joining it would fabricate a path that is
			// not a cgroup at all, so adopt the mount point (the closest
			// cgroup this mount can still speak about) instead.
			return m.point
		}
	}
	if rel == "" {
		return m.point
	}
	return filepath.Join(m.point, rel)
}

// cgroupMountPoint finds the mount point of a cgroup filesystem from
// /proc/self/mountinfo. controller, when set, must appear in the super options
// (how a cgroup v1 mount advertises its controllers).
func cgroupMountPoint(fstype, controller string) (cgroupMount, bool) {
	raw, err := readLimitFile(procSelfMountinfo)
	if err != nil {
		return cgroupMount{}, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		left, right, found := strings.Cut(line, " - ")
		if !found {
			continue
		}
		leftFields := strings.Fields(left)
		rightFields := strings.Fields(right)
		if len(leftFields) < 5 || len(rightFields) < 3 {
			continue
		}
		if rightFields[0] != fstype {
			continue
		}
		if controller != "" && !superOptionsContain(rightFields[2], controller) {
			continue
		}
		return cgroupMount{point: leftFields[4], root: leftFields[3]}, true
	}
	return cgroupMount{}, false
}

func superOptionsContain(options, controller string) bool {
	for _, opt := range strings.Split(options, ",") {
		if opt == controller {
			return true
		}
	}
	return false
}

// walkMemoryLimits returns the tightest finite memory limit from dir up to root
// (inclusive) and the file it came from. Several file names are probed per level
// because a v1 memory mount keeps the limit directly in the cgroup directory
// while a hybrid single-root layout nests a controller directory.
func walkMemoryLimits(dir, root string, names ...string) (int64, string) {
	var (
		best       int64
		bestSource string
	)
	for {
		// First name that parses wins at THIS level: the hybrid probe exists for
		// layout differences, and a real child cgroup that happens to be called
		// "memory" must not contribute its own (tighter) cap.
		for _, name := range names {
			path := filepath.Join(dir, name)
			raw, err := readLimitFile(path)
			if err != nil {
				continue
			}
			value, ok := parseCgroupLimit(string(raw))
			if !ok {
				continue
			}
			if best == 0 || value < best {
				best, bestSource = value, path
			}
			break
		}
		if dir == root || dir == "/" || len(dir) <= len(root) {
			break
		}
		dir = filepath.Dir(dir)
	}
	return best, bestSource
}

// parseCgroupLimit reads one cgroup limit value. ok is false for the "max"
// marker (v2), the v1 "unlimited" sentinels (PAGE_COUNTER_MAX and the
// 0x7FFFFFFFFFFFF000-style ceiling), and anything unparseable.
func parseCgroupLimit(raw string) (int64, bool) {
	text := strings.TrimSpace(raw)
	if text == "" || strings.EqualFold(text, "max") {
		return 0, false
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 {
		return 0, false
	}
	// cgroup v1 reports "no limit" as a huge page-aligned number (on 64-bit:
	// 9223372036854771712). Anything near the int64 ceiling is unlimited.
	const unlimitedFloor = int64(1) << 60
	if value >= unlimitedFloor {
		return 0, false
	}
	return value, true
}
