# CSI DaemonSet Rolling Upgrade — Mount Survival Design

## Problem

When the `drive9-csi-node` DaemonSet is updated (rolling update), the CSI Pod
restarts. Currently, `shutdownNodeMounts()` runs on Pod exit and actively
unmounts all `drive9 mount` processes + staging targets. After restart, recovery
re-mounts the staging targets, but existing business Pods lose their mount
points because:

1. The old staging target is unmounted → kubelet's bind mount (staging → publish
   target) loses its source → business Pod's HostToContainer bind mount goes
   stale (ESTALE/EIO).
2. Recovery creates a new mount on the same staging target path, which
   propagates to the kubelet publish target, but **not** into already-running
   business Pods — HostToContainer propagation is established at Pod creation
   time and is one-way.

## Root Cause

`shutdownNodeMounts()` does not distinguish between "CSI Pod rolling upgrade"
and "intentional volume cleanup." It unconditionally calls `drive9 umount` +
`unmountPath` for every recorded mount, destroying the FUSE connection.

## Solution

### Core Idea

Run `drive9 mount` processes in the **host mount namespace** AND in a
**host-managed systemd scope** so they are fully decoupled from the CSI Pod's
container lifecycle. The CSI driver process never actively unmounts on exit; all
mount cleanup is driven by the CSI lifecycle API (`NodeUnstageVolume`).

### Why both nsenter and systemd-run are required

- **`nsenter --mount`** changes the mount namespace so the FUSE mount happens
  directly on the host filesystem. But nsenter alone does NOT move the process
  out of the CSI container's cgroup — kubelet's cgroup cleanup on Pod deletion
  would still kill it.
- **`systemd-run --scope`** places the process into a host-managed systemd
  transient scope (`system.slice/drive9-mount-<vol>.scope`), outside the CSI
  Pod's cgroup hierarchy. This is what makes the process survive Pod deletion.

### Support matrix and prerequisites

**Supported**: Linux nodes with systemd as init system (Ubuntu, Amazon Linux,
CentOS, RHEL, Flatcar, Bottlerocket). This covers >95% of production K8s nodes.

**Not supported**: Non-systemd nodes (Alpine-based, custom init). These will
**fail fast** at mount time with a clear error:
`"drive9-csi: host systemd required for mount process lifecycle management"`.

**Security posture**: The CSI node plugin runs with `privileged: true` +
`SYS_ADMIN` + host kubelet path + host `/proc` access. This is node-root level
capability, which is the standard security model for CSI FUSE drivers (same as
JuiceFS CSI, s3-csi, etc.). Code paths are restricted to:
- `/host-proc/1/ns/mnt` — enter host mount namespace
- `systemd-run` / `systemctl` — manage drive9 mount scopes only
- No arbitrary host `/proc` operations

### Design

#### 1. Startup preflight

On CSI driver startup (before accepting any gRPC calls), run preflight checks:

```
1. Open /host-proc/1/ns/mnt (verify host /proc is mounted correctly)
2. nsenter --mount=/host-proc/1/ns/mnt -- /bin/true
   (verify nsenter into host mount namespace works)
3. nsenter --mount=/host-proc/1/ns/mnt -- \
       systemd-run --scope --unit=drive9-preflight -- /bin/true
   (verify transient scope creation works end-to-end: systemd-run binary
   exists, D-Bus is accessible, host systemd accepts scope creation,
   unit reaches active state and exits cleanly)
4. Verify unit completed: systemctl is-failed drive9-preflight.scope returns
   non-zero (unit ran /bin/true successfully)
5. Clean up: systemctl reset-failed drive9-preflight.scope (remove dead unit)
```

If any check fails → log error with specific failure reason, set driver to
degraded mode (reject `NodeStageVolume` with `FAILED_PRECONDITION` and
actionable error message). Do not silently fall back to in-container mount.

#### 2. Host-namespace + host-cgroup mount process

Change `startDrive9Mount` to launch `drive9 mount` via `nsenter` + `systemd-run`:

```
nsenter --mount=/host-proc/1/ns/mnt -- \
    systemd-run --scope --unit=drive9-mount-<escaped-volume-id> -- \
    /var/lib/drive9-csi/bin/drive9 mount --foreground ... <staging-target>
```

The mount process:
- Runs in the host's mount namespace (FUSE mount directly on host)
- Lives in a host systemd scope cgroup (`system.slice/drive9-mount-<vol>.scope`)
- Survives CSI Pod restart — kubelet cleans Pod cgroup, not host systemd scopes
- Cleaned up via `NodeUnstageVolume` → `drive9 umount` (control socket), with
  `systemctl stop` as fallback

#### 3. Systemd unit naming and idempotency

**Unit naming**: `drive9-mount-<escaped-volume-id>.scope`
- Use `systemd-escape` to safely encode volume IDs containing `/`, `.`, or
  other special characters
- Mount state JSON records `systemdUnit` field for reverse lookup

**Idempotency** (`NodeStageVolume` retry):
- If scope already exists AND pid matches state AND mount point exists →
  return success (idempotent)
- If scope exists but pid doesn't match → `systemctl stop` scope, then
  re-mount
- If scope doesn't exist but state file exists → state is stale, clean up
  and re-mount

**Recovery split-state handling** (on CSI driver restart):

| Scope exists | PID matches | Mount exists | Control socket | Action |
|---|---|---|---|---|
| yes | yes | yes | yes | Skip (healthy) |
| yes | yes | yes | no | Stop scope, unmount, re-mount |
| yes | yes | no | - | Stop scope, re-mount |
| yes | no | - | - | Stop scope, clean state, re-mount |
| no | yes | yes | yes | Adopt (create scope? or leave as-is) |
| no | yes | no | - | Kill PID, clean state, re-mount |
| no | no | yes | - | Kernel unmount, clean state, re-mount |
| no | no | no | - | Clean state (nothing to recover) |

#### 4. Host `/proc` access (without hostPID)

Mount host `/proc` into the CSI container for `nsenter` namespace entry:

```yaml
volumeMounts:
  - name: host-proc
    mountPath: /host-proc
    readOnly: true
volumes:
  - name: host-proc
    hostPath:
      path: /proc
      type: Directory
```

This avoids `hostPID: true` which would expose all host processes to the
container. The CSI driver only accesses `/host-proc/1/ns/mnt` for namespace
entry.

#### 5. Versioned binary installation (init container)

Install the `drive9` binary to a versioned path on the host:

```yaml
initContainers:
  - name: install-drive9
    image: ghcr.io/drive9-ai/drive9-csi:<tag>
    command:
      - sh
      - -c
      - |
        SHORT_SHA=$(drive9 version --short-sha 2>/dev/null || echo unknown)
        cp -f /usr/local/bin/drive9 "/host-state/bin/drive9-${SHORT_SHA}"
        ln -sf "drive9-${SHORT_SHA}" /host-state/bin/drive9
    volumeMounts:
      - name: state-dir
        mountPath: /host-state
```

Binary layout on host (`/var/lib/drive9-csi/bin/`):
- `drive9-<sha>` — versioned binary (immutable once written)
- `drive9` — symlink to current version (updated on each CSI Pod start)

Mount state records `binaryPath` for auditability. Old mount processes hold
an open fd to the old binary inode; symlink update doesn't affect them.

**Binary GC**: During recovery, scan all state files for referenced
`binaryPath` values. Remove any `drive9-*` binaries in the bin directory that
are not referenced by any live mount state.

#### 6. Remove shutdown unmount on SIGTERM

Remove `shutdownNodeMounts()` from the SIGTERM/exit path. The CSI driver
process exits cleanly (stops gRPC server, deregisters from kubelet) without
touching mount processes.

SIGTERM is sent on: rolling update, `kubectl delete pod`, Pod eviction,
preStop hook timeout. In all cases, mount processes should survive.

#### 7. Cleanup sequence (NodeUnstageVolume)

Fixed ordering — each step runs regardless of previous step's result:

```
1. drive9 umount via control socket (30s timeout)
2. If step 1 failed: systemctl stop drive9-mount-<vol>.scope (10s timeout)
3. Verify: isMountPoint(stagingTarget) == false
   If still mounted: kernel unmount (unix.Unmount)
   If busy: lazy unmount (MNT_DETACH)
4. Verify: PID dead (pidMatchesState == false)
   If still alive: SIGKILL + wait 5s
5. Delete mount state file
```

**Terminal state**: mount gone + PID gone + scope gone + state file gone.

**State file deletion rule**: state file is deleted ONLY after all of the
following are confirmed:
- `isMountPoint(stagingTarget)` returns false
- PID is dead (`pidMatchesState` returns false)
- Scope is inactive or doesn't exist

If any of these conditions is NOT met after all cleanup attempts, the state
file is **preserved** and `NodeUnstageVolume` returns an error. This ensures
the orphaned resource remains manageable by subsequent recovery or
`NodeUnstageVolume` retries. Never delete state to "clean up" an
unresolvable orphan — that makes it permanently unmanageable.

#### 8. Recovery (minimal change to existing code)

`recoverNodeMounts()` enhanced with systemd scope awareness:
- Check `pidMatchesState(state)` AND scope active → skip (healthy)
- Otherwise → run split-state matrix (section 3 above)
- After successful re-mount → `repairPublishTargets` (existing code, unchanged)

#### 9. Priority class for drain ordering

```yaml
spec:
  template:
    spec:
      priorityClassName: system-node-critical
```

Ensures CSI driver Pod is evicted last during `kubectl drain`, so
`NodeUnstageVolume` calls from consumer Pod evictions are served by a running
CSI driver.

### Mount propagation

With nsenter, the FUSE mount originates in the host mount namespace:
- Staging target: `/var/lib/kubelet/plugins/kubernetes.io/csi/...` — host path
- Kubelet bind mount (staging → publish target): both in host namespace
- Business Pod mount: HostToContainer bind mount established at Pod creation

The CSI Pod still needs `mountPropagation: Bidirectional` on the kubelet-dir
volume mount so the CSI driver (inside the container) can see host mount state
for `isMountPoint()` checks.

### Resource isolation

Mount processes in host systemd scopes are not constrained by CSI Pod cgroup
limits. This is the standard model for production FUSE mount daemons.

V1: `drive9 mount` manages its own cache memory via `--cache-dir` configuration.
Future: per-scope resource limits via `systemd-run --property=MemoryMax=...` and
OOM score adjustment.

### Changes required

| File | Change |
|---|---|
| `internal/driver/mount_linux.go` | `startDrive9Mount`: nsenter + systemd-run wrapper; unit naming with systemd-escape |
| `internal/driver/mount_linux.go` | `drive9Umount` / cleanup: fixed 5-step sequence with systemctl stop fallback |
| `internal/driver/driver.go` | Startup preflight; remove `shutdownNodeMounts()` from SIGTERM handler |
| `internal/driver/node_recovery.go` | Split-state matrix handling; systemd scope liveness check |
| `deploy/kubernetes/node.yaml` | Init container (versioned binary), host-proc volume, priorityClassName |
| Mount state JSON | Add `binaryPath` and `systemdUnit` fields |

### Verification plan (e2e)

1. **Preflight**: CSI driver starts, passes all preflight checks, logs success
2. **Mount**: `NodeStageVolume` creates mount via systemd scope; verify
   `/proc/<pid>/cgroup` shows `system.slice/drive9-mount-*`, NOT `kubepods/`
3. **Rolling update**: `kubectl rollout restart ds/drive9-csi-node` — business
   Pod maintains open fd loop (read/write/fsync), verify mount PID/startTime/
   mount id unchanged
4. **Pod delete**: `kubectl delete pod drive9-csi-node-xxx` — same verification
   as rolling update
5. **New Pod mount**: create new PVC + Pod after upgrade, confirm new binary
   version via `readlink /var/lib/drive9-csi/bin/drive9` and state file
6. **NodeUnstageVolume**: delete all consumer Pods → mount cleaned up, scope
   stopped, state deleted, mount point gone
7. **Mount crash**: `kill -9` mount PID → recovery re-mounts, scope recreated
8. **Idempotency**: call `NodeStageVolume` twice for same volume → second call
   returns success without creating duplicate mount
9. **Binary GC**: after all old mounts cleaned, old binary removed
10. **Node drain**: `kubectl drain` — consumer Pods evicted first, mounts
    cleaned via `NodeUnstageVolume`, CSI Pod evicted last
