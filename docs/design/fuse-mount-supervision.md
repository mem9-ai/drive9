# Drive9 FUSE Mount Daemon Reliability & Supervision

**Status:** design consensus (approved for implementation)
**Date:** 2026-08-08
**Scope:** mount daemon liveness, exit contracts, stale-mount cleanup, built-in supervision without systemd
**Non-scope:** CSI, live reexec / fd handoff (`fuse-clean-state-reexec-audit.md`), Windows/WebDAV first-class supervision in P0/P1

---

## 1. Problem summary

In long-lived agent sandboxes, the drive9 FUSE daemon is a critical process that currently has **orphan semantics and a success-biased exit path**. When it dies, customers see a dead mountpoint (`ENOTCONN` / "Transport endpoint is not connected") with no clear reason, no automatic recovery, and no platform-agnostic supervisor.

### 1.1 What fails today (mapped to code)

| Failure mode | Current behavior | Code anchor |
| --- | --- | --- |
| Serve loop ends (kernel connection drop, unexpected unmount) | `server.Wait()` returns → `shutdown(); return nil` → **exit 0**, often **no reason log** | `pkg/fuse/mount.go` (~716–718) |
| Background mount | Parent starts `--foreground` child with **`Setsid: true`**, waits for pidfile readiness, **exits 0** and never supervises | `cmd/drive9/cli/mount_background_unix.go`, `mount.go` `startMountBackgroundImpl` |
| SIGKILL / OOM kill | No atexit; kernel FUSE mount remains; pidfile may linger | e2e crash-recovery paths force-unmount after `kill -9` |
| Intentional umount vs crash | Both look like "process gone"; no stop token; systemd `Restart=always` fights `drive9 umount` | `UmountCmd` |
| Health / hang | No continuous probe after mount; freeze leaves session "up" while IO wedges | `probeMountPointReady` is startup-only |
| Alerts | Logs under cache/tmp; no status surface | `mountBackgroundLogPath` |

### 1.2 Customer-visible symptom chain

1. Daemon dies silently (or exits 0 after Wait).
2. Kernel keeps a dead FUSE superblock (for example at `/mnt/agents`).
3. All agent IO returns `ENOTCONN` / `ECONNABORTED`.
4. Without systemd, nothing restarts the daemon; even with systemd, clean umount restarts unless carefully gated.
5. Recovery requires force-unmount + remount, or sandbox recreate.

### 1.3 Root-cause framing

This is not primarily a FUSE durability bug. It is a **process lifecycle / supervision** gap. Data recovery after crash is already partly covered (journal / shadow / pending + e2e crash recovery). **Mount availability** is not.

### 1.4 Customer incident context (illustrative)

- Long-lived sandbox (~17h) with FUSE mount at `/mnt/agents`.
- Daemon dies mid-session; no auto-reconnect.
- Fingerprints: `Transport endpoint is not connected` (ENOTCONN after daemon death), `Software caused connection abort` (ECONNABORTED, often daemon↔server path).
- Mount flags example: `drive9 mount --mode=fuse -allow-other -readdir-prefetch --parallel-read-concurrency 8 --read-concurrency 48 --profile coding-agent :/path /mnt/drive9`
- Temporary mitigation: pasted systemd units with healthcheck + `Restart=always` (side effect: `drive9 umount` exit 0 causes restart unless `systemctl stop`). Many sandboxes have **no systemd**.

---

## 2. Goals / non-goals

### Goals

1. **Distinguish clean stop vs abnormal death** via exit codes + structured logs.
2. **Never leave a silent success exit** when the serve loop ended unexpectedly.
3. **Best-effort stale mount cleanup** whenever a process that owns the mount dies with a chance to run (signals, Wait return, panic recovery, supervisor reaping).
4. **Built-in supervisor** that works with **no systemd** (containers, sandboxes, macOS user sessions).
5. **Intentional stop does not restart** (`drive9 umount` stays correct).
6. **Bounded auto-restart** with backoff and permanent-failure classification.
7. **Healthcheck** that detects dead mounts and wedged daemons, not just process liveness.
8. Minimal CLI disruption: default path becomes reliable; opt-out remains for debugging.
9. Optional systemd unit generation as a **supplement**, not the primary design.

### Non-goals

- Transparent binary upgrade / fd handoff reexec (separate V0 audit).
- Full CSI Node agent or mount-pod architecture.
- Fixing remote backend outages (ECONNABORTED to server) by magically continuing offline — only ensure the **local FUSE endpoint stays usable or is recovered** when the *daemon* dies.
- Replacing agent platform process managers.
- Windows / WebDAV / vault mount supervision in P0/P1 (design should not paint into a corner).
- Preventing OOM / SIGKILL (detect and recover only).

---

## 3. Architecture

### 3.1 Process model: Supervisor + Worker

```
drive9 mount …                      # CLI parent: wait ready, exit 0 (UX unchanged)
  └─ SUPERVISOR (long-lived)        # restart policy, health, stale cleanup, stop token
       └─ WORKER --foreground       # existing pkg/fuse.Mount; honest exits
```

| Mode | Flag | Behavior | Primary use |
| --- | --- | --- | --- |
| Supervised detach | **default** for background | CLI spawns supervisor, returns after ready | desktop / VM / containers without orphan reapers |
| Supervised foreground | `--supervise-foreground` | CLI process **is** supervisor, blocks | long-lived sandboxes that keep a main process; systemd `ExecStart` |
| Legacy | `--no-supervise` | today's single orphan daemon | escape hatch |

**Hard constraints:**

- **Worker is never `Setsid`.** It must remain a child of the supervisor so the supervisor can `waitpid`, signal, and probe it.
- Supervisor may detach for background mode only. Prefer `--supervise-foreground` when the environment may reap orphan session leaders, so the mount tree stays a first-class process.
- systemd is a **P2 supplement**, not the product primary path.

**Why supervisor-outside-worker (not self-restart inside Mount):**

| Approach | Survives SIGKILL of daemon? | Stale mount after death? | Storm control | Complexity |
| --- | --- | --- | --- | --- |
| In-daemon restart only | No | Only if something external cleans | Hard | Low but incomplete |
| Parent supervisor (**chosen**) | Yes | Yes | Easy | Medium |
| systemd only | Yes on systemd hosts | Via units | Easy | Low, **fails without systemd** |

**Who watches the supervisor?** In-binary, nobody. If the whole process tree is killed, platforms use `drive9 mount ensure` as the reconcile primitive (entrypoint loop, cron, container `HEALTHCHECK`).

### 3.2 Failure classes

| Class | Examples | Detection | Action |
| --- | --- | --- | --- |
| Intentional stop | `drive9 umount`, stop token, supervisor STOPPING path | stop token / STOPPING state | drain + unmount; **exit 0**; **no restart** |
| External clean unmount | bare `fusermount -u`; mount inactive after Wait | Wait returns + mount inactive + no stop intent | log; **exit 0**; **no restart** |
| Serve loop abnormal | Wait ends while mount still active / ENOTCONN | Wait + `activeMountPoint` re-probe | log + force-unmount; **exit 3**; **restart** |
| Hang / health fail | process alive, FUSE probe times out N times | supervisor health loop | SIGTERM → grace → SIGKILL; force-unmount; **restart** |
| Panic | recovered panic in Mount | top-level recover | log stack + force-unmount; **exit 4**; **restart** |
| External kill | SIGKILL, OOM | supervisor `waitpid` | force-unmount; **restart** |
| Permanent start fail | bad flags, auth rejected, remote missing | worker exit 2/5 | **no restart** (≤3 permanent attempts then give up) |
| Transient start fail | mount busy, brief network | worker exit 6 | **restart** with backoff |
| Backend outage while mounted | ECONNABORTED to drive9 server | status / SSE age | **no remount**; degraded status only |

**Backend disconnect ≠ FUSE death.** Do not remount on every remote error. Only act when **local mount health** fails or the **worker process** exits.

### 3.3 Exit code contract (worker)

Implemented as a typed error with `ExitCode() int` (already honored by `cmd/drive9/main.go` `fatal()`).

| Code | Meaning | Supervisor restarts? |
| --- | --- | --- |
| **0** | Intentional / clean stop | **No** |
| **1** | Force-quit after second signal (intentional stop semantics) | **No** |
| **2** | Usage / flag errors | **No** |
| **3** | Serve loop ended abnormally | **Yes** |
| **4** | Unhealthy / panic | **Yes** |
| **5** | Permanent startup failure | **No** (after ≤3 attempts) |
| **6** | Transient startup failure | **Yes** |
| signal death | Kill / OOM observed by supervisor | **Yes** |

Supervisor final exit codes:

| Code | Meaning |
| --- | --- |
| 0 | Intentional stop completed |
| 1 | Internal supervisor error |
| 3 | Gave up (circuit open / permanent start budget exhausted) |

#### Implementation constraint: intent-first restart decisions

Restart decisions are **supervisor-intent-first**, not "exit code alone":

- Health kills must restart even if the worker exits 0 after SIGTERM (clean unmount path) — gate "no restart" on **stop token / STOPPING**, not only codes 0/1.
- On worker exit 0 while supervisor is still RUNNING, re-check that the mountpoint is inactive; if still active, treat as serve-loop-abnormal.
- During P0, classify startup failures into 5/6 aggressively so a generic exit 1 cannot silently suppress a deserved restart.

---

## 4. Detailed design

### 4.1 Exit classification and logging (P0)

#### 4.1.1 Fix the critical `server.Wait()` path

Current end of `Mount()`:

```go
server.Wait()
shutdown()
return nil  // ALWAYS SUCCESS — production bug
```

Replace with an explicit wait-reason model:

1. Signal handler sets `unmountRequested` (atomic) before drain/unmount.
2. After `server.Wait()`:
   - If `unmountRequested` and unmount path completed → clean stop, exit 0.
   - Else if mountpoint is no longer active (with short re-probe, ~250ms, to avoid racing external-umount teardown) → external unmount, exit 0.
   - Else → **serve ended unexpectedly**: log reason, force-unmount, exit 3.

#### 4.1.2 Panic recovery

Top-level `recover` in `pkg/fuse.Mount`: log stack, force-unmount if mount still active, return exit-4 error (library code returns errors; does not `os.Exit`).

#### 4.1.3 Logging requirements

On every terminal transition, emit one structured line (human + key=value / JSON line for scrapers):

```
drive9: mount lifecycle event=serve_end reason=unexpected_wait mountpoint=/mnt/agents pid=123 uptime=4h12m
drive9: mount lifecycle event=force_unmount mountpoint=/mnt/agents result=ok|err
drive9: mount lifecycle event=exit code=3 reason=fuse_serve_ended_unexpectedly
```

Also write a best-effort exit reason record via `mountstate` (atomic file next to the pid scheme) so `drive9 mount status` can report why the last daemon died even after the process is gone.

Optional forensics (P1/P2): include RSS / pending write counts in the exit record (rusage helpers already exist in-tree).

#### 4.1.4 FUSE `CreationTime` in process state

WebDAV already records `CreationTime` for PID-reuse safety. FUSE `WriteProcessState` must write it too so supervisor/umount do not signal the wrong process after PID reuse.

### 4.2 Stale mount cleanup

Three layers (defense in depth):

| Layer | When | Mechanism |
| --- | --- | --- |
| **L1 In-worker** | Abnormal Wait, panic recover, second signal | existing `forceUnmount` + stronger Linux death-path flags |
| **L2 Supervisor** | Worker `Wait` for any reason; health fail; pre-start | Always ensure mountpoint is not a dead FUSE mount before restart |
| **L3 External** | Supervisor itself SIGKILLed | `drive9 mount ensure` / agent preflight |

#### Death-path force-unmount

Prefer lazy/force variants for *death cleanup* (not necessarily for intentional graceful umount):

- Linux death path: `fusermount3 -uz` → `fusermount -uz` → `umount -l`
- Darwin death path: keep `diskutil unmount force` / `umount -f`
- Graceful `drive9 umount`: non-lazy first; fall back to force if EBUSY/ENOTCONN

Export shared helpers used by fuse, CLI umount recovery, and supervisor:

- Preferred: thin exports `fuse.ForceUnmount`, `fuse.ProbeMountPointReady`, or a small `pkg/mountprobe` both can import.

#### Stale detection before remount

1. Mount active **and** `stat/open/readdir` returns ENOTCONN/ECONNABORTED/EIO → stale → force-unmount.
2. Process state PID dead but mount still active → stale → force-unmount.
3. Only then start a new worker.
4. If mount is active **and** probe OK **and** a live worker owns it → refuse double-mount.

### 4.3 Healthcheck

#### What to probe

Supervisor loop (defaults):

| Knob | Default | Flag |
| --- | --- | --- |
| Interval | 10s | `--health-interval` |
| Probe timeout | 5s | `--health-timeout` |
| Consecutive failures | 3 | `--health-failures` |

Probe composition:

1. Worker process alive + `CreationTime` match (PID-reuse safe).
2. Mount active (`activeMountPoint` / equivalent).
3. Usable IO under short timeout: reuse `probeMountPointReady` (stat + readdir through the mountpoint — real FUSE round trip).

**Hard rules:**

- Do **not** treat remote API failures as mount death. Probe hits the **FUSE path**, not `DRIVE9_SERVER`.
- Do **not** probe the worker's own FUSE mount **from inside the worker** as the primary hang detector (self-deadlock risk when the serve loop is the suspect). Primary hang detection is **supervisor-side**.
- Count health failures only in **RUNNING** state (not SPAWN / STOPPING / drain).
- On hang: SIGTERM worker → grace (`--stop-timeout`, default 60s, matching umount timeout) → SIGKILL → force-unmount → restart.

Optional secondary signal (P1 if cheap): control-socket `status` ping (read-only; does not mutate like drain).

### 4.4 Auto-restart policy

#### Intentional stop (must not restart)

`drive9 umount` is the first-class intentional stop for supervised mounts:

1. Write **stop token** file (atomic), derived from mountpoint hash under `os.TempDir()` / `XDG_RUNTIME_DIR` (not under the FUSE mountpoint).
2. SIGTERM the **supervisor** (preferred) when present.
3. Supervisor enters STOPPING:
   - `stop_requested=true`
   - SIGTERM worker (existing drain/unmount path)
   - wait up to timeout; force-unmount + kill if needed
   - exit **0**
4. Remove stop token + pid/status files.

Marker-file fallback when supervisor socket is unreachable (dead/racing supervisor): a supervisor that later sees its worker exit checks the marker first — present → no restart, exit 0.

This also fixes temporary systemd pastes: unit `ExecStop` must call `drive9 umount` (writes stop token), not merely kill the process.

#### Restart rules

```
on worker exit:
  if stop_requested / STOPPING: do not restart; supervisor exits 0
  if permanent start class (exit 2/5) and attempts >= 3: give up; exit 3
  if restarts >= MaxRestarts in Window: open circuit (stay alive)
  else: force-unmount if needed; sleep backoff; start worker
```

Defaults:

| Knob | Default | Rationale |
| --- | --- | --- |
| Max restarts in window | **5** | avoid flapping |
| Window | **10m** sliding | standard StartLimit-style |
| Stable reset | **10m** healthy | reset counters after stable period |
| Backoff | 1s, 2s, 4s, … **cap 30s** + jitter | absorb kernel/FUSE races |
| Permanent start attempts | **3** then give up | bad config must not storm |

#### Circuit open behavior

- Supervisor stays resident so `mount status`, `ensure --reset`, and `drive9 umount` keep working.
- Status shows `circuit_open`.
- **Force-unmount** on circuit open so the platform does not sit on silent ENOTCONN while the supervisor idles.
- Recovery: `drive9 mount ensure`, explicit reset, or new `drive9 mount` / `drive9 umount`.

#### Remount identity

Supervisor restarts worker with the **same argv/env snapshot** captured at first start (credentials stay those snapshotted at mount time — preserves mount-lifetime credential binding). Persist sanitized `Args []string` in process/supervisor state so `mount ensure` can reconstruct flags after a fully dead tree (strip secrets; credentials continue via env as today).

### 4.5 Alerts / observability

P0/P1 local-first (sandbox constraint):

1. **Background log** (existing path) — lifecycle events always.
2. **Status JSON** — supervisor sidecar for platform probes (no secrets).
3. **stderr** when `--supervise-foreground`.

P2 optional:

- `--alert-webhook URL` — fire-and-forget POST on restart / give-up / healthkill (short timeout, rate-limited).
- `--alert-file path` — append line-delimited JSON events.
- Metrics counters if cheap (`restarts_total`, `health_fail_total`).

No dependency on drive9-server for mount-local reliability alerts.

### 4.6 CLI UX

#### `drive9 mount` flags (additive)

| Flag | Default | Meaning |
| --- | --- | --- |
| *(default background)* | supervise on | background supervisor + worker |
| `--foreground` | off | worker only, blocking (tests / external supervisors) |
| `--supervise-foreground` | off | this process is supervisor, blocks (sandbox-friendly) |
| `--no-supervise` | off | legacy single background worker |
| `--max-restarts N` | 5 | circuit window budget |
| `--restart-window dur` | 10m | circuit window |
| `--health-interval dur` | 10s | supervisor |
| `--health-timeout dur` | 5s | supervisor |
| `--health-failures N` | 3 | consecutive failures |
| `--stop-timeout dur` | 60s | SIGTERM→SIGKILL grace |
| `--restart-backoff-max dur` | 30s | backoff cap |

#### New / extended commands

| Command | Role |
| --- | --- |
| `drive9 mount status [--json] <mountpoint>` | merge supervisor + worker + probe into machine-readable status |
| `drive9 mount ensure <mountpoint>` | idempotent reconcile: healthy→0; stale/dead→clean+start; supervisor dead but worker alive→adopt-by-monitor (poll + CreationTime, not waitpid) |
| `drive9 mount health <mountpoint>` | P2 / scripts: exit 0 healthy, 1 unhealthy |
| `drive9 umount` | supervisor-aware: stop token → SIGTERM supervisor → existing fusermount path; wait for supervisor exit |
| `drive9 doctor fuse` | live supervised checks, stale ENOTCONN detection |

#### Readiness handshake change (P1)

Today `waitForBackgroundMountReady` requires `state.PID == childPID`. With the supervisor as the spawned child, readiness becomes:

- mountstate exists with a live **worker** PID (and CreationTime match when present), **and**
- `ProbeMountPointReady(mountpoint)` passes (same ready timeout as today).

Output gains daemon pid, supervisor pid, and log path.

### 4.7 Data model (`pkg/mountstate`)

Backward-compatible JSON extensions on `ProcessState` (and/or sibling supervisor state file):

```go
type ProcessState struct {
    // existing fields...
    CreationTime           uint64   `json:"creation_time,omitempty"` // WRITE for FUSE too
    Role                   string   `json:"role,omitempty"`          // "worker" | "supervisor"
    SupervisorPID          int      `json:"supervisor_pid,omitempty"`
    SupervisorCreationTime uint64   `json:"supervisor_creation_time,omitempty"`
    WorkerPID              int      `json:"worker_pid,omitempty"`
    LogPath                string   `json:"log_path,omitempty"`
    StatusPath             string   `json:"status_path,omitempty"`
    StopTokenPath          string   `json:"stop_token_path,omitempty"`
    Supervise              bool     `json:"supervise,omitempty"`
    Args                   []string `json:"args,omitempty"` // sanitized; no secrets
}
```

Supervisor status sidecar (example fields):

```json
{
  "role": "supervisor",
  "mount_point": "/mnt/agents",
  "worker_pid": 1234,
  "supervisor_pid": 1200,
  "state": "running|restarting|stopping|circuit_open|failed",
  "last_health": "ok|fail",
  "last_health_at": "...",
  "last_health_error": "",
  "restarts": 2,
  "last_exit_code": 3,
  "last_exit_reason": "serve_loop_abnormal",
  "stop_requested": false
}
```

**Recommendation for pidfile ownership in supervised mode:** supervisor owns the authoritative `WriteProcessState` after ready (so umount finds the right process); worker may skip the global pidfile when launched with an internal `--supervised` flag, or supervisor overwrites after ready — pick one and test umount thoroughly. Control socket remains on the worker.

Stop token / status / supervisor state files use the same hash scheme as pidfiles under temp/runtime dirs — **never under the FUSE mountpoint** (mount may be wedged).

Supervisor exclusivity: flock on `drive9-mount-<hash>.supervise.lock` so two `drive9 mount` invocations for the same mountpoint cannot create two supervisors.

### 4.8 Interaction with systemd (supplement only)

Generate optional unit (P2), for example `drive9 mount --print-systemd-unit …`:

```ini
[Service]
Type=simple
ExecStart=/usr/bin/drive9 mount --supervise-foreground ...
ExecStop=/usr/bin/drive9 umount --timeout 60s <mountpoint>
Restart=on-failure
RestartSec=2
KillMode=mixed
TimeoutStopSec=70
# Do NOT use Restart=always without stop-token semantics —
# that was the temporary paste footgun.
```

Because intentional stop exits 0, `Restart=on-failure` is correct. Document: never `Restart=always` without stop-token semantics; pair `TimeoutStopSec` with the worker's second-SIGTERM force-quit path.

Existing pasted units keep working better after P0 alone: exit 3/4/6 become restartable under `on-failure`, exit 0 no longer lies about clean death.

### 4.9 Data safety

Restart is a **cold remount**. Existing journal WAL replay, shadow store, pending index, and commit-queue recovery (keyed by stable mount-hash cache dirs) make crash remount data-safe enough for the customer path. Prefer SIGTERM before SIGKILL on health kills so the drain path can run. `close-sync`-class durability profiles further bound in-flight loss.

Live open file descriptors held by workloads at crash time get EIO/ENOTCONN — inherent to FUSE remount, acceptable for this reliability scope (not live reexec).

---

## 5. Phased implementation plan

### P0 — Honest death + local cleanup

**Customer impact:** logs show why mount died; abnormal death exits non-zero; best-effort unmount on death; less ENOTCONN after "polite" exits. **Does not alone survive SIGKILL.**

| Task | Files (indicative) |
| --- | --- |
| Wait-end reason + non-zero exit | `pkg/fuse/mount.go` |
| Typed mount exit codes | `pkg/fuse/mount_exit.go` (new); wire through CLI / `main.fatal` |
| Panic recover → force unmount + exit 4 | `pkg/fuse/mount.go` |
| Death-path force unmount uses lazy/force variants | `pkg/fuse/mount.go` (`forceUnmount` variants) |
| Lifecycle event logs | `pkg/fuse/mount.go` |
| Exit reason record | `pkg/mountstate/exit.go` (new) |
| FUSE `CreationTime` in process state | `pkg/fuse/mount.go`, `pkg/mountstate` |
| Unit tests for exit reasons | `pkg/fuse/mount_exit_test.go`, extend shutdown tests |
| E2E: non-SIGKILL abnormal end → non-zero + unmounted | `e2e/` small addition |

**Acceptance:** non-SIGKILL deaths never leave silent exit 0 with ENOTCONN.

**Foreground SIGTERM semantics (clarify):** unsupervised / foreground worker SIGTERM remains intentional stop (exit 0) for operator expectation. Supervised worker SIGTERM **from supervisor during STOPPING** is intentional. Serve ended without signal/stop → exit 3. SIGKILL has no worker code path; supervisor handles in P1.

### P1 — Built-in supervisor (customer fix)

| Task | Files (indicative) |
| --- | --- |
| Supervisor loop: start/wait/restart/backoff/stop token/circuit | `cmd/drive9/cli/mount_supervise*.go` and/or `pkg/mountsupervisor/` |
| Default background path → supervised | `cmd/drive9/cli/mount.go` `startMountBackgroundImpl` |
| Remove Setsid from worker; reconsider supervisor detach | `cmd/drive9/cli/mount_background_unix.go` |
| Health/restart flags | `cmd/drive9/cli/mount.go` |
| Stop token + umount integration | `cmd/drive9/cli/mount.go` umount path |
| Shared force-unmount + stale detect helpers | `pkg/fuse` exports or `pkg/mountprobe` |
| Status JSON writer | supervisor |
| ProcessState / supervisor state fields + sanitized Args | `pkg/mountstate` |
| `mount status`, `mount ensure` | new CLI files; `MountCmd` dispatch |
| Readiness handshake for supervisor child | `waitForBackgroundMountReady*` |
| Tests: kill -9 restart, umount no restart, max restarts, stop races | unit + e2e |
| Docs: sandbox recipe → `--supervise-foreground` / `ensure` | docs |

**Acceptance:**

```bash
drive9 mount --mode=fuse ... :/projects/<id> /mnt/agents
# or:
drive9 mount --supervise-foreground ... &

kill -9 <worker_pid>
# within seconds: mount usable again OR clean remount; no permanent ENOTCONN

drive9 umount /mnt/agents
# process tree gone; no restart
```

### P2 — Ops polish

| Task | Notes |
| --- | --- |
| `drive9 mount health` | exit 0/1 for scripts / systemd |
| `--print-systemd-unit` | `on-failure` + `ExecStop=umount` |
| Optional webhook / alert file | fire-and-forget |
| `doctor fuse` live supervised checks | stale + status |
| Log rotation for mount-logs | pre-existing unbounded growth |
| Optional sd_notify | only if customers use WatchdogSec |
| Control-socket status op polish | if not done in P1 |
| Vault / WebDAV parity if needed | later |

---

## 6. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Restart storms on bad config | Exit 2/5 non-restartable; permanent start budget 3; circuit 5/10m |
| Restart during active writes loses dirty data | Existing journal/shadow recovery; SIGTERM before SIGKILL; consecutive health failures before kill |
| `drive9 umount` races restart | Stop token **before** signal; STOPPING checked before every restart |
| PID reuse kills wrong process | Always store/check `CreationTime` |
| Double mounts | Pre-start stale detect + refuse healthy live mount owned by other PID; flock |
| Setsid/orphan GC still kills supervisor | Document `--supervise-foreground` / `mount ensure` as sandbox entrypoint |
| Lazy unmount leaves busy mounts weird | Lazy only on death path; graceful umount tries normal first |
| Credential snapshot sensitive | Keep 0600; status file must **not** include secrets |
| Supervisor dies, worker lives | Worker remains usable; umount can still signal worker; `ensure` adopts |
| Hang probe false positives under load | 5s timeout × 3 failures; probe is tiny readdir; RUNNING-only; tunable |
| Health kill causes worker exit 0 | Intent-first restart: no stop token → still restart |
| Circuit open with ENOTCONN left behind | Force-unmount on circuit open |
| Readiness handshake assumes child PID is worker | P1 changes ready check to worker + probe |
| Adopt unsupervised worker loses waitpid fidelity | Document poll-based liveness; `--restart` override on ensure if needed |
| Token expiry on long-lived remount | Auth fail at start → exit 5, no storm; `ensure` may re-resolve; in-band restart reuses snapshot |

---

## 7. Recommended customer recipes

```bash
# Preferred in sandboxes that keep a main process / have orphan reapers:
drive9 mount --supervise-foreground --mode=fuse -allow-other \
  -readdir-prefetch --parallel-read-concurrency 8 --read-concurrency 48 \
  --profile coding-agent :/path /mnt/drive9

# Or platform-side periodic reconcile:
drive9 mount ensure /mnt/agents

# Default supervised background when orphan GC is not hostile:
drive9 mount --mode=fuse ... :/projects/<id> /mnt/agents

# Clean stop (never restarts):
drive9 umount /mnt/agents

# Inspect:
drive9 mount status /mnt/agents
drive9 doctor fuse --mountpoint /mnt/agents
```

---

## 8. Decision summary

| Decision | Choice |
| --- | --- |
| Primary reliability mechanism | **In-binary supervisor + worker** |
| systemd | Optional unit generator only (P2) |
| Default background behavior | **Supervised** |
| Setsid on worker | **Remove** |
| Unexpected `server.Wait()` | **Log + force-unmount + exit ≠ 0** |
| Intentional stop | **Stop token + umount → supervisor exit 0, no restart** |
| Health | Supervisor-side local FUSE probe + consecutive failure threshold |
| Storm control | Exit taxonomy + circuit breaker (5 / 10m) |
| Sandbox integration | `--supervise-foreground` and/or `mount ensure` |
| Ship order | **P0 → P1 → P2** |
| Product answer | Not "paste a systemd unit" |

---

## 9. Consensus record

This design captures the agreed implementation plan for in-binary mount supervision:

- Supervisor + worker process model without requiring systemd
- P0 honest exit classification via `unmountRequested` + `activeMountPoint`
- Supervisor-side FUSE probe as primary hang detector (worker self-probe rejected as primary)
- Stop token + SIGTERM supervisor for intentional stop
- Circuit breaker stay-alive semantics with `circuit_open`
- `mount ensure` as the no-systemd platform reconcile primitive
- `--supervise-foreground` as the sandbox/orphan-reaper recipe

Implementation constraints include intent-first restart, circuit open force-unmount, persist sanitized Args, readiness handshake change, RUNNING-only health counting, and adopt-by-monitor caveats.

---

## 10. Related documents

- `docs/design/fuse-clean-state-reexec-audit.md` — live reexec / fd handoff (orthogonal; not this work)
- `docs/design/fuse-durability-policy.md` — write/fsync durability profiles (orthogonal; restarts are cold remount + existing recovery)
- `docs/openclaw-drive9-fuse.md` — operational FUSE guidance (update after P1 for supervised recipes)
