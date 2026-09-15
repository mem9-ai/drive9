#!/usr/bin/env node
// Cross-channel fs.watch visibility probe for a Drive9 mount.
//
// usage: node watch_probe.js <mountDir> <resultJson(local)> <readyFile(local)> <expectedPayload>
//
// Phase A (own write): the probe writes target.txt through the mount itself
// and requires its own watchers to see it. Phase B (remote write): after the
// ready marker appears the harness mutates trigger.stamp and target.txt via
// the drive9 CLI (server-side); the probe measures which notification channel
// observes the change and how fast: fs.watch(file), fs.watch(dir),
// fs.watchFile stat polling, and a raw stat poll.
//
// Content visibility requires an EXACT match against <expectedPayload> (the
// full bytes the harness wrote server-side). A truncated write or a stale
// payload from another round is classified (truncated / mismatch) and the
// probe keeps observing; only the full payload sets content_verified.
//
// Exit codes: 0 result written (full payload observed),
//            3 full payload never observed (result still written), 1 crash.

'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

const [mountDir, resultPath, readyPath, expectedPayload] = process.argv.slice(2);

async function selftest() {
  // Negative-control regression (no mount required): a delayed read started
  // from a pre-barrier phase-A event must never be credited as a remote
  // event, even when the remote payload lands while that read is blocked.
  // Drives the probe as a child with WATCH_PROBE_DELAY_READ_MS so reads
  // straddle the remote write; asserts the credited epoch (if any) is
  // post-barrier. The pre-barrier fence makes the stale-event misattribution
  // structurally impossible; the old confirm-on-read implementation credits
  // it, so this test distinguishes the fix.
  const os = require('os');
  const { spawn } = require('child_process');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'watch-probe-selftest-'));
  const payload = `v2-remote-selftest-${Date.now()}\n`;
  const resultPathSelf = path.join(dir, 'result.json');
  const readyPathSelf = path.join(dir, 'ready');
  const child = spawn(process.execPath, [
    __filename,
    dir,
    resultPathSelf,
    readyPathSelf,
    payload,
  ], { env: { ...process.env, WATCH_PROBE_DELAY_READ_MS: '700' }, stdio: 'ignore' });
  const deadline = Date.now() + 30000;
  while (!fs.existsSync(readyPathSelf) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  if (!fs.existsSync(readyPathSelf)) {
    child.kill('SIGKILL');
    throw new Error('selftest: probe never armed');
  }
  // Harness side: trigger, then the remote full payload while the delayed
  // phase-A reads may still be in flight.
  await fsp.writeFile(path.join(dir, 'trigger.stamp'), `${Date.now()}\n`);
  await new Promise((resolve) => setTimeout(resolve, 120));
  await fsp.writeFile(path.join(dir, 'target.txt'), payload);
  const code = await new Promise((resolve) => child.on('exit', resolve));
  const report = JSON.parse(await fsp.readFile(resultPathSelf, 'utf8'));
  const pb = report.phase_b;
  const failures = [];
  if (code !== 0) failures.push(`probe exit=${code}`);
  if (!pb.content_verified) failures.push('content not verified');
  const barrier = report.barrier_epoch_ms;
  if (typeof barrier !== 'number') failures.push('no barrier epoch');
  for (const channel of ['file_watch_epoch_ms', 'dir_watch_epoch_ms']) {
    const credited = pb[channel];
    if (credited !== null && typeof barrier === 'number' && credited < barrier) {
      failures.push(`${channel}=${credited} credited pre-barrier (barrier=${barrier})`);
    }
  }
  fs.rmSync(dir, { recursive: true, force: true });
  if (failures.length > 0) {
    throw new Error(`selftest failed: ${failures.join('; ')}`);
  }
  console.log('watch probe selftest: pre-barrier stale events cannot be credited; content verified');
}

const target = path.join(mountDir, 'target.txt');
const trigger = path.join(mountDir, 'trigger.stamp');

const V1 = 'v1-probe\n';
const V2_PREFIX = 'v2-remote';
const PHASE_A_WAIT_MS = 15000;
const PHASE_B_WAIT_MS = 180000;

const result = {
  started_epoch_ms: Date.now(),
  mount_dir: mountDir,
  expected_payload_bytes: expectedPayload ? expectedPayload.length : null,
  watcher_errors: {},
  phase_a: { file_event: false, dir_event: false, ms: null },
  phase_b: {
    trigger_seen_epoch_ms: null,
    content_verified: false,
    content_match: null, // 'exact' | 'truncated' | 'mismatch' once any change is seen
    content_epoch_ms: null,
    file_watch_epoch_ms: null,
    dir_watch_epoch_ms: null,
    watchfile_epoch_ms: null,
    statpoll_epoch_ms: null,
    partial_content: null,
    other_content: null,
    last_content: null,
  },
};

function recordWatcherError(kind, err) {
  result.watcher_errors[kind] = `${err.code || err.name || 'error'}: ${err.message}`;
}

let phase = 'A';
let lastContent = V1;
// Monotone lower bound on the mutation time: updated whenever any read still
// observes pre-mutation content. Evaluated retroactively at finalize time so
// a bound that advances AFTER a candidate was recorded still fences it.
let lastPreMutationObsEpoch = 0;
// Sentinel barrier: after phase A the probe performs one more own write and
// waits for its event; everything at/before that event is phase-A backlog.
// Only after the barrier does the probe signal ready, so the harness's remote
// mutation provably happens post-barrier and pre-barrier events (however
// delayed their reads) can never be credited as remote events.
let barrierEpoch = null;
let armed = false;
const pendingEventEpochs = { file_watch: [], dir_watch: [] };
let barrierResolve = null;

// Test-only hook: delays every content read so a blocked read spanning the
// mutation can be constructed deterministically on a healthy local fs
// (used by `selftest`). Never set in production runs.
const READ_DELAY_MS = parseInt(process.env.WATCH_PROBE_DELAY_READ_MS || '0', 10) || 0;

async function readContent() {
  if (READ_DELAY_MS > 0) {
    await new Promise((resolve) => setTimeout(resolve, READ_DELAY_MS));
  }
  try {
    return await fsp.readFile(target, 'utf8');
  } catch {
    return null;
  }
}

async function noteContent(channel, epochMs) {
  const content = await readContent();
  if (content !== null && content !== lastContent) {
    lastContent = content;
    if (content === expectedPayload) {
      result.phase_b.content_verified = true;
      result.phase_b.content_match = 'exact';
      result.phase_b.content_epoch_ms ??= epochMs;
      result.phase_b.last_content = content.slice(0, 80);
      // Attribution is evidence-collecting, not committing: the event epoch
      // becomes a CANDIDATE only when the read triggered by that event
      // returned the full remote payload AND the event is post-barrier.
      // Candidates are re-checked against the final monotone bound at exit,
      // so a stale-content observation completing later retroactively fences
      // an already-collected candidate. Non-event channels (stat poll,
      // watchFile) record observation epochs directly — they carry no
      // event-causality claim.
      if (armed && (channel === 'file_watch' || channel === 'dir_watch') && epochMs >= barrierEpoch) {
        pendingEventEpochs[channel].push(epochMs);
      }
    } else {
      if (phase === 'B') {
        lastPreMutationObsEpoch = Date.now();
      }
      if (content.startsWith(V2_PREFIX)) {
        // A truncated or wrong-round v2 payload: keep observing — only the
        // full expected payload satisfies the correctness floor.
        result.phase_b.content_match = content.length < expectedPayload.length ? 'truncated' : 'mismatch';
        if (content.length < expectedPayload.length) {
          result.phase_b.partial_content ??= `${content.length}/${expectedPayload.length} bytes: ${content.slice(0, 80)}`;
        } else {
          result.phase_b.other_content ??= content.slice(0, 80);
        }
      }
    }
    // Anything else (e.g. the phase-A own-write content) is simply pre-mutation
    // state, not a remote-round payload — no classification, no attribution.
  } else if (content !== null && phase === 'B' && content !== expectedPayload) {
    // Unchanged pre-mutation content still tightens the mutation lower bound.
    lastPreMutationObsEpoch = Date.now();
  }
  return content;
}

async function main() {
  if (mountDir === 'selftest') {
    await selftest();
    return;
  }
  await fsp.writeFile(target, V1);

  let fileWatcher = null;
  let dirWatcher = null;
  const onWatcherError = (kind) => (err) => {
    recordWatcherError(kind, err);
    const watcher = kind === 'fs_watch_file' ? fileWatcher : dirWatcher;
    try {
      if (watcher) watcher.close();
    } catch {
      /* already closed */
    }
  };
  try {
    fileWatcher = fs.watch(target, (eventType) => {
      const now = Date.now();
      if (phase === 'A') {
        if (!result.phase_a.file_event) {
          result.phase_a.file_event = true;
          result.phase_a.ms ??= now - phaseAStart;
        }
        if (barrierResolve) {
          const resolve = barrierResolve;
          barrierResolve = null;
          resolve(now);
        }
      } else if (armed) {
        // Candidates only while armed; the finalize pass re-checks them.
        result.phase_b.file_event_type ??= eventType;
        noteContent('file_watch', now).catch(() => {});
      }
    });
    // A late watcher error must be recorded and that channel closed, not
    // crash the probe (an unhandled 'error' event would turn an otherwise
    // classifiable outcome into a hard probe failure).
    fileWatcher.on('error', onWatcherError('fs_watch_file'));
  } catch (err) {
    recordWatcherError('fs_watch_file', err);
  }
  try {
    dirWatcher = fs.watch(mountDir, (eventType, filename) => {
      const name = path.basename(String(filename || ''));
      // trigger.stamp events say nothing about the target mutation; crediting
      // the dir-watch channel from them would fake remote event support.
      if (name !== 'target.txt') return;
      const now = Date.now();
      if (phase === 'A') {
        if (!result.phase_a.dir_event) {
          result.phase_a.dir_event = true;
          result.phase_a.ms ??= now - phaseAStart;
        }
        if (barrierResolve) {
          const resolve = barrierResolve;
          barrierResolve = null;
          resolve(now);
        }
      } else if (armed) {
        result.phase_b.dir_event_type ??= eventType;
        noteContent('dir_watch', now).catch(() => {});
      }
    });
    dirWatcher.on('error', onWatcherError('fs_watch_dir'));
  } catch (err) {
    recordWatcherError('fs_watch_dir', err);
  }
  try {
    fs.watchFile(target, { interval: 200, persistent: false }, (curr, prev) => {
      if (phase !== 'B') return;
      if (curr.mtimeMs === prev.mtimeMs && curr.size === prev.size) return;
      const now = Date.now();
      if (result.phase_b.trigger_seen_epoch_ms !== null && result.phase_b.watchfile_epoch_ms === null) {
        result.phase_b.watchfile_epoch_ms = now;
      }
      noteContent('watchfile', now).catch(() => {});
    });
  } catch (err) {
    recordWatcherError('fs_watchfile', err);
  }

  // --- Phase A: mutate through the mount and wait for our own watchers. ----
  const phaseAStart = Date.now();
  await new Promise((resolve) => setTimeout(resolve, 300));
  await fsp.writeFile(target, 'v1b-own-write\n');
  const phaseADeadline = Date.now() + PHASE_A_WAIT_MS;
  while (Date.now() < phaseADeadline && !(result.phase_a.file_event || result.phase_a.dir_event)) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  // --- Sentinel barrier: flush the phase-A event backlog. -------------------
  // One more probe-owned write; the NEXT target event is the barrier. inotify
  // delivers per-watch in order, so every event at/before the barrier is
  // phase-A backlog however delayed its delivery (or its read) is. Only after
  // the barrier does the probe signal ready — the harness's remote mutation
  // therefore happens provably post-barrier, and pre-barrier events can never
  // be credited as remote. If the barrier cannot be observed (broken
  // watchers), stay conservative: armed stays false and no event is credited.
  await new Promise((resolve) => setTimeout(resolve, 200));
  barrierEpoch = await new Promise((resolve) => {
    barrierResolve = resolve;
    fsp.writeFile(target, 'v1b-own-write\n').catch(() => {});
    setTimeout(() => {
      if (barrierResolve === resolve) {
        barrierResolve = null;
        resolve(null);
      }
    }, 2000);
  });
  armed = barrierEpoch !== null && (result.phase_a.file_event || result.phase_a.dir_event);
  result.barrier_epoch_ms = barrierEpoch;
  result.armed = armed;

  // --- Hand off to the harness, then watch for the remote mutation. --------
  await fsp.writeFile(readyPath, `ready ${Date.now()} armed=${armed}\n`);
  phase = 'B';

  const phaseBDeadline = Date.now() + PHASE_B_WAIT_MS;
  let statBaseline = null;
  while (Date.now() < phaseBDeadline) {
    let stamp = null;
    try {
      stamp = await fsp.readFile(trigger, 'utf8');
    } catch {
      stamp = null;
    }
    if (stamp) {
      result.phase_b.trigger_seen_epoch_ms ??= Date.now();
      result.phase_b.trigger_value = stamp.trim().slice(0, 40);
      // Capture the pre-mutation stat baseline only once: re-capturing it on
      // every poll would fold an already-applied mutation into the baseline
      // and the raw-stat-poll channel could never detect the change.
      if (statBaseline === null) {
        const baseline = await fsp.stat(target).catch(() => null);
        statBaseline = baseline ? { size: baseline.size, mtimeMs: baseline.mtimeMs } : null;
      }
    }
    const now = Date.now();
    if (result.phase_b.trigger_seen_epoch_ms !== null) {
      const stat = await fsp.stat(target).catch(() => null);
      if (stat) {
        result.phase_b.stat_size = stat.size;
        result.phase_b.stat_mtime_ms = stat.mtimeMs;
        // Stamp the raw-stat-poll channel from its own observation so the
        // metric survives even when a watch event's content read wins the
        // race to first see the new bytes.
        if (statBaseline && (stat.size !== statBaseline.size || stat.mtimeMs !== statBaseline.mtimeMs)) {
          result.phase_b.statpoll_epoch_ms ??= now;
        }
      }
      await noteContent('statpoll', now);
    }
    if (result.phase_b.content_verified) break;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  result.finished_epoch_ms = Date.now();
  // --- Finalize attribution -------------------------------------------------
  // Candidates were collected when their event's read confirmed the exact
  // payload; re-check them against the FINAL monotone pre-mutation bound so
  // a stale-content observation that completed after a candidate was
  // collected retroactively fences it (async read completion ordering).
  if (armed && barrierEpoch !== null) {
    for (const channel of ['file_watch', 'dir_watch']) {
      const candidates = pendingEventEpochs[channel];
      const credited = candidates.filter((epoch) => epoch >= barrierEpoch && epoch >= lastPreMutationObsEpoch);
      if (credited.length > 0) {
        result.phase_b[`${channel}_epoch_ms`] = Math.min(...credited);
      }
      const fenced = candidates.filter((epoch) => !credited.includes(epoch));
      if (fenced.length > 0) {
        result.phase_b.fenced_event_epochs ??= result.phase_b.fenced_event_epochs || [];
        for (const epoch of fenced.slice(0, 8)) {
          result.phase_b.fenced_event_epochs.push(`${channel}@${epoch} (bound ${lastPreMutationObsEpoch})`);
        }
      }
    }
  } else {
    result.phase_b.attribution_disarmed = true;
  }
  // When the v2 content was never confirmed, record what a fresh read
  // actually returned so the artifact shows whether the mount served stale
  // data, truncated data, or read errors alongside the updated stat.
  if (!result.phase_b.content_verified) {
    result.phase_b.final_content = (await readContent()) ?? null;
  }
  if (fileWatcher) fileWatcher.close();
  if (dirWatcher) dirWatcher.close();
  try {
    fs.unwatchFile(target);
  } catch {
    /* already closed */
  }
  await fsp.writeFile(resultPath, JSON.stringify(result, null, 2) + '\n');
  process.exit(result.phase_b.content_verified ? 0 : 3);
}

main().catch((err) => {
  result.crash = String(err && err.stack ? err.stack : err);
  fsp
    .writeFile(resultPath, JSON.stringify(result, null, 2) + '\n')
    .catch(() => {})
    .finally(() => process.exit(1));
});
