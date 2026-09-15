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
  // Negative control (no mount required), matching the reviewer's scenario:
  // generation-1 events keep arriving late (replayed after ready) with
  // delayed reads that straddle the remote write, and the remote write
  // itself produces ZERO watch notifications (gen-2 muted). The generation
  // boundary must keep those late gen-1 events inert: no candidate may be
  // credited, while content still verifies through polling. Removing the
  // generation guard makes the replayed gen-1 events dispatch reads that
  // return the payload after the write, credit an epoch, and FAIL this test
  // (verified during development by disabling the guard).
  const os = require('os');
  const { spawn } = require('child_process');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'watch-probe-selftest-'));
  const payload = `v2-remote-selftest-${Date.now()}\n`;
  const resultPathSelf = path.join(dir, 'result.json');
  const readyPathSelf = path.join(dir, 'ready');
  const child = spawn(process.execPath, [__filename, dir, resultPathSelf, readyPathSelf, payload], {
    env: {
      ...process.env,
      WATCH_PROBE_DELAY_READ_MS: '700',
      WATCH_PROBE_TEST_WATCH_BACKEND: 'fake',
      WATCH_PROBE_TEST_REPLAY_GEN1_AFTER_READY_MS: '30',
      WATCH_PROBE_TEST_MUTE_GEN2: '1',
    },
    stdio: 'ignore',
  });
  const deadline = Date.now() + 30000;
  while (!fs.existsSync(readyPathSelf) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  if (!fs.existsSync(readyPathSelf)) {
    child.kill('SIGKILL');
    throw new Error('selftest: probe never armed');
  }
  // Timing contract: the replayed gen-1 event fires at ready+30ms and its
  // 700ms-delayed read must be the FIRST observer of the payload (the loop's
  // statpoll read cannot start before the trigger exists, so the trigger is
  // written at +100ms and the payload at +300ms). With the generation guard
  // removed, that first-observer read credits the stale event and this test
  // fails; with the guard, gen-1 handlers are inert and only the poll loop
  // ever observes the payload.
  await new Promise((resolve) => setTimeout(resolve, 100));
  await fsp.writeFile(path.join(dir, 'trigger.stamp'), `${Date.now()}\n`);
  await new Promise((resolve) => setTimeout(resolve, 200));
  await fsp.writeFile(path.join(dir, 'target.txt'), payload);
  const code = await new Promise((resolve) => child.on('exit', resolve));
  const report = JSON.parse(await fsp.readFile(resultPathSelf, 'utf8'));
  const pb = report.phase_b;
  const failures = [];
  if (code !== 0) failures.push(`probe exit=${code}`);
  if (!pb.content_verified) failures.push('content not verified');
  if (report.armed !== true) failures.push('probe not armed');
  for (const channel of ['file_watch_epoch_ms', 'dir_watch_epoch_ms']) {
    if (pb[channel] !== null) {
      failures.push(`${channel}=${pb[channel]} credited from late gen-1 events despite zero gen-2 notifications`);
    }
  }
  fs.rmSync(dir, { recursive: true, force: true });
  if (failures.length > 0) {
    throw new Error(`selftest failed: ${failures.join('; ')}`);
  }
  console.log('watch probe selftest: late gen-1 events + zero gen-2 notifications credit nothing; content verified');
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
// Generation boundary: attribution candidates may only come from
// generation-2 watchers, created after phase A closes its watchers and
// before the ready marker is written. Events delivered through generation-1
// watchers (however late, whichever channel) are generation-checked inert.
let generation = 1;
let barrierEpoch = null; // gen-2 watcher creation epoch (the causal boundary)
let armed = false;
const pendingEventEpochs = { file_watch: [], dir_watch: [] };

// Test-only hooks (used by `selftest`; never set in production runs):
// - WATCH_PROBE_DELAY_READ_MS: delay every content read, so a read started
//   before the mutation can straddle it.
// - WATCH_PROBE_TEST_WATCH_BACKEND=fake: scripted fake watchers driven by
//   the selftest instead of real fs.watch.
// - WATCH_PROBE_TEST_REPLAY_GEN1_AFTER_READY_MS: keep replaying synthetic
//   generation-1 events after ready (late backlog simulation).
// - WATCH_PROBE_TEST_MUTE_GEN2: gen-2 watchers never deliver events
//   (simulates a mount whose remote overwrite produces zero notifications).
const READ_DELAY_MS = parseInt(process.env.WATCH_PROBE_DELAY_READ_MS || '0', 10) || 0;
const WATCH_BACKEND = process.env.WATCH_PROBE_TEST_WATCH_BACKEND || 'real';
const REPLAY_GEN1_AFTER_READY_MS = parseInt(process.env.WATCH_PROBE_TEST_REPLAY_GEN1_AFTER_READY_MS || '0', 10) || 0;
const MUTE_GEN2 = process.env.WATCH_PROBE_TEST_MUTE_GEN2 === '1';

// Fake watcher registry for the test backend: the selftest replays events
// into stashed generation-1 callbacks to simulate arbitrarily late backlog.
const fakeWatchRegistry = [];

function fakeWatchRegister(channel, gen, handler) {
  const rec = { channel, gen, handler, closed: false };
  fakeWatchRegistry.push(rec);
  return {
    close() {
      rec.closed = true;
    },
  };
}

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

  // Watchers are created per generation. Generation 1 serves phase A only;
  // before signaling ready the probe closes them and creates generation-2
  // watchers that alone may collect attribution candidates. Events delivered
  // late through a closed generation-1 watcher hit a generation-checked
  // handler and are inert — no read, no candidate — so phase-A backlog can
  // never be laundered into a remote event, whichever channel it lands on.
  let fileWatcher = null;
  let dirWatcher = null;
  const pendingReads = new Set();

  function dispatchNote(channel, epochMs) {
    const p = noteContent(channel, epochMs).catch(() => {});
    pendingReads.add(p);
    p.finally(() => pendingReads.delete(p)).catch(() => {});
  }

  function makeFileHandler(gen) {
    return (eventType) => {
      const now = Date.now();
      if (gen !== generation) return;
      if (phase === 'A') {
        if (!result.phase_a.file_event) {
          result.phase_a.file_event = true;
          result.phase_a.ms ??= now - phaseAStart;
        }
      } else if (armed) {
        result.phase_b.file_event_type ??= eventType;
        dispatchNote('file_watch', now);
      }
    };
  }

  function makeDirHandler(gen) {
    return (eventType, filename) => {
      const name = path.basename(String(filename || ''));
      if (name !== 'target.txt') return;
      const now = Date.now();
      if (gen !== generation) return;
      if (phase === 'A') {
        if (!result.phase_a.dir_event) {
          result.phase_a.dir_event = true;
          result.phase_a.ms ??= now - phaseAStart;
        }
      } else if (armed) {
        result.phase_b.dir_event_type ??= eventType;
        dispatchNote('dir_watch', now);
      }
    };
  }

  function openWatchers(gen) {
    const handlers = {};
    try {
      if (WATCH_BACKEND === 'fake') {
        handlers.file = fakeWatchRegister('file', gen, makeFileHandler(gen));
        handlers.dir = fakeWatchRegister('dir', gen, makeDirHandler(gen));
        if (gen >= 2 && MUTE_GEN2) {
          // Test hook: gen-2 watchers never deliver events (simulates a mount
          // whose remote overwrite produces zero notifications).
          handlers.file = { close() {} };
          handlers.dir = { close() {} };
          result.watcher_backend = 'fake+mute-gen2';
        } else {
          result.watcher_backend = 'fake';
        }
      } else {
        const fw = fs.watch(target, makeFileHandler(gen));
        fw.on('error', (err) => {
          recordWatcherError('fs_watch_file', err);
          try {
            fw.close();
          } catch {
            /* already closed */
          }
        });
        const dw = fs.watch(mountDir, makeDirHandler(gen));
        dw.on('error', (err) => {
          recordWatcherError('fs_watch_dir', err);
          try {
            dw.close();
          } catch {
            /* already closed */
          }
        });
        handlers.file = fw;
        handlers.dir = dw;
      }
    } catch (err) {
      recordWatcherError(`fs_watch_gen${gen}`, err);
    }
    return handlers;
  }

  function closeWatchers(handlers) {
    for (const w of Object.values(handlers || {})) {
      try {
        if (w && typeof w.close === 'function') w.close();
      } catch {
        /* already closed */
      }
    }
  }

  let gen1Handlers = openWatchers(1);
  fileWatcher = gen1Handlers.file;
  dirWatcher = gen1Handlers.dir;
  if (WATCH_BACKEND === 'fake') {
    // Fake watchers deliver nothing spontaneously; treat phase A as observed
    // so the selftest skips the real-event wait.
    result.phase_a.file_event = true;
    result.phase_a.dir_event = true;
  }
  try {
    fs.watchFile(target, { interval: 200, persistent: false }, (curr, prev) => {
      if (phase !== 'B') return;
      if (curr.mtimeMs === prev.mtimeMs && curr.size === prev.size) return;
      const now = Date.now();
      if (result.phase_b.trigger_seen_epoch_ms !== null && result.phase_b.watchfile_epoch_ms === null) {
        result.phase_b.watchfile_epoch_ms = now;
      }
      dispatchNote('watchfile', now);
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

  // --- Generation boundary: retire phase-A watchers, arm fresh ones. --------
  // Close the generation-1 watchers (late deliveries stay inert via the
  // generation check), let any in-flight event-loop callbacks settle, then
  // create generation-2 watchers. inotify/kqueue only report events occurring
  // after watch establishment, so gen-2 watchers cannot observe phase-A
  // writes; the harness mutates only after seeing ready, i.e. strictly after
  // gen-2 exists. Any event they deliver is therefore attributable to the
  // remote window by construction — no "identify the sentinel event" guess.
  generation = 2; // gen-1 handlers become inert from here on
  closeWatchers(gen1Handlers);
  gen1Handlers = null;
  await new Promise((resolve) => setTimeout(resolve, 300));
  const gen2Handlers = openWatchers(2);
  fileWatcher = gen2Handlers.file;
  dirWatcher = gen2Handlers.dir;
  barrierEpoch = Date.now();
  armed = true;
  result.barrier_epoch_ms = barrierEpoch;
  result.armed = armed;
  if (REPLAY_GEN1_AFTER_READY_MS > 0) {
    // Test hook: replay synthetic generation-1 events after ready — late
    // phase-A backlog whose delayed reads may straddle the mutation. With the
    // generation guard these are inert; without it they would be credited.
    setTimeout(() => {
      const timer = setInterval(() => {
        // Deliver through gen-1 records even though their handles are
        // closed: real watchers can still fire callbacks for events that
        // were queued before close — exactly the late-backlog hazard this
        // regression simulates.
        for (const rec of fakeWatchRegistry) {
          if (rec.gen === 1) {
            rec.handler('change', 'target.txt');
          }
        }
      }, 100);
      setTimeout(() => clearInterval(timer), PHASE_B_WAIT_MS - REPLAY_GEN1_AFTER_READY_MS);
    }, REPLAY_GEN1_AFTER_READY_MS);
  }

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

  // --- Teardown, then drain in-flight reads BEFORE finalizing attribution ---
  // New events stop here; every fire-and-forget noteContent read settles so
  // the monotone pre-mutation bound is final when candidates are re-checked.
  // Without the drain, a read completing after the loop break could advance
  // the bound past an already-recorded candidate and the retroactive fence
  // would silently not apply.
  if (fileWatcher) fileWatcher.close();
  if (dirWatcher) dirWatcher.close();
  closeWatchers(gen2Handlers);
  try {
    fs.unwatchFile(target);
  } catch {
    /* already closed */
  }
  await Promise.allSettled([...pendingReads]);
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
