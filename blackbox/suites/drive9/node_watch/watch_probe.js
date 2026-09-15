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

async function readContent() {
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
      result.phase_b[`${channel}_epoch_ms`] ??= epochMs;
    } else if (content.startsWith(V2_PREFIX)) {
      // A truncated or wrong-round v2 payload: keep observing — only the
      // full expected payload satisfies the correctness floor.
      result.phase_b.content_match = content.length < expectedPayload.length ? 'truncated' : 'mismatch';
      if (content.length < expectedPayload.length) {
        result.phase_b.partial_content ??= `${content.length}/${expectedPayload.length} bytes: ${content.slice(0, 80)}`;
      } else {
        result.phase_b.other_content ??= content.slice(0, 80);
      }
    }
    // Anything else (e.g. the phase-A own-write content) is simply pre-mutation
    // state, not a remote-round payload — no classification.
  }
  return content;
}

async function main() {
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
      } else if (result.phase_b.trigger_seen_epoch_ms !== null && result.phase_b.file_watch_epoch_ms === null) {
        result.phase_b.file_watch_epoch_ms = now;
        result.phase_b.file_event_type = eventType;
      }
      if (phase === 'B') noteContent('file_watch', now).catch(() => {});
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
      } else if (result.phase_b.trigger_seen_epoch_ms !== null && result.phase_b.dir_watch_epoch_ms === null) {
        result.phase_b.dir_watch_epoch_ms = now;
        result.phase_b.dir_event_type = eventType;
      }
      if (phase === 'B') noteContent('dir_watch', now).catch(() => {});
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

  // --- Hand off to the harness, then watch for the remote mutation. --------
  await fsp.writeFile(readyPath, `ready ${Date.now()}\n`);
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
