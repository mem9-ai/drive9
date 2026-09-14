#!/usr/bin/env node
// Cross-channel fs.watch visibility probe for a Drive9 mount.
//
// usage: node watch_probe.js <mountDir> <resultJson(local)> <readyFile(local)>
//
// Phase A (own write): the probe writes target.txt through the mount itself
// and requires its own watchers to see it. Phase B (remote write): after the
// ready marker appears the harness mutates trigger.stamp and target.txt via
// the drive9 CLI (server-side); the probe measures which notification channel
// observes the change and how fast: fs.watch(file), fs.watch(dir),
// fs.watchFile stat polling, and a raw stat poll.
//
// Exit codes: 0 result written (change observed through >=1 channel),
//            3 change never observed (result still written), 1 crash.

'use strict';

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

const [mountDir, resultPath, readyPath] = process.argv.slice(2);
const target = path.join(mountDir, 'target.txt');
const trigger = path.join(mountDir, 'trigger.stamp');

const V1 = 'v1-probe\n';
const V2_PREFIX = 'v2-remote';
const PHASE_A_WAIT_MS = 15000;
const PHASE_B_WAIT_MS = 180000;

const result = {
  started_epoch_ms: Date.now(),
  mount_dir: mountDir,
  watcher_errors: {},
  phase_a: { file_event: false, dir_event: false, ms: null },
  phase_b: {
    trigger_seen_epoch_ms: null,
    content_verified: false,
    content_epoch_ms: null,
    file_watch_epoch_ms: null,
    dir_watch_epoch_ms: null,
    watchfile_epoch_ms: null,
    statpoll_epoch_ms: null,
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
    if (content.startsWith(V2_PREFIX)) {
      result.phase_b.content_verified = true;
      result.phase_b.content_epoch_ms ??= epochMs;
      result.phase_b.last_content = content.slice(0, 80);
      result.phase_b[`${channel}_epoch_ms`] ??= epochMs;
    }
  }
  return content;
}

async function main() {
  await fsp.writeFile(target, V1);

  let fileWatcher = null;
  let dirWatcher = null;
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
  } catch (err) {
    recordWatcherError('fs_watch_file', err);
  }
  try {
    dirWatcher = fs.watch(mountDir, (eventType, filename) => {
      const name = path.basename(String(filename || ''));
      if (name !== 'target.txt' && name !== 'trigger.stamp') return;
      const now = Date.now();
      if (phase === 'A') {
        if (name === 'target.txt' && !result.phase_a.dir_event) {
          result.phase_a.dir_event = true;
          result.phase_a.ms ??= now - phaseAStart;
        }
      } else if (name === 'target.txt' && result.phase_b.trigger_seen_epoch_ms !== null && result.phase_b.dir_watch_epoch_ms === null) {
        result.phase_b.dir_watch_epoch_ms = now;
        result.phase_b.dir_event_type = eventType;
      }
      if (phase === 'B') noteContent('dir_watch', now).catch(() => {});
    });
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
    }
    const now = Date.now();
    if (result.phase_b.trigger_seen_epoch_ms !== null) {
      const stat = await fsp.stat(target).catch(() => null);
      if (stat) {
        result.phase_b.stat_size = stat.size;
        result.phase_b.stat_mtime_ms = stat.mtimeMs;
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
