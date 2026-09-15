#!/usr/bin/env node
// Deterministic Node.js fs smoke workload for a Drive9 FUSE mount.
//
// Modes:
//   create <workDir> <manifestOut>   Build the fixture tree through the Node
//                                    fs/promises/stream APIs, verify semantics
//                                    inline, and record checksums to a manifest
//                                    on the local host (not the mount).
//   verify <workDir> <manifestIn>    Re-verify the fixture against the manifest
//                                    (used after unmount/remount).
//   probe <path>                     Print "<sha256> <size>" for one file.
//
// Every check prints "PASS <name>" / "FAIL <name>: <detail>" and the process
// exits non-zero when any check failed. Only Node stdlib is used so the suite
// runs on any stock node >= 18.17.

'use strict';

const crypto = require('crypto');
const fsp = require('fs/promises');
const fs = require('fs');
const path = require('path');

let passed = 0;
let failed = 0;

function ok(name, cond, detail) {
  if (cond) {
    passed += 1;
    console.log(`PASS ${name}`);
  } else {
    failed += 1;
    console.log(`FAIL ${name}: ${detail === undefined ? 'unexpected' : detail}`);
  }
}

function shown(value) {
  return JSON.stringify(value, (_, v) => (typeof v === 'bigint' ? `${v}n` : v));
}

function eq(name, got, want) {
  ok(name, got === want, `want=${shown(want)} got=${shown(got)}`);
}

function sha256(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

// Deterministic payload independent of RNG: repeated sha256 counters.
function payload(seed, size) {
  const hash = crypto.createHash('sha256').update(seed).digest();
  const out = Buffer.alloc(size);
  for (let offset = 0; offset < size; offset += hash.length) {
    const round = crypto.createHash('sha256').update(hash).update(String(offset)).digest();
    round.copy(out, offset);
  }
  return out;
}

async function errorCode(fn) {
  try {
    await fn();
    return '';
  } catch (err) {
    return err.code || `ERR:${err.message}`;
  }
}

async function streamWrite(filePath, data, highWaterMark) {
  await new Promise((resolve, reject) => {
    const stream = fs.createWriteStream(filePath, { highWaterMark });
    stream.on('error', reject);
    stream.on('finish', resolve);
    stream.end(data);
  });
}

async function streamRead(filePath, highWaterMark) {
  const chunks = [];
  await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { highWaterMark });
    stream.on('error', reject);
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('end', resolve);
  });
  return Buffer.concat(chunks);
}

async function runCreate(workDir, manifestPath) {
  const largeBytes = Number(process.env.FUSE_NODEFS_LARGE_BYTES || 9 * 1024 * 1024);
  const large = payload('drive9-nodefs-large', largeBytes);
  const small = payload('drive9-nodefs-small', 64 * 1024);

  // --- basic sync roundtrip (the sync binding is the thing under test) ----
  fs.writeFileSync(path.join(workDir, 'roundtrip.txt'), 'drive9 node fs smoke\n', 'utf8');
  eq('writeFileSync/readFileSync utf8 roundtrip',
     fs.readFileSync(path.join(workDir, 'roundtrip.txt'), 'utf8'), 'drive9 node fs smoke\n');
  await fsp.appendFile(path.join(workDir, 'roundtrip.txt'), 'appended\n', 'utf8');
  eq('appendFile appends', (await fsp.readFile(path.join(workDir, 'roundtrip.txt'), 'utf8')).endsWith('appended\n'), true);

  // --- streams (default + small highWaterMark) ----------------------------
  const streamPath = path.join(workDir, 'large-stream.bin');
  await streamWrite(streamPath, large, 64 * 1024);
  const streamedBack = await streamRead(streamPath, 64 * 1024);
  eq('createWriteStream/createReadStream 64KiB checksum', sha256(streamedBack), sha256(large));
  const smallStreamPath = path.join(workDir, 'small-stream.bin');
  await streamWrite(smallStreamPath, small, 4096);
  eq('stream 4KiB highWaterMark checksum', sha256(await streamRead(smallStreamPath, 4096)), sha256(small));

  // --- FileHandle positional IO, sparse write, truncate, durability -------
  const handlePath = path.join(workDir, 'handle.bin');
  const handle = await fsp.open(handlePath, 'w+');
  try {
    await handle.write(payload('drive9-nodefs-head', 4096), 0, 4096, 0);
    const sparseOffset = 1024 * 1024;
    const tailPayload = payload('drive9-nodefs-tail', 4096);
    await handle.write(tailPayload, 0, 4096, sparseOffset);
    const statAfterSparse = await handle.stat();
    eq('positional sparse write grows size', statAfterSparse.size, sparseOffset + 4096);
    const hole = Buffer.alloc(4096);
    const { bytesRead: holeRead } = await handle.read(hole, 0, 4096, 4096);
    eq('hole between positional writes reads zeros', holeRead === 4096 && hole.every((b) => b === 0), true);
    const tail = Buffer.alloc(4096);
    const { bytesRead: tailRead } = await handle.read(tail, 0, 4096, sparseOffset);
    eq('positional read at offset', tailRead === 4096 && sha256(tail) === sha256(tailPayload), true);
    await handle.truncate(4096);
    eq('FileHandle.truncate shrinks', (await handle.stat()).size, 4096);
    await handle.datasync();
    await handle.sync();
  } finally {
    await handle.close();
  }
  eq('FileHandle data survives close', (await fsp.stat(handlePath)).size, 4096);

  // open with 'a' append flag
  const appendHandle = await fsp.open(path.join(workDir, 'append.bin'), 'a');
  try {
    await appendHandle.write('one');
    await appendHandle.write('two');
  } finally {
    await appendHandle.close();
  }
  eq("open 'a' appends across writes", (await fsp.readFile(path.join(workDir, 'append.bin'), 'utf8')), 'onetwo');

  // --- directories --------------------------------------------------------
  const nested = path.join(workDir, 'tree', 'a', 'b');
  await fsp.mkdir(nested, { recursive: true });
  await fsp.writeFile(path.join(nested, 'leaf.txt'), 'leaf\n');
  await fsp.symlink('leaf.txt', path.join(nested, 'leaf-link'));
  const dirents = await fsp.readdir(path.join(nested), { withFileTypes: true });
  const direntTypes = Object.fromEntries(dirents.map((d) => [d.name, d.isDirectory() ? 'dir' : d.isFile() ? 'file' : d.isSymbolicLink() ? 'symlink' : 'other']));
  eq('readdir withFileTypes reports types', direntTypes['leaf.txt'], 'file');
  eq('readdir withFileTypes symlink type', direntTypes['leaf-link'], 'symlink');
  const recursive = await fsp.readdir(path.join(workDir, 'tree'), { recursive: true, withFileTypes: true });
  eq('readdir recursive finds leaf', recursive.some((d) => d.name === 'leaf.txt'), true);
  const tmpDir = await fsp.mkdtemp(path.join(workDir, 'mkdtemp-'));
  eq('mkdtemp creates directory', (await fsp.stat(tmpDir)).isDirectory(), true);

  // --- rename, including atomic replace ------------------------------------
  const tmpTarget = path.join(workDir, 'publish.tmp');
  const finalTarget = path.join(workDir, 'published.txt');
  await fsp.writeFile(tmpTarget, 'v2\n');
  await fsp.rename(tmpTarget, finalTarget);
  eq('temp-write + rename publishes content', (await fsp.readFile(finalTarget, 'utf8')), 'v2\n');
  eq('renamed temp no longer exists', await errorCode(() => fsp.access(tmpTarget)), 'ENOENT');
  await fsp.writeFile(path.join(workDir, 'replace-old.txt'), 'old\n');
  await fsp.writeFile(path.join(workDir, 'replace-new.txt'), 'new\n');
  await fsp.rename(path.join(workDir, 'replace-new.txt'), path.join(workDir, 'replace-old.txt'));
  eq('rename over existing file replaces content', (await fsp.readFile(path.join(workDir, 'replace-old.txt'), 'utf8')), 'new\n');
  await fsp.mkdir(path.join(workDir, 'dir-rename-target'));
  await fsp.rename(path.join(workDir, 'dir-rename-target'), path.join(workDir, 'tree', 'renamed-dir'));
  eq('directory rename', (await fsp.stat(path.join(workDir, 'tree', 'renamed-dir'))).isDirectory(), true);

  // --- symlink + hardlink ---------------------------------------------------
  eq('readlink returns target', await fsp.readlink(path.join(nested, 'leaf-link')), 'leaf.txt');
  eq('lstat sees symlink', (await fsp.lstat(path.join(nested, 'leaf-link'))).isSymbolicLink(), true);
  eq('stat follows symlink', (await fsp.stat(path.join(nested, 'leaf-link'))).isFile(), true);
  const hardTarget = path.join(workDir, 'hard-origin.bin');
  await fsp.writeFile(hardTarget, payload('drive9-nodefs-hard', 8192));
  const hardLink = path.join(workDir, 'hard-link.bin');
  await fsp.link(hardTarget, hardLink);
  eq('hardlink nlink is 2', (await fsp.stat(hardTarget)).nlink, 2);
  eq('hardlink content parity', sha256(await fsp.readFile(hardLink)), sha256(payload('drive9-nodefs-hard', 8192)));
  await fsp.unlink(hardTarget);
  eq('hardlink survives unlink of origin', sha256(await fsp.readFile(hardLink)), sha256(payload('drive9-nodefs-hard', 8192)));

  // --- stat family ----------------------------------------------------------
  const statPath = path.join(workDir, 'stat-target.txt');
  await fsp.writeFile(statPath, 'stat me\n');
  const before = await fsp.stat(statPath);
  await new Promise((resolve) => setTimeout(resolve, 1100));
  await fsp.writeFile(statPath, 'stat me longer\n');
  const after = await fsp.stat(statPath);
  ok('stat size tracks write', after.size === 'stat me longer\n'.length, `size=${after.size}`);
  // The deliberate 1.1s gap above makes a strictly greater mtime safe even on
  // filesystems with 1-second timestamp granularity; `>=` would let a mount
  // that never advances mtime pass.
  ok('mtime strictly advances on write', after.mtimeMs > before.mtimeMs, `before=${before.mtimeMs} after=${after.mtimeMs}`);
  const bigStat = await fsp.stat(statPath, { bigint: true });
  eq('bigint stat size', bigStat.size, BigInt('stat me longer\n'.length));
  if (typeof fsp.statfs === 'function') {
    const sfs = await fsp.statfs(workDir);
    ok('statfs bsize positive', sfs.bsize > 0, `bsize=${sfs.bsize}`);
    ok('statfs blocks >= bfree >= bavail', sfs.blocks >= sfs.bfree && sfs.bfree >= sfs.bavail, `blocks=${sfs.blocks} bfree=${sfs.bfree} bavail=${sfs.bavail}`);
  }
  const resolved = await fsp.realpath(path.join(workDir, 'tree', 'a', 'b'));
  eq('realpath resolves nested dir', resolved.endsWith(path.join('tree', 'a', 'b')), true);
  if (fs.realpath.native) {
    const nativeResolved = await new Promise((resolve, reject) => fs.realpath.native(path.join(workDir, 'tree', 'a', 'b'), (err, p) => (err ? reject(err) : resolve(p))));
    eq('realpath.native agrees with realpath', nativeResolved, resolved);
  }

  // --- copyFile (plain + FICLONE flag exercises the clone/CoW fallback) ------
  const copyTarget = path.join(workDir, 'large-copy.bin');
  await fsp.copyFile(streamPath, copyTarget);
  eq('copyFile checksum', sha256(await fsp.readFile(copyTarget)), sha256(large));
  const ficloneTarget = path.join(workDir, 'large-copy-ficlone.bin');
  await fsp.copyFile(streamPath, ficloneTarget, fs.constants.COPYFILE_FICLONE);
  eq('copyFile COPYFILE_FICLONE checksum', sha256(await fsp.readFile(ficloneTarget)), sha256(large));

  // --- mode + timestamps ------------------------------------------------------
  const modePath = path.join(workDir, 'mode.txt');
  await fsp.writeFile(modePath, 'mode\n', { mode: 0o600 });
  await fsp.chmod(modePath, 0o600);
  eq('chmod 0600 visible in stat mode', (await fsp.stat(modePath)).mode & 0o777, 0o600);
  const utimesPath = path.join(workDir, 'utimes.txt');
  await fsp.writeFile(utimesPath, 'utimes\n');
  const stamp = new Date(Math.floor((Date.now() - 60_000) / 1000) * 1000);
  await fsp.utimes(utimesPath, stamp, stamp);
  eq('utimes sets mtime', Math.floor((await fsp.stat(utimesPath)).mtimeMs), Math.floor(stamp.getTime()));

  // --- error-code matrix (ecosystem tools switch on err.code) -----------------
  eq('readFile missing path -> ENOENT', await errorCode(() => fsp.readFile(path.join(workDir, 'definitely-missing.txt'))), 'ENOENT');
  eq('mkdir existing dir -> EEXIST', await errorCode(() => fsp.mkdir(path.join(workDir, 'tree'))), 'EEXIST');
  eq("open 'ax' existing -> EEXIST", await errorCode(() => fsp.open(finalTarget, 'ax')), 'EEXIST');
  eq('rmdir non-empty -> ENOTEMPTY', await errorCode(() => fsp.rmdir(path.join(workDir, 'tree'))), 'ENOTEMPTY');
  eq('readFile directory -> EISDIR', await errorCode(() => fsp.readFile(path.join(workDir, 'tree'))), 'EISDIR');

  // --- cross-channel fixture written by Node, read back by the CLI -----------
  const crossContent = `drive9-nodefs-cross-${process.pid}-${Date.now()}\n`;
  await fsp.mkdir(path.join(workDir, 'cross-channel'));
  await fsp.writeFile(path.join(workDir, 'cross-channel', 'node-written.txt'), crossContent, 'utf8');

  // --- cleanup of throwaway dirs ----------------------------------------------
  await fsp.rm(tmpDir, { recursive: true, force: true });
  eq('rm recursive removes mkdtemp dir', await errorCode(() => fsp.access(tmpDir)), 'ENOENT');

  // --- manifest: exact bytes for every persisted file ----------------------
  // Existence alone hides content loss (same-size zeroed files pass); record
  // sha256+size per entry and re-hash every entry after remount.
  const entryPaths = [
    'roundtrip.txt',
    'large-stream.bin',
    'small-stream.bin',
    'handle.bin',
    'append.bin',
    'published.txt',
    'replace-old.txt',
    'stat-target.txt',
    'hard-link.bin',
    'large-copy.bin',
    'large-copy-ficlone.bin',
    'mode.txt',
    'utimes.txt',
    'cross-channel/node-written.txt',
    'tree/a/b/leaf.txt',
  ];
  const entries = [];
  for (const rel of entryPaths) {
    const buf = await fsp.readFile(path.join(workDir, rel));
    entries.push({ path: rel, sha256: sha256(buf), size: buf.length });
  }
  const manifest = {
    large_sha256: sha256(large),
    large_size: large.length,
    small_sha256: sha256(small),
    hard_sha256: sha256(payload('drive9-nodefs-hard', 8192)),
    cross_content: crossContent,
    entries,
    symlink: 'tree/a/b/leaf-link',
    dir_entries: {
      top: ['append.bin', 'cross-channel', 'handle.bin', 'hard-link.bin', 'large-copy-ficlone.bin', 'large-copy.bin', 'large-stream.bin', 'mode.txt', 'published.txt', 'replace-old.txt', 'roundtrip.txt', 'small-stream.bin', 'stat-target.txt', 'tree', 'utimes.txt'],
      nested: ['leaf-link', 'leaf.txt'],
    },
  };
  await fsp.writeFile(manifestPath, JSON.stringify(manifest, null, 2) + '\n');
  return manifest;
}

async function runVerify(workDir, manifestPath) {
  const manifest = JSON.parse(await fsp.readFile(manifestPath, 'utf8'));
  for (const entry of manifest.entries) {
    const target = path.join(workDir, entry.path);
    let detail = '';
    try {
      const buf = await fsp.readFile(target);
      if (!(await fsp.stat(target)).isFile()) {
        detail = 'not a regular file';
      } else if (buf.length !== entry.size) {
        detail = `size want=${entry.size} got=${buf.length}`;
      } else if (sha256(buf) !== entry.sha256) {
        detail = 'sha256 mismatch';
      }
    } catch (err) {
      detail = err.code || err.message;
    }
    ok(`remounted entry content matches: ${entry.path}`, !detail, detail);
  }
  eq('remounted large stream checksum', sha256(await fsp.readFile(path.join(workDir, 'large-stream.bin'))), manifest.large_sha256);
  eq('remounted large stream size', (await fsp.stat(path.join(workDir, 'large-stream.bin'))).size, manifest.large_size);
  eq('remounted small stream checksum', sha256(await fsp.readFile(path.join(workDir, 'small-stream.bin'))), manifest.small_sha256);
  eq('remounted hardlink checksum', sha256(await fsp.readFile(path.join(workDir, 'hard-link.bin'))), manifest.hard_sha256);
  eq('remounted cross-channel content', (await fsp.readFile(path.join(workDir, 'cross-channel', 'node-written.txt'), 'utf8')), manifest.cross_content);
  eq('remounted symlink still resolves', (await fsp.readFile(path.join(workDir, manifest.symlink), 'utf8')), 'leaf\n');
  eq('remounted top-level readdir matches', (await fsp.readdir(workDir)).sort().join(','), manifest.dir_entries.top.join(','));
  eq('remounted nested readdir matches', (await fsp.readdir(path.join(workDir, 'tree', 'a', 'b'))).sort().join(','), manifest.dir_entries.nested.join(','));
  const dirents = await fsp.readdir(path.join(workDir, 'tree', 'a', 'b'), { withFileTypes: true });
  const leaf = dirents.find((d) => d.name === 'leaf.txt');
  const leafLink = dirents.find((d) => d.name === 'leaf-link');
  eq('remounted withFileTypes file type', leaf && leaf.isFile(), true);
  eq('remounted withFileTypes symlink type', leafLink && leafLink.isSymbolicLink(), true);
}

async function main() {
  const [mode, a, b] = process.argv.slice(2);
  const workDir = path.resolve(a);
  if (mode === 'create') {
    await fsp.mkdir(workDir, { recursive: true });
    await runCreate(workDir, path.resolve(b));
  } else if (mode === 'verify') {
    await runVerify(workDir, path.resolve(b));
  } else if (mode === 'probe') {
    const buf = await fsp.readFile(workDir);
    console.log(`${sha256(buf)} ${buf.length}`);
  } else {
    console.error(`usage: nodefs_smoke.js create|verify <workDir> <manifest> | probe <path>`);
    process.exit(2);
  }
  console.log(`RESULT: ${passed}/${passed + failed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error('nodefs smoke workload crashed:', err);
  process.exit(1);
});
