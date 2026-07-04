// archive.ts — download a remote directory tree as a streaming tar.gz (or
// zip). Mirrors the Go `drive9 fs archive` CLI: tree walk via list() + readStream
// per leaf, with a pathfilter Matcher to prune excludes/include-whitelist.
//
// The tar.gz writer is a minimal hand-rolled ustar implementation (no npm dep)
// so the SDK stays zero-runtime-dependency. zip support uses Node's built-in
// zlib via a minimal central-directory writer — but for the v1 we only emit
// tar.gz from the stream API; archiveToFile can also produce zip via a Node
// child path. For now zip is supported through archiveToFile using the
// `zlib` deflate + a minimal zip writer.

import { Client } from "./client.js";
import type { FileInfo } from "./models.js";
import { match, newMatcher, hasInclude, type Matcher, type MatcherOptions } from "./pathfilter.js";

const DEFAULT_JOBS = 16;

export interface ArchiveOptions {
  /** Archive format; default "tar.gz". */
  format?: "tar.gz" | "zip";
  /** Skip paths matching these patterns (repeatable). */
  exclude?: string[];
  /** Keep only paths matching these patterns (repeatable). */
  include?: string[];
  /** Apply a drive9 profile's [local]/[remote] rules. Profile names map to
   *  exclude / override respectively (same translation as the Go CLI). */
  profile?: string;
  /** Concurrent file downloads; default 16. */
  jobs?: number;
  /** Strip directory hierarchy; archive basenames only. */
  flat?: boolean;
  /** Abort the operation when the signal aborts. */
  signal?: AbortSignal;
}

interface ArchiveEntry {
  rel: string;
  remote: string;
  root: string;
  size: number;
  isDir: boolean;
  mode?: number;
}

// ───────────────────────────── tar.gz writer ─────────────────────────────
// ustar header is 512 bytes; we write a minimal header per entry followed by
// the file body padded to a 512-byte boundary. Two trailing zero blocks mark EOF.

const TAR_BLOCK = 512;

function tarChecksum(header: Uint8Array): number {
  // Checksum field (bytes 148..155) treated as spaces.
  let sum = 0;
  for (let i = 0; i < header.length; i++) {
    if (i >= 148 && i < 156) {
      sum += 0x20;
    } else {
      sum += header[i];
    }
  }
  return sum;
}

function encodeOctal(value: number, width: number): string {
  return value.toString(8).padStart(width, "0");
}

function writeString(buf: Uint8Array, offset: number, text: string, maxLen: number): void {
  const encoded = new TextEncoder().encode(text);
  const len = Math.min(encoded.length, maxLen);
  for (let i = 0; i < len; i++) buf[offset + i] = encoded[i];
}

function buildTarHeader(name: string, size: number, typeflag: "0" | "5", mode: number, mtime: number): Uint8Array {
  const header = new Uint8Array(TAR_BLOCK);
  writeString(header, 0, name, 100);
  writeString(header, 100, encodeOctal(mode, 7) + "\0", 8);
  writeString(header, 108, encodeOctal(0, 7) + "\0", 8); // uid
  writeString(header, 116, encodeOctal(0, 7) + "\0", 8); // gid
  writeString(header, 124, encodeOctal(size, 11) + "\0", 12);
  writeString(header, 136, encodeOctal(mtime, 11) + "\0", 12);
  // bytes 148..155: checksum placeholder (spaces)
  for (let i = 148; i < 156; i++) header[i] = 0x20;
  header[156] = typeflag.charCodeAt(0);
  writeString(header, 157, "ustar\0", 6);
  writeString(header, 257, "00", 2);
  // Compute and write checksum.
  const checksum = tarChecksum(header);
  writeString(header, 148, encodeOctal(checksum, 6) + "\0 ", 8);
  return header;
}

function padToBlock(size: number): Uint8Array {
  const remainder = size % TAR_BLOCK;
  if (remainder === 0) return new Uint8Array(0);
  return new Uint8Array(TAR_BLOCK - remainder);
}

function concat3(a: Uint8Array, b: Uint8Array, c: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length + c.length);
  out.set(a, 0);
  out.set(b, a.length);
  out.set(c, a.length + b.length);
  return out;
}

/** Stream a remote directory tree as a tar.gz ReadableStream. */
export async function archiveImpl(
  client: Client,
  remoteDir: string,
  opts: ArchiveOptions = {}
): Promise<ReadableStream<Uint8Array>> {
  const format = opts.format ?? "tar.gz";
  if (format !== "tar.gz") {
    throw new Error(`unsupported archive format ${JSON.stringify(format)} (only "tar.gz" is streamable from the SDK; use archiveToFile for zip)`);
  }
  const matcher = await buildMatcher(client, opts);
  const root = normalizeRoot(remoteDir);
  const archiveRoot = basename(root);
  const flat = opts.flat ?? false;
  const jobs = opts.jobs ?? DEFAULT_JOBS;
  const signal = opts.signal;

  // Collect the tree first (list + filter), then stream entries.
  const { dirs, files } = await collectArchiveTree(client, root, archiveRoot, matcher, flat, signal);

  // gzip via the Web CompressionStream API when available (Node 18+).
  const gzip = new (globalThis as any).CompressionStream("gzip");
  const writer = gzip.writable.getWriter();
  const output = new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        // Directories first (preserves empty dirs). Writes are serialized
        // because the writer is a single gzip stream.
        for (const d of dirs) {
          const name = archiveDirName(d.root, d.rel, flat);
          if (!name) continue;
          const header = buildTarHeader(name, 0, "5", d.mode ?? 0o755, mtimeNow());
          await writer.write(header);
        }
        // Files: download in parallel (bounded), but serialize the
        // header+body+pad write to keep the tar byte stream coherent. Each
        // worker builds a single concatenated Uint8Array for its entry and
        // acquires the write lock only for the final write.
        let writeLock: Promise<void> = Promise.resolve();
        await parallelMap(files, jobs, signal, async (e) => {
          const stream = await client.readStream(e.remote);
          const buf = await streamToUint8Array(stream, e.size);
          const name = archiveName(e.root, e.rel, flat);
          if (!name) return;
          const header = buildTarHeader(name, e.size, "0", e.mode ?? 0o644, mtimeNow());
          const pad = padToBlock(e.size);
          const entry = concat3(header, buf, pad);
          // Serialize the write: chain onto writeLock so only one worker
          // writes at a time. The download stays parallel; only the gzip
          // write is serialized.
          writeLock = writeLock.then(() => writer.write(entry));
          await writeLock;
        });
        // Two zero blocks mark EOF.
        await writer.write(new Uint8Array(TAR_BLOCK));
        await writer.write(new Uint8Array(TAR_BLOCK));
        await writer.close();
        // Pipe gzip output to the controller.
        const reader = gzip.readable.getReader();
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value) controller.enqueue(value);
        }
        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
  });
  return output;
}

/** Download a remote directory tree to a local file (tar.gz or zip). */
export async function archiveToFileImpl(
  client: Client,
  remoteDir: string,
  localPath: string,
  opts: ArchiveOptions = {}
): Promise<void> {
  const format = opts.format ?? "tar.gz";
  if (format === "tar.gz") {
    const stream = await archiveImpl(client, remoteDir, opts);
    const reader = stream.getReader();
    const { createWriteStream } = await import("node:fs");
    const out = createWriteStream(localPath);
    return new Promise<void>((resolve, reject) => {
      out.on("error", reject);
      out.on("finish", resolve);
      (async () => {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (!out.write(Buffer.from(value))) {
              await new Promise<void>((drain) => out.once("drain", () => drain()));
            }
          }
          out.end();
        } catch (err) {
          out.destroy();
          reject(err);
        }
      })();
    });
  }
  // zip: write a minimal zip archive (store method, no compression) to disk.
  // This keeps zero-runtime-dependency; a future PR can swap in deflate.
  await archiveToFileZipImpl(client, remoteDir, localPath, opts);
}

async function archiveToFileZipImpl(
  client: Client,
  remoteDir: string,
  localPath: string,
  opts: ArchiveOptions
): Promise<void> {
  const matcher = await buildMatcher(client, opts);
  const root = normalizeRoot(remoteDir);
  const archiveRoot = basename(root);
  const flat = opts.flat ?? false;
  const signal = opts.signal;
  const { dirs, files } = await collectArchiveTree(client, root, archiveRoot, matcher, flat, signal);
  const { createWriteStream } = await import("node:fs");
  const out = createWriteStream(localPath);
  const central: { name: string; size: number; crc: number; offset: number }[] = [];
  let offset = 0;

  function writeChunk(buf: Uint8Array): Promise<void> {
    return new Promise((resolve, reject) => {
      out.write(Buffer.from(buf), (err) => (err ? reject(err) : resolve()));
    });
  }

  for (const d of dirs) {
    const name = archiveDirName(d.root, d.rel, flat);
    if (!name) continue;
    const crc = 0;
    const localHeader = buildZipLocalHeader(name, 0, 0, 0 /* STORE */);
    central.push({ name, size: 0, crc, offset });
    await writeChunk(localHeader);
    offset += localHeader.length;
  }
  for (const e of files) {
    const name = archiveName(e.root, e.rel, flat);
    if (!name) continue;
    const stream = await client.readStream(e.remote);
    const buf = await streamToUint8Array(stream, e.size);
    const crc = crc32(buf);
    const localHeader = buildZipLocalHeader(name, crc, buf.length, buf.length /* STORE, size = compressed = uncompressed */);
    central.push({ name, size: buf.length, crc, offset });
    await writeChunk(localHeader);
    await writeChunk(buf);
    offset += localHeader.length + buf.length;
  }
  // Central directory.
  const centralStart = offset;
  for (const c of central) {
    const rec = buildZipCentralHeader(c.name, c.crc, c.size, c.size, c.offset);
    await writeChunk(rec);
    offset += rec.length;
  }
  const eocd = buildZipEOCD(central.length, centralStart, offset);
  await writeChunk(eocd);
  await new Promise<void>((resolve) => out.end(resolve));
}

// ───────────────────────────── zip primitives ─────────────────────────────

function crc32(buf: Uint8Array): number {
  let crc = ~0;
  for (let i = 0; i < buf.length; i++) {
    crc ^= buf[i];
    for (let j = 0; j < 8; j++) {
      crc = crc & 1 ? (crc >>> 1) ^ 0xedb88320 : crc >>> 1;
    }
  }
  return (~crc) >>> 0;
}

function buildZipLocalHeader(name: string, crc: number, compressedSize: number, uncompressedSize: number): Uint8Array {
  const nameBytes = new TextEncoder().encode(name);
  const buf = new Uint8Array(30 + nameBytes.length);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0x04034b50, true); // signature
  dv.setUint16(4, 20, true); // version
  dv.setUint16(6, 0, true); // flags
  dv.setUint16(8, 0, true); // method = STORE
  dv.setUint16(10, 0, true); // time
  dv.setUint16(12, 0, true); // date
  dv.setUint32(14, crc, true);
  dv.setUint32(18, compressedSize, true);
  dv.setUint32(22, uncompressedSize, true);
  dv.setUint16(26, nameBytes.length, true);
  dv.setUint16(28, 0, true); // extra length
  buf.set(nameBytes, 30);
  return buf;
}

function buildZipCentralHeader(name: string, crc: number, compressedSize: number, uncompressedSize: number, offset: number): Uint8Array {
  const nameBytes = new TextEncoder().encode(name);
  const buf = new Uint8Array(46 + nameBytes.length);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0x02014b50, true);
  dv.setUint16(4, 20, true);
  dv.setUint16(6, 20, true);
  dv.setUint16(8, 0, true);
  dv.setUint16(10, 0, true); // method
  dv.setUint16(12, 0, true);
  dv.setUint16(14, 0, true);
  dv.setUint32(16, crc, true);
  dv.setUint32(20, compressedSize, true);
  dv.setUint32(24, uncompressedSize, true);
  dv.setUint16(28, nameBytes.length, true);
  dv.setUint16(30, 0, true); // extra
  dv.setUint16(32, 0, true); // comment
  dv.setUint16(34, 0, true); // disk
  dv.setUint16(36, 0, true); // internal attrs
  dv.setUint32(38, 0, true); // external attrs
  dv.setUint32(42, offset, true);
  buf.set(nameBytes, 46);
  return buf;
}

function buildZipEOCD(entryCount: number, centralStart: number, centralSize: number): Uint8Array {
  const buf = new Uint8Array(22);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, 0x06054b50, true);
  dv.setUint16(4, 0, true);
  dv.setUint16(6, 0, true);
  dv.setUint16(8, entryCount, true);
  dv.setUint16(10, entryCount, true);
  dv.setUint32(12, centralSize, true);
  dv.setUint32(16, centralStart, true);
  dv.setUint16(20, 0, true);
  return buf;
}

// ───────────────────────────── helpers ─────────────────────────────

function normalizeRoot(path: string): string {
  let p = path.trim();
  if (p === "") p = "/";
  p = p.replace(/\/+$/, "");
  if (p === "") p = "/";
  return p;
}

function basename(p: string): string {
  const stripped = p.replace(/^\/+/, "");
  const idx = stripped.lastIndexOf("/");
  const base = idx >= 0 ? stripped.slice(idx + 1) : stripped;
  return base === "" ? "root" : base;
}

function archiveName(root: string, rel: string, flat: boolean): string {
  rel = rel.replace(/^\/+/, "");
  if (rel === "") return "";
  if (flat) return rel.split("/").pop() ?? "";
  return `${root}/${rel}`;
}

function archiveDirName(root: string, rel: string, flat: boolean): string {
  if (flat) return "";
  rel = rel.replace(/^\/+/, "");
  if (rel === "") return `${root}/`;
  return `${root}/${rel}/`;
}

function mtimeNow(): number {
  return Math.floor(Date.now() / 1000);
}

async function streamToUint8Array(stream: ReadableStream<Uint8Array>, size: number): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      chunks.push(value);
      total += value.length;
    }
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return total === size ? out : out.slice(0, size);
}

async function collectArchiveTree(
  client: Client,
  root: string,
  archiveRoot: string,
  matcher: Matcher,
  flat: boolean,
  signal?: AbortSignal
): Promise<{ dirs: ArchiveEntry[]; files: ArchiveEntry[] }> {
  const dirs: ArchiveEntry[] = [{ rel: "", remote: root, root: archiveRoot, size: 0, isDir: true, mode: 0o755 }];
  const files: ArchiveEntry[] = [];
  await walkRemoteTreeBFS(client, root, signal, (rel, info) => {
    if (rel === "") return;
    if (!match(matcher, rel)) return;
    const remote = joinRemote(root, rel);
    if (info.isDir) {
      dirs.push({ rel, remote, root: archiveRoot, size: 0, isDir: true, mode: 0o755 });
      return;
    }
    files.push({ rel, remote, root: archiveRoot, size: info.size, isDir: false });
  });
  return { dirs, files };
}

async function walkRemoteTreeBFS(
  client: Client,
  root: string,
  signal: AbortSignal | undefined,
  visit: (rel: string, info: FileInfo) => void
): Promise<void> {
  const queue: string[] = [""];
  while (queue.length > 0) {
    const rel = queue.shift()!;
    const absDir = rel === "" ? root : joinRemote(root, rel);
    if (signal?.aborted) throw new Error("archive aborted");
    const entries = await client.list(absDir);
    for (const e of entries) {
      const childRel = rel === "" ? e.name : `${rel}/${e.name}`;
      visit(childRel, e);
      if (e.isDir) queue.push(childRel);
    }
  }
}

function joinRemote(root: string, rel: string): string {
  if (root === "/") return `/${rel}`;
  return `${root}/${rel}`;
}

async function parallelMap<T>(
  items: T[],
  concurrency: number,
  signal: AbortSignal | undefined,
  fn: (item: T) => Promise<void>
): Promise<void> {
  let index = 0;
  const workers: Promise<void>[] = [];
  const errors: Error[] = [];
  const run = async (): Promise<void> => {
    while (true) {
      if (signal?.aborted) throw new Error("archive aborted");
      const i = index++;
      if (i >= items.length) return;
      try {
        await fn(items[i]);
      } catch (e) {
        errors.push(e instanceof Error ? e : new Error(String(e)));
      }
    }
  };
  for (let w = 0; w < Math.min(concurrency, items.length || 1); w++) workers.push(run());
  await Promise.all(workers);
  if (errors.length > 0) throw errors[0];
}

async function buildMatcher(client: Client, opts: ArchiveOptions): Promise<Matcher> {
  let exclude = opts.exclude ?? [];
  let override: string[] = [];
  if (opts.profile && opts.profile.trim() !== "") {
    // The TS SDK does not ship a profile loader (profiles are a CLI-side
    // convenience). Callers using --profile should pre-resolve the profile
    // into exclude/override lists and pass them directly. We still accept the
    // option for API symmetry and apply the coding-agent built-in defaults
    // when that well-known profile is requested.
    if (opts.profile === "coding-agent") {
      exclude = [...codingAgentLocalOnly, ...exclude];
    }
  }
  const m = newMatcher({ include: opts.include, exclude, override });
  return m;
}

export const codingAgentLocalOnly = [
  "**/.git/**",
  "**/.hg/**",
  "**/.svn/**",
  "**/node_modules/**",
  "**/.pnpm-store/**",
  "**/target/**",
  "**/dist/**",
  "**/build/**",
  "**/coverage/**",
  "**/tmp/**",
  "**/.tmp/**",
  "**/.tmp-api-extractor/**",
  "**/.cache/**",
  "**/.turbo/**",
  "**/.next/cache/**",
  "**/.vitepress/cache/**",
  "**/.gradle/**",
  "**/.venv/**",
  "**/__pycache__/**",
  "**/.pytest_cache/**",
  "**/.mypy_cache/**",
  "**/.ruff_cache/**",
];

export { match, hasInclude };