import * as fs from "fs";
import * as path from "path";

import { appendStreamImpl } from "./append.js";
import { bodyInit } from "./compat.js";
import { checkError, Drive9Error, StatusError } from "./error.js";
import {
  commitFSLayer,
  createFSLayer,
  checkpointFSLayer,
  diffFSLayer,
  getFSLayer,
  getFSLayerCheckpoint,
  getFSLayerEntry,
  listFSLayerEvents,
  listFSLayers,
  readFSLayerFile,
  readFSLayerFileStream,
  rollbackFSLayer,
  uploadFSLayerFile,
  upsertFSLayerEntry,
} from "./layer.js";
import {
  deleteGitWorkspace,
  getGitObjectPack,
  getGitOverlayEntry,
  getGitState,
  getGitWorkspace,
  getGitWorkspaceByRoot,
  listGitObjectPacks,
  listGitOverlayEntries,
  listGitTree,
  listGitWorkspaces,
  putGitObjectPack,
  putGitOverlayEntry,
  replaceGitTree,
  upsertGitState,
  upsertGitWorkspace,
} from "./git.js";
import {
  appendJournalEntries,
  createJournal,
  readJournalEntries,
  searchJournal,
  verifyJournal,
} from "./journal.js";
import { MaxBatchReadSmallPaths, MaxBatchStatPaths } from "./models.js";
import type {
  BatchReadSmallResult,
  BatchStatResult,
  EventHandler,
  EventLifecycle,
  FileInfo,
  FSLayer,
  FSLayerCheckpoint,
  FSLayerCheckpointRequest,
  FSLayerCommit,
  FSLayerCreateRequest,
  FSLayerEntry,
  FSLayerEntryRequest,
  FSLayerEvent,
  GitObjectPack,
  GitObjectPackRequest,
  GitOverlayEntry,
  GitOverlayEntryRequest,
  GitState,
  GitStateRequest,
  GitTreeNode,
  GitTreeReplaceRequest,
  GitWorkspace,
  GitWorkspaceRequest,
  IssueScopedTokenRequest,
  IssueScopedTokenResponse,
  Journal,
  JournalAppendResponse,
  JournalCreateRequest,
  JournalEntry,
  JournalEntryInput,
  JournalSearchMatch,
  JournalSearchRequest,
  JournalVerifyResult,
  SearchResult,
  StatMetadataResult,
  StatResult,
  TenantStatus,
  UploadSummary,
  VaultGrantIssueRequest,
  VaultGrantIssueResponse,
  VaultGrantRevokeRequest,
  WatchEventsOptions,
  WriteOptions,
} from "./models.js";
import { patchFileImpl, type ProgressFn, type ReadPartFn } from "./patch.js";
import { StreamWriter } from "./stream.js";
import { issueScopedToken, revokeScopedToken, revokeScopedTokenByAPIKey } from "./tokens.js";
import {
  readStreamImpl,
  readStreamRangeImpl,
  resumeUploadImpl,
  writeStreamImpl,
  writeStreamWithSummaryImpl,
} from "./transfer.js";
import { Semaphore } from "./utils.js";
import {
  createVaultSecret,
  deleteVaultSecret,
  issueVaultGrant,
  issueVaultToken,
  listReadableVaultSecrets,
  listVaultSecrets,
  queryVaultAudit,
  readVaultSecret,
  readVaultSecretAsOwner,
  readVaultSecretField,
  readVaultSecretFieldAsOwner,
  revokeVaultGrant,
  revokeVaultToken,
  updateVaultSecret,
} from "./vault.js";
import { watchEvents, watchEventsWithLifecycle } from "./events.js";

const DEFAULT_SMALL_FILE_THRESHOLD = 50_000;
const DEFAULT_SERVER = "https://api.drive9.ai";
const DOWNLOAD_DIR_CONCURRENCY = 16;

interface Drive9Config {
  server?: string;
  current_context?: string;
  contexts?: Record<string, Drive9Context>;
}

interface Drive9Context {
  type?: string;
  server?: string;
  api_key?: string;
}

function defaultConfigPath(): string {
  const override = process.env.DRIVE9_CONFIG?.trim();
  if (override) return override;
  const home = process.env.HOME || process.env.USERPROFILE || "";
  return path.join(home, ".drive9", "config");
}

function credentialsFromContext(cfg: Drive9Config, name: string): { server: string; apiKey: string } | undefined {
  const entry = cfg.contexts?.[name];
  const kind = entry?.type?.trim() || "owner";
  if (!entry || (kind !== "owner" && kind !== "fs_scoped")) return undefined;
  const apiKey = entry.api_key?.trim();
  if (!apiKey) return undefined;
  const server = entry.server?.trim() || cfg.server?.trim();
  if (!server) return undefined;
  return { server, apiKey };
}

function loadConfigFile(): { server?: string; apiKey?: string } | undefined {
  const cfgPath = defaultConfigPath();
  try {
    const data = fs.readFileSync(cfgPath, "utf-8");
    const cfg = JSON.parse(data) as Drive9Config;
    if (cfg.current_context) {
      const current = credentialsFromContext(cfg, cfg.current_context);
      if (current) return current;
    }
    return { server: cfg.server?.trim() || undefined };
  } catch (err) {
    if (typeof err === "object" && err && "code" in err && err.code === "ENOENT") {
      return undefined;
    }
    console.warn(`drive9: failed to load config from ${cfgPath}: ${err}`);
    return undefined;
  }
}

function loadConfig(): { server: string; apiKey?: string } {
  const envServer = process.env.DRIVE9_SERVER?.trim() || process.env.DRIVE9_BASE?.trim() || undefined;
  const envKey = process.env.DRIVE9_API_KEY?.trim() || undefined;
  const file = loadConfigFile();
  return {
    server: envServer || file?.server || DEFAULT_SERVER,
    apiKey: envKey || file?.apiKey,
  };
}

function normalizeWriteOptions(options?: number | WriteOptions): Required<Pick<WriteOptions, "expectedRevision">> & Omit<WriteOptions, "expectedRevision"> {
  if (typeof options === "number") {
    return { expectedRevision: options };
  }
  return { expectedRevision: options?.expectedRevision ?? -1, tags: options?.tags, description: options?.description };
}

function headerEntries(init?: Record<string, string>): [string, string][] {
  return Object.entries(init || {});
}

function appendTagHeaderEntries(headers: [string, string][], tags?: Record<string, string>): void {
  for (const [key, value] of Object.entries(tags || {})) {
    headers.push(["X-Dat9-Tag", `${key}=${value}`]);
  }
}

async function streamToBytes(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const resp = new Response(stream);
  return new Uint8Array(await resp.arrayBuffer());
}

function rawPayload(body: unknown): { body?: BodyInit; contentType?: string } {
  if (body == null) return {};
  if (body instanceof Uint8Array) return { body: bodyInit(body), contentType: "application/octet-stream" };
  if (typeof body === "string") return { body, contentType: "application/json" };
  if (body instanceof ArrayBuffer) return { body, contentType: "application/octet-stream" };
  if (typeof body === "object" && "getReader" in body) return { body: body as BodyInit };
  return { body: JSON.stringify(body), contentType: "application/json" };
}

export class Client {
  readonly baseUrl: string;
  readonly apiKey?: string;
  smallFileThreshold: number;
  private actor = "";
  private statusCache?: TenantStatus;

  constructor(baseUrl?: string, apiKey?: string) {
    const cfg = loadConfig();
    this.baseUrl = (baseUrl ?? cfg.server).replace(/\/$/, "");
    this.apiKey = (apiKey ?? cfg.apiKey) || undefined;
    this.smallFileThreshold = DEFAULT_SMALL_FILE_THRESHOLD;
  }

  static defaultClient(): Client {
    return new Client();
  }

  withSmallFileThreshold(threshold: number): this {
    this.smallFileThreshold = threshold;
    return this;
  }

  setActor(actor: string): void {
    this.actor = actor;
  }

  baseURL(): string {
    return this.baseUrl;
  }

  fsUrl(path: string): string {
    const p = path.startsWith("/") ? path : `/${path}`;
    return `${this.baseUrl}/v1/fs${p}`;
  }

  vaultUrl(path: string): string {
    const p = path.startsWith("/") ? path : `/${path}`;
    return `${this.baseUrl}/v1/vault${p}`;
  }

  authHeaders(init?: Record<string, string>): Record<string, string> {
    const h: Record<string, string> = { ...init };
    if (this.apiKey) {
      h.Authorization = `Bearer ${this.apiKey}`;
    }
    if (this.actor) {
      h["X-Dat9-Actor"] = this.actor;
    }
    return h;
  }

  async warm(): Promise<void> {
    try {
      await this.status();
    } catch {
      // Go SDK Warm is best-effort; keep that contract for startup paths.
    }
  }

  async status(): Promise<TenantStatus> {
    if (this.statusCache) return this.statusCache;
    const resp = await fetch(`${this.baseUrl}/v1/status`, { headers: this.authHeaders() });
    await checkError(resp);
    this.statusCache = (await resp.json()) as TenantStatus;
    if (typeof this.statusCache.inline_threshold === "number" && this.statusCache.inline_threshold > 0) {
      this.smallFileThreshold = this.statusCache.inline_threshold;
    }
    return this.statusCache;
  }

  async maxUploadBytes(): Promise<number> {
    const status = await this.status();
    return typeof status.max_upload_bytes === "number" ? status.max_upload_bytes : 0;
  }

  async smallFileThresholdValue(): Promise<number> {
    const status = await this.status();
    return typeof status.inline_threshold === "number" ? status.inline_threshold : this.smallFileThreshold;
  }

  cachedSmallFileThreshold(): number {
    return this.smallFileThreshold;
  }

  async rawPost(endpoint: string, body?: unknown): Promise<Response> {
    const payload = rawPayload(body);
    const headers = this.authHeaders(payload.contentType ? { "Content-Type": payload.contentType } : undefined);
    const resp = await fetch(`${this.baseUrl}${endpoint}`, { method: "POST", headers, body: payload.body });
    await checkError(resp);
    return resp;
  }

  async rawDelete(endpoint: string, body?: unknown): Promise<Response> {
    const payload = rawPayload(body);
    const headers = this.authHeaders(payload.contentType ? { "Content-Type": payload.contentType } : undefined);
    const resp = await fetch(`${this.baseUrl}${endpoint}`, { method: "DELETE", headers, body: payload.body });
    await checkError(resp);
    return resp;
  }

  async write(path: string, data: Uint8Array, options?: number | WriteOptions): Promise<void> {
    await this.writeWithRevision(path, data, options);
  }

  async writeWithRevision(path: string, data: Uint8Array, options?: number | WriteOptions): Promise<number> {
    const opts = normalizeWriteOptions(options);
    const headers = headerEntries(this.authHeaders({ "Content-Type": "application/octet-stream" }));
    if (opts.expectedRevision >= 0) {
      headers.push(["X-Dat9-Expected-Revision", String(opts.expectedRevision)]);
    }
    if (opts.description) {
      headers.push(["X-Dat9-Description", opts.description]);
    }
    appendTagHeaderEntries(headers, opts.tags);
    const resp = await fetch(this.fsUrl(path), { method: "PUT", headers, body: bodyInit(data) });
    await checkError(resp);
    const body = (await resp.json().catch(() => undefined)) as { revision?: number } | undefined;
    return body?.revision || 0;
  }

  async createFile(path: string): Promise<number> {
    const resp = await fetch(`${this.fsUrl(path)}?create=1`, { method: "POST", headers: this.authHeaders() });
    await checkError(resp);
    const body = (await resp.json().catch(() => undefined)) as { revision?: number } | undefined;
    return body?.revision || 0;
  }

  async symlink(target: string, linkPath: string): Promise<void> {
    const resp = await fetch(`${this.fsUrl(linkPath)}?symlink=1`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ target }),
    });
    await checkError(resp);
  }

  async hardlink(srcPath: string, dstPath: string): Promise<void> {
    const resp = await fetch(`${this.fsUrl(dstPath)}?hardlink=1`, {
      method: "POST",
      headers: this.authHeaders({ "X-Dat9-Hardlink-Source": srcPath }),
    });
    await checkError(resp);
  }

  async read(path: string): Promise<Uint8Array> {
    const resp = await fetch(this.fsUrl(path), { headers: this.authHeaders() });
    await checkError(resp);
    return new Uint8Array(await resp.arrayBuffer());
  }

  async readAt(path: string, offset: number, length: number): Promise<Uint8Array> {
    return streamToBytes(await this.readStreamRange(path, offset, length));
  }

  async append(path: string, data: Uint8Array, options?: WriteOptions): Promise<void> {
    await appendStreamImpl(this, path, data, data.length, options);
  }

  async appendStream(path: string, stream: ReadableStream<Uint8Array> | Uint8Array, size: number, options?: WriteOptions): Promise<void> {
    await appendStreamImpl(this, path, stream, size, options);
  }

  async list(path: string): Promise<FileInfo[]> {
    const resp = await fetch(`${this.fsUrl(path)}?list=1`, { headers: this.authHeaders() });
    await checkError(resp);
    const body = (await resp.json()) as { entries?: Array<{ name: string; size: number; isDir: boolean; mtime?: number }> };
    return (body.entries || []).map((e) => ({
      name: e.name,
      size: e.size,
      isDir: e.isDir,
      mtime: e.mtime != null ? new Date(e.mtime * 1000) : undefined,
    }));
  }

  async batchStat(paths: string[]): Promise<BatchStatResult[]> {
    if (paths.length === 0) return [];
    if (paths.length > MaxBatchStatPaths) {
      throw new Drive9Error(`batch stat: ${paths.length} paths exceeds limit of ${MaxBatchStatPaths}`);
    }
    const resp = await fetch(`${this.baseUrl}/v1/fs:batch-stat`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ paths }),
    });
    await checkError(resp);
    const body = (await resp.json()) as { results?: BatchStatResult[] };
    const results = body.results || [];
    if (results.length !== paths.length) {
      throw new Drive9Error(`batch stat: got ${results.length} results for ${paths.length} paths`);
    }
    return results;
  }

  async batchReadSmall(paths: string[], maxBytes = 0): Promise<BatchReadSmallResult[]> {
    if (paths.length === 0) return [];
    if (paths.length > MaxBatchReadSmallPaths) {
      throw new Drive9Error(`batch read-small: ${paths.length} paths exceeds limit of ${MaxBatchReadSmallPaths}`);
    }
    const resp = await fetch(`${this.baseUrl}/v1/fs:batch-read-small`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ paths, max_bytes: maxBytes }),
    });
    await checkError(resp);
    const body = (await resp.json()) as { results?: Array<Omit<BatchReadSmallResult, "data"> & { data?: string }> };
    const results = body.results || [];
    if (results.length !== paths.length) {
      throw new Drive9Error(`batch read-small: got ${results.length} results for ${paths.length} paths`);
    }
    return results.map((r, i) => {
      if (r.path !== paths[i]) {
        throw new Drive9Error(`batch read-small: result[${i}] path = ${r.path}, want ${paths[i]}`);
      }
      return { ...r, data: r.data ? new Uint8Array(Buffer.from(r.data, "base64")) : undefined };
    });
  }

  async stat(path: string): Promise<StatResult> {
    const resp = await fetch(this.fsUrl(path), { method: "HEAD", headers: this.authHeaders() });
    if (resp.status === 404) {
      throw new Drive9Error(`not found: ${path}`);
    }
    if (!resp.ok) {
      throw new StatusError(`HTTP ${resp.status}`, resp.status);
    }
    const size = Number(resp.headers.get("content-length") || "0");
    const revision = Number(resp.headers.get("x-dat9-revision") || "0");
    const mtimeHeader = resp.headers.get("x-dat9-mtime");
    const modeHeader = resp.headers.get("x-dat9-mode");
    const nlinkHeader = resp.headers.get("x-dat9-nlink");
    return {
      size,
      isDir: resp.headers.get("x-dat9-isdir") === "true",
      revision,
      mtime: mtimeHeader ? new Date(Number(mtimeHeader) * 1000) : undefined,
      mode: modeHeader ? Number(modeHeader) : undefined,
      hasMode: modeHeader != null,
      resource_id: resp.headers.get("x-dat9-resource-id") || undefined,
      nlink: nlinkHeader ? Number(nlinkHeader) : undefined,
    };
  }

  async statMetadata(path: string): Promise<StatMetadataResult> {
    const resp = await fetch(`${this.fsUrl(path)}?stat=1`, { headers: this.authHeaders() });
    await checkError(resp);
    const contentType = resp.headers.get("content-type") || "";
    if (!contentType.toLowerCase().startsWith("application/json")) {
      throw new Drive9Error(`stat metadata fallback: unexpected Content-Type ${contentType}`);
    }
    const body = (await resp.json()) as StatMetadataResult;
    return { ...body, tags: body.tags || {} };
  }

  async statMetadataCompat(path: string): Promise<StatMetadataResult> {
    try {
      return await this.statMetadata(path);
    } catch (err) {
      const status = err instanceof StatusError ? err.statusCode : 0;
      const fallback =
        status === 400 ||
        status === 405 ||
        status === 501 ||
        (err instanceof Error && err.message.includes("stat metadata fallback"));
      if (!fallback) throw err;
      const st = await this.stat(path);
      return {
        size: st.size,
        isdir: st.isDir,
        resource_id: st.resource_id,
        nlink: st.nlink,
        revision: st.revision,
        mtime: st.mtime ? Math.floor(st.mtime.getTime() / 1000) : undefined,
        content_type: "",
        semantic_text: "",
        tags: {},
        degraded: true,
      };
    }
  }

  async delete(path: string): Promise<void> {
    const resp = await fetch(this.fsUrl(path), { method: "DELETE", headers: this.authHeaders() });
    await checkError(resp);
  }

  async deleteFile(path: string): Promise<void> {
    const resp = await fetch(`${this.fsUrl(path)}?kind=file`, { method: "DELETE", headers: this.authHeaders() });
    await checkError(resp);
  }

  async deleteDir(path: string): Promise<void> {
    const resp = await fetch(`${this.fsUrl(path)}?kind=dir`, { method: "DELETE", headers: this.authHeaders() });
    await checkError(resp);
  }

  async removeAll(path: string): Promise<void> {
    const resp = await fetch(`${this.fsUrl(path)}?recursive=1`, { method: "DELETE", headers: this.authHeaders() });
    await checkError(resp);
  }

  async copy(srcPath: string, dstPath: string): Promise<void> {
    const headers = this.authHeaders({ "X-Dat9-Copy-Source": srcPath });
    const resp = await fetch(`${this.fsUrl(dstPath)}?copy`, { method: "POST", headers });
    await checkError(resp);
  }

  async rename(oldPath: string, newPath: string): Promise<void> {
    const headers = this.authHeaders({ "X-Dat9-Rename-Source": oldPath });
    const resp = await fetch(`${this.fsUrl(newPath)}?rename`, { method: "POST", headers });
    await checkError(resp);
  }

  async mkdir(path: string, mode = 0o755): Promise<void> {
    const suffix = mode === 0o755 ? "?mkdir" : `?mkdir&mode=${encodeURIComponent(String(mode))}`;
    const resp = await fetch(`${this.fsUrl(path)}${suffix}`, { method: "POST", headers: this.authHeaders() });
    await checkError(resp);
  }

  async chmod(path: string, mode: number): Promise<void> {
    const resp = await fetch(`${this.fsUrl(path)}?chmod`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ mode }),
    });
    await checkError(resp);
  }

  async sql(query: string): Promise<unknown[]> {
    const resp = await fetch(`${this.baseUrl}/v1/sql`, {
      method: "POST",
      headers: this.authHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ query }),
    });
    await checkError(resp);
    return (await resp.json()) as unknown[];
  }

  async grep(query: string, pathPrefix: string, limit = 0): Promise<SearchResult[]> {
    return this.grepWithLayer(query, pathPrefix, limit, "");
  }

  async grepWithLayer(query: string, pathPrefix: string, limit = 0, layerRef = ""): Promise<SearchResult[]> {
    let url = `${this.fsUrl(pathPrefix)}?grep=${encodeURIComponent(query)}`;
    if (limit > 0) url += `&limit=${limit}`;
    if (layerRef.trim()) url += `&layer=${encodeURIComponent(layerRef.trim())}`;
    const resp = await fetch(url, { headers: this.authHeaders() });
    await checkError(resp);
    return (await resp.json()) as SearchResult[];
  }

  async find(pathPrefix: string, params: Record<string, string> = {}): Promise<SearchResult[]> {
    const qs = new URLSearchParams({ find: "", ...params });
    const resp = await fetch(`${this.fsUrl(pathPrefix)}?${qs.toString()}`, { headers: this.authHeaders() });
    await checkError(resp);
    return (await resp.json()) as SearchResult[];
  }

  async writeStream(
    path: string,
    stream: ReadableStream<Uint8Array> | Uint8Array,
    size: number,
    options?: number | WriteOptions
  ): Promise<void> {
    return writeStreamImpl(this, path, stream, size, options);
  }

  async writeStreamWithSummary(
    path: string,
    stream: ReadableStream<Uint8Array> | Uint8Array,
    size: number,
    options?: number | WriteOptions
  ): Promise<UploadSummary> {
    return writeStreamWithSummaryImpl(this, path, stream, size, options);
  }

  async readStream(path: string): Promise<ReadableStream<Uint8Array>> {
    return readStreamImpl(this, path);
  }

  async readStreamRange(path: string, offset: number, length: number): Promise<ReadableStream<Uint8Array>> {
    return readStreamRangeImpl(this, path, offset, length);
  }

  async downloadToFile(remotePath: string, localPath: string): Promise<void> {
    const stream = await this.readStream(remotePath);
    const reader = stream.getReader();
    await new Promise<void>((resolve, reject) => {
      const out = fs.createWriteStream(localPath);
      out.on("error", reject);
      out.on("finish", resolve);
      void (async () => {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (!out.write(Buffer.from(value))) {
              await new Promise((drain) => out.once("drain", drain));
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

  /**
   * Download an entire remote directory tree to a local directory.
   *
   * The local directory is created if it does not exist; if it exists it
   * must be a directory. Any pre-existing descendant path is treated as
   * a conflict and aborts the download before any file is written, so
   * downloadDir never overwrites or truncates existing local files.
   * Symlinks in the remote tree are rejected.
   *
   * Mirrors the Go SDK's Client.DownloadDirCtx and the CLI's
   * `drive9 fs cp -r` remote→local path.
   */
  async downloadDir(remotePath: string, localPath: string): Promise<void> {
    // Source must exist and be a directory.
    const info = await this.stat(remotePath);
    if (!info.isDir) {
      throw new Drive9Error(`downloadDir requires a directory source; ${remotePath} is a file`);
    }

    // Walk the remote tree via list() BFS, collecting relative dirs and
    // files BEFORE touching the local filesystem so a malformed listing
    // or transport error can't leave a partial dst dir.
    const dirs: string[] = []; // relative (slash-separated) dir paths
    const files: { remote: string; rel: string }[] = []; // relative file paths
    const queue: string[] = [""]; // relative paths to expand; "" = root
    while (queue.length > 0) {
      const rel = queue.shift()!;
      const absDir = rel === "" ? remotePath : `${remotePath}/${rel}`;
      const entries = await this.list(absDir);
      for (const e of entries) {
        const childRel = rel === "" ? e.name : `${rel}/${e.name}`;
        // Reject entry names that could escape localPath.
        joinLocalSafe(localPath, childRel);
        if (e.isDir) {
          dirs.push(childRel);
          queue.push(childRel);
        } else {
          files.push({ remote: `${remotePath}/${childRel}`, rel: childRel });
        }
      }
    }

    // Pre-resolve every local destination path using joinLocalSafe so
    // a malicious/malformed remote name (e.g. "../escape.txt") cannot
    // escape localPath.
    const dstFiles = files.map((f) => ({
      remote: f.remote,
      local: joinLocalSafe(localPath, f.rel),
    }));
    const dstDirs = dirs.map((d) => joinLocalSafe(localPath, d));

    // Destination root preflight.
    let dstExists = false;
    try {
      const dstInfo = fs.statSync(localPath);
      dstExists = true;
      if (!dstInfo.isDirectory()) {
        throw new Drive9Error(`local destination ${localPath} exists and is not a directory`);
      }
    } catch (err: unknown) {
      if (!isNodeError(err, "ENOENT")) throw err;
    }

    // Descendant preflight: any pre-existing dir/file under localPath
    // is a conflict. Uses lstatSync (not statSync) so a pre-existing
    // symlink also counts as a conflict — we don't want to follow a
    // symlink into someone else's directory.
    preflightLocalDestinations([...dstDirs, ...dstFiles.map((f) => f.local)]);

    // Create dst root if needed.
    if (!dstExists) {
      fs.mkdirSync(localPath, { recursive: true });
    }

    // Create descendant dirs in parent-before-child order (sorted by
    // path length). Empty dirs are preserved.
    dstDirs.sort((a, b) => a.length - b.length);
    for (const d of dstDirs) {
      fs.mkdirSync(d, { recursive: true });
    }

    // Bounded-parallel download. Sibling failures are collected; the
    // first error is re-thrown after all in-flight downloads settle.
    const sem = new Semaphore(DOWNLOAD_DIR_CONCURRENCY);
    const errors: unknown[] = [];
    await Promise.all(
      dstFiles.map(async (f) => {
        await sem.acquire();
        try {
          await this.downloadToFile(f.remote, f.local);
        } catch (err) {
          errors.push(err);
        } finally {
          sem.release();
        }
      })
    );
    if (errors.length > 0) {
      throw errors[0];
    }
  }

  async resumeUpload(path: string, data: Uint8Array): Promise<void> {
    return resumeUploadImpl(this, path, data);
  }

  async patchFile(
    path: string,
    newSize: number,
    dirtyParts: number[],
    readPart: ReadPartFn,
    progress?: ProgressFn,
    partSize?: number
  ): Promise<void> {
    return patchFileImpl(this, path, newSize, dirtyParts, readPart, progress, partSize);
  }

  newStreamWriter(path: string, totalSize: number, options: number | WriteOptions = -1, abortOnError = true): StreamWriter {
    return new StreamWriter(this, path, totalSize, options, abortOnError);
  }

  async issueScopedToken(req: IssueScopedTokenRequest): Promise<IssueScopedTokenResponse> {
    return issueScopedToken(this, req);
  }

  async revokeScopedToken(tokenId: string): Promise<void> {
    return revokeScopedToken(this, tokenId);
  }

  async revokeScopedTokenByAPIKey(apiKey: string): Promise<void> {
    return revokeScopedTokenByAPIKey(this, apiKey);
  }

  async createVaultSecret(name: string, fields: Record<string, unknown>): Promise<import("./models.js").VaultSecret> {
    return createVaultSecret(this, name, fields);
  }

  async updateVaultSecret(name: string, fields: Record<string, unknown>): Promise<import("./models.js").VaultSecret> {
    return updateVaultSecret(this, name, fields);
  }

  async deleteVaultSecret(name: string): Promise<void> {
    return deleteVaultSecret(this, name);
  }

  async listVaultSecrets(): Promise<import("./models.js").VaultSecret[]> {
    return listVaultSecrets(this);
  }

  async issueVaultToken(agentId: string, taskId: string, scope: string[], ttlSeconds: number): Promise<import("./models.js").VaultTokenIssueResponse> {
    return issueVaultToken(this, agentId, taskId, scope, ttlSeconds);
  }

  async revokeVaultToken(tokenId: string): Promise<void> {
    return revokeVaultToken(this, tokenId);
  }

  async issueVaultGrant(req: VaultGrantIssueRequest): Promise<VaultGrantIssueResponse> {
    return issueVaultGrant(this, req);
  }

  async revokeVaultGrant(grantId: string, req: VaultGrantRevokeRequest = {}): Promise<void> {
    return revokeVaultGrant(this, grantId, req);
  }

  async queryVaultAudit(secretName?: string, limit?: number): Promise<import("./models.js").VaultAuditEvent[]> {
    return queryVaultAudit(this, secretName, limit);
  }

  async listReadableVaultSecrets(): Promise<string[]> {
    return listReadableVaultSecrets(this);
  }

  async readVaultSecret(name: string): Promise<Record<string, unknown>> {
    return readVaultSecret(this, name);
  }

  async readVaultSecretField(name: string, field: string): Promise<string> {
    return readVaultSecretField(this, name, field);
  }

  async readVaultSecretAsOwner(name: string): Promise<Record<string, unknown>> {
    return readVaultSecretAsOwner(this, name);
  }

  async readVaultSecretFieldAsOwner(name: string, field: string): Promise<string> {
    return readVaultSecretFieldAsOwner(this, name, field);
  }

  async watchEvents(actor: string, handler: EventHandler, options: WatchEventsOptions = {}): Promise<void> {
    return watchEvents(this, actor, handler, options);
  }

  async watchEventsWithLifecycle(
    actor: string,
    handler: EventHandler,
    lifecycle: EventLifecycle = {},
    options: WatchEventsOptions = {}
  ): Promise<void> {
    return watchEventsWithLifecycle(this, actor, handler, lifecycle, options);
  }

  async createFSLayer(req: FSLayerCreateRequest): Promise<FSLayer> {
    return createFSLayer(this, req);
  }

  async listFSLayers(): Promise<FSLayer[]> {
    return listFSLayers(this);
  }

  async getFSLayer(layerId: string): Promise<FSLayer> {
    return getFSLayer(this, layerId);
  }

  async diffFSLayer(layerId: string, maxSeq?: number): Promise<FSLayerEntry[]> {
    return diffFSLayer(this, layerId, maxSeq, false);
  }

  async replayFSLayer(layerId: string, maxSeq?: number): Promise<FSLayerEntry[]> {
    return diffFSLayer(this, layerId, maxSeq, true);
  }

  async upsertFSLayerEntry(layerId: string, req: FSLayerEntryRequest): Promise<FSLayerEntry> {
    return upsertFSLayerEntry(this, layerId, req);
  }

  async uploadFSLayerFile(layerId: string, path: string, data: Uint8Array, opts: { baseRevision?: number; mode?: number } = {}): Promise<FSLayerEntry> {
    return uploadFSLayerFile(this, layerId, path, data, opts);
  }

  async readFSLayerFile(layerId: string, path: string, maxSeq?: number): Promise<Uint8Array> {
    return readFSLayerFile(this, layerId, path, maxSeq);
  }

  async readFSLayerFileStream(layerId: string, path: string, maxSeq?: number): Promise<ReadableStream<Uint8Array>> {
    return readFSLayerFileStream(this, layerId, path, maxSeq);
  }

  async getFSLayerEntry(layerId: string, path: string, maxSeq?: number): Promise<FSLayerEntry> {
    return getFSLayerEntry(this, layerId, path, maxSeq);
  }

  async checkpointFSLayer(layerId: string, req: FSLayerCheckpointRequest): Promise<FSLayerCheckpoint> {
    return checkpointFSLayer(this, layerId, req);
  }

  async getFSLayerCheckpoint(checkpointId: string): Promise<FSLayerCheckpoint> {
    return getFSLayerCheckpoint(this, checkpointId);
  }

  async listFSLayerEvents(layerId: string, since = 0): Promise<FSLayerEvent[]> {
    return listFSLayerEvents(this, layerId, since);
  }

  async rollbackFSLayer(layerId: string): Promise<void> {
    return rollbackFSLayer(this, layerId);
  }

  async commitFSLayer(layerId: string): Promise<FSLayerCommit> {
    return commitFSLayer(this, layerId);
  }

  async upsertGitWorkspace(req: GitWorkspaceRequest): Promise<GitWorkspace> {
    return upsertGitWorkspace(this, req);
  }

  async getGitWorkspaceByRoot(rootPath: string): Promise<GitWorkspace> {
    return getGitWorkspaceByRoot(this, rootPath);
  }

  async getGitWorkspace(workspaceId: string): Promise<GitWorkspace> {
    return getGitWorkspace(this, workspaceId);
  }

  async deleteGitWorkspace(workspaceId: string): Promise<void> {
    return deleteGitWorkspace(this, workspaceId);
  }

  async listGitWorkspaces(): Promise<GitWorkspace[]> {
    return listGitWorkspaces(this);
  }

  async replaceGitTree(workspaceId: string, req: GitTreeReplaceRequest): Promise<void> {
    return replaceGitTree(this, workspaceId, req);
  }

  async listGitTree(workspaceId: string, commitSHA: string): Promise<GitTreeNode[]> {
    return listGitTree(this, workspaceId, commitSHA);
  }

  async upsertGitState(workspaceId: string, req: GitStateRequest): Promise<GitState> {
    return upsertGitState(this, workspaceId, req);
  }

  async getGitState(workspaceId: string): Promise<GitState> {
    return getGitState(this, workspaceId);
  }

  async putGitObjectPack(workspaceId: string, req: GitObjectPackRequest): Promise<GitObjectPack> {
    return putGitObjectPack(this, workspaceId, req);
  }

  async listGitObjectPacks(workspaceId: string): Promise<GitObjectPack[]> {
    return listGitObjectPacks(this, workspaceId);
  }

  async getGitObjectPack(workspaceId: string, packId: string): Promise<GitObjectPack> {
    return getGitObjectPack(this, workspaceId, packId);
  }

  async putGitOverlayEntry(workspaceId: string, req: GitOverlayEntryRequest): Promise<GitOverlayEntry> {
    return putGitOverlayEntry(this, workspaceId, req);
  }

  async getGitOverlayEntry(workspaceId: string, relPath: string): Promise<GitOverlayEntry> {
    return getGitOverlayEntry(this, workspaceId, relPath);
  }

  async listGitOverlayEntries(workspaceId: string): Promise<GitOverlayEntry[]> {
    return listGitOverlayEntries(this, workspaceId);
  }

  async createJournal(req: JournalCreateRequest): Promise<Journal> {
    return createJournal(this, req);
  }

  async appendJournalEntries(journalId: string, appendId: string, entries: JournalEntryInput[]): Promise<JournalAppendResponse> {
    return appendJournalEntries(this, journalId, appendId, entries);
  }

  async readJournalEntries(journalId: string, afterSeq = 0, limit = 0): Promise<JournalEntry[]> {
    return readJournalEntries(this, journalId, afterSeq, limit);
  }

  async searchJournal(req: JournalSearchRequest): Promise<JournalSearchMatch[]> {
    return searchJournal(this, req);
  }

  async verifyJournal(journalId: string): Promise<JournalVerifyResult> {
    return verifyJournal(this, journalId);
  }
}

// joinLocalSafe joins a local base with a slash-separated relative
// segment, rejecting anything that would escape the base (`..`,
// absolute segments). This is needed because `rel` originates from
// server-supplied directory listings: a misbehaving or compromised
// server could return entry names like "../etc/passwd" and a naive
// path.join would write outside localPath.
function joinLocalSafe(base: string, rel: string): string {
  if (rel === "" || rel === ".") return base;
  if (rel.startsWith("/")) {
    throw new Drive9Error(`relative segment must not start with /: ${rel}`);
  }
  for (const seg of rel.split("/")) {
    if (seg === "..") {
      throw new Drive9Error(`relative segment must not contain ..: ${rel}`);
    }
  }
  const joined = path.join(base, rel);
  const cleanBase = path.resolve(base);
  if (cleanBase !== "" && !joined.startsWith(cleanBase + path.sep) && joined !== cleanBase) {
    throw new Drive9Error(`computed path ${joined} escapes base ${base}`);
  }
  return joined;
}

// preflightLocalDestinations checks every local destination path with
// lstatSync. Any path that exists aborts the download before any
// mkdir/download.
function preflightLocalDestinations(paths: string[]): void {
  const seen = new Set<string>();
  for (const p of paths) {
    if (seen.has(p)) continue;
    seen.add(p);
    try {
      fs.lstatSync(p);
      throw new Drive9Error(`local destination ${p} already exists; downloadDir refuses to overwrite`);
    } catch (err: unknown) {
      if (!isNodeError(err, "ENOENT")) throw err;
    }
  }
}

// isNodeError reports whether `err` is a Node.js filesystem error with
// the given code (e.g. "ENOENT"). Used to distinguish "path does not
// exist" from other stat/lstat failures.
function isNodeError(err: unknown, code: string): boolean {
  return err instanceof Error && (err as NodeJS.ErrnoException).code === code;
}
