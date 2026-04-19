import * as fs from "fs";
import * as path from "path";
import { bodyInit } from "./compat.js";
import { checkError, ConflictError, Drive9Error, StatusError } from "./error.js";
import type { FileInfo, SearchResult, StatResult } from "./models.js";
import { patchFileImpl, type ProgressFn, type ReadPartFn } from "./patch.js";
import { StreamWriter } from "./stream.js";
import { readStreamImpl, readStreamRangeImpl, resumeUploadImpl, writeStreamImpl } from "./transfer.js";
import {
  createVaultSecret,
  deleteVaultSecret,
  issueVaultToken,
  listReadableVaultSecrets,
  listVaultSecrets,
  queryVaultAudit,
  readVaultSecret,
  readVaultSecretField,
  revokeVaultToken,
  updateVaultSecret,
} from "./vault.js";

const DEFAULT_SMALL_FILE_THRESHOLD = 50_000;

function loadConfigFile(): { server: string; apiKey?: string } | undefined {
  const home = process.env.HOME || process.env.USERPROFILE || "";
  const cfgPath = path.join(home, ".drive9", "config");
  try {
    const data = fs.readFileSync(cfgPath, "utf-8");
    const cfg = JSON.parse(data) as {
      server?: string;
      current_context?: string;
      contexts?: Record<string, { api_key?: string }>;
    };
    const server = cfg.server || "https://api.drive9.ai";
    let apiKey: string | undefined;
    if (cfg.current_context && cfg.contexts) {
      apiKey = cfg.contexts[cfg.current_context]?.api_key;
    }
    return { server, apiKey };
  } catch (err) {
    if (typeof err === "object" && err && "code" in err && err.code === "ENOENT") {
      return undefined;
    }
    console.warn(`drive9: failed to load config from ${cfgPath}: ${err}`);
    return undefined;
  }
}

function loadConfig(): { server: string; apiKey?: string } {
  const envServer = process.env.DRIVE9_SERVER?.trim() || undefined;
  const envKey = process.env.DRIVE9_API_KEY?.trim() || undefined;
  const file = loadConfigFile();
  return {
    server: envServer || file?.server || "https://api.drive9.ai",
    apiKey: envKey || file?.apiKey,
  };
}

export class Client {
  readonly baseUrl: string;
  readonly apiKey?: string;
  smallFileThreshold: number;

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
      h["Authorization"] = `Bearer ${this.apiKey}`;
    }
    return h;
  }

  async write(path: string, data: Uint8Array, expectedRevision = -1): Promise<void> {
    const headers = this.authHeaders({ "Content-Type": "application/octet-stream" });
    if (expectedRevision >= 0) {
      headers["X-Dat9-Expected-Revision"] = String(expectedRevision);
    }
    const resp = await fetch(this.fsUrl(path), { method: "PUT", headers, body: bodyInit(data) });
    await checkError(resp);
  }

  async read(path: string): Promise<Uint8Array> {
    const resp = await fetch(this.fsUrl(path), { headers: this.authHeaders() });
    await checkError(resp);
    return new Uint8Array(await resp.arrayBuffer());
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
    const mtime = mtimeHeader ? new Date(Number(mtimeHeader) * 1000) : undefined;
    return {
      size,
      isDir: resp.headers.get("x-dat9-isdir") === "true",
      revision,
      mtime,
    };
  }

  async delete(path: string): Promise<void> {
    const resp = await fetch(this.fsUrl(path), { method: "DELETE", headers: this.authHeaders() });
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

  async mkdir(path: string): Promise<void> {
    const resp = await fetch(`${this.fsUrl(path)}?mkdir`, { method: "POST", headers: this.authHeaders() });
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
    let url = `${this.fsUrl(pathPrefix)}?grep=${encodeURIComponent(query)}`;
    if (limit > 0) url += `&limit=${limit}`;
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
    expectedRevision?: number
  ): Promise<void> {
    return writeStreamImpl(this, path, stream, size, expectedRevision ?? -1);
  }

  async readStream(path: string): Promise<ReadableStream<Uint8Array>> {
    return readStreamImpl(this, path);
  }

  async readStreamRange(path: string, offset: number, length: number): Promise<ReadableStream<Uint8Array>> {
    return readStreamRangeImpl(this, path, offset, length);
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

  newStreamWriter(path: string, totalSize: number, expectedRevision = -1, abortOnError = true): StreamWriter {
    return new StreamWriter(this, path, totalSize, expectedRevision, abortOnError);
  }

  // Vault methods
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

  async issueVaultToken(
    agent: string,
    scope: string[],
    perm: string,
    ttlSeconds: number,
    labelHint?: string,
  ): Promise<import("./models.js").VaultTokenIssueResponse> {
    return issueVaultToken(this, agent, scope, perm, ttlSeconds, labelHint);
  }

  async revokeVaultToken(grantId: string): Promise<void> {
    return revokeVaultToken(this, grantId);
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
}
