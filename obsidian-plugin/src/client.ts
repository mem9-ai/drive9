import { requestUrl, RequestUrlParam } from "obsidian";
import type { FileInfo, StatResult } from "./types";

export class Drive9Client {
  private baseUrl: string;
  private apiKey: string;

  constructor(serverUrl: string, apiKey: string) {
    this.baseUrl = serverUrl.replace(/\/+$/, "");
    this.apiKey = apiKey;
  }

  private headers(extra?: Record<string, string>): Record<string, string> {
    const h: Record<string, string> = {};
    if (this.apiKey) {
      h["Authorization"] = `Bearer ${this.apiKey}`;
    }
    if (extra) {
      Object.assign(h, extra);
    }
    return h;
  }

  private fsUrl(path: string, query?: string): string {
    const p = path.startsWith("/") ? path : "/" + path;
    const base = `${this.baseUrl}/v1/fs${encodeURI(p)}`;
    return query ? `${base}?${query}` : base;
  }

  async stat(path: string): Promise<StatResult | null> {
    try {
      const resp = await requestUrl({
        url: this.fsUrl(path),
        method: "HEAD",
        headers: this.headers(),
        throw: false,
      });
      if (resp.status === 404) return null;
      if (resp.status >= 400) {
        throw new Error(`stat ${path}: HTTP ${resp.status}`);
      }
      return {
        size: parseInt(resp.headers["content-length"] || "0", 10),
        isDir: resp.headers["x-dat9-isdir"] === "true",
        revision: parseInt(resp.headers["x-dat9-revision"] || "0", 10),
        mtime: parseInt(resp.headers["x-dat9-mtime"] || "0", 10),
      };
    } catch (err) {
      if (err instanceof Error && err.message.includes("404")) return null;
      throw err;
    }
  }

  async read(path: string): Promise<ArrayBuffer> {
    const resp = await requestUrl({
      url: this.fsUrl(path),
      method: "GET",
      headers: this.headers(),
    });
    return resp.arrayBuffer;
  }

  async write(
    path: string,
    data: ArrayBuffer,
    expectedRevision?: number,
  ): Promise<void> {
    const extra: Record<string, string> = {
      "Content-Type": "application/octet-stream",
    };
    if (expectedRevision !== undefined && expectedRevision > 0) {
      extra["X-Dat9-Expected-Revision"] = String(expectedRevision);
    }
    const resp = await requestUrl({
      url: this.fsUrl(path),
      method: "PUT",
      headers: this.headers(extra),
      body: data,
      throw: false,
    });
    if (resp.status === 409) {
      throw new RevisionConflictError(path, expectedRevision ?? 0);
    }
    if (resp.status >= 400) {
      const body = tryParseJson(resp.text);
      throw new Error(
        `write ${path}: HTTP ${resp.status} — ${body?.error || resp.text}`,
      );
    }
  }

  async delete(path: string): Promise<void> {
    const resp = await requestUrl({
      url: this.fsUrl(path),
      method: "DELETE",
      headers: this.headers(),
      throw: false,
    });
    if (resp.status === 404) return; // already gone
    if (resp.status >= 400) {
      throw new Error(`delete ${path}: HTTP ${resp.status}`);
    }
  }

  async rename(oldPath: string, newPath: string): Promise<void> {
    const resp = await requestUrl({
      url: this.fsUrl(newPath, "rename"),
      method: "POST",
      headers: this.headers({ "X-Dat9-Rename-Source": oldPath }),
      throw: false,
    });
    if (resp.status >= 400) {
      throw new Error(`rename ${oldPath} → ${newPath}: HTTP ${resp.status}`);
    }
  }

  async mkdir(path: string): Promise<void> {
    const resp = await requestUrl({
      url: this.fsUrl(path, "mkdir"),
      method: "POST",
      headers: this.headers(),
      throw: false,
    });
    if (resp.status >= 400) {
      throw new Error(`mkdir ${path}: HTTP ${resp.status}`);
    }
  }

  async list(path: string): Promise<FileInfo[]> {
    const resp = await requestUrl({
      url: this.fsUrl(path, "list=1"),
      method: "GET",
      headers: this.headers(),
      throw: false,
    });
    if (resp.status === 404) return [];
    if (resp.status >= 400) {
      throw new Error(`list ${path}: HTTP ${resp.status}`);
    }
    const data = resp.json;
    return (data.entries || []) as FileInfo[];
  }

  async listRecursive(basePath: string): Promise<Map<string, FileInfo>> {
    const result = new Map<string, FileInfo>();
    const queue = [basePath];

    while (queue.length > 0) {
      const dir = queue.shift()!;
      const entries = await this.list(dir);
      for (const entry of entries) {
        const fullPath =
          dir === "/" ? `/${entry.name}` : `${dir}/${entry.name}`;
        if (entry.isDir) {
          queue.push(fullPath);
        } else {
          result.set(fullPath, entry);
        }
      }
    }
    return result;
  }

  async ping(): Promise<boolean> {
    try {
      const resp = await requestUrl({
        url: `${this.baseUrl}/healthz`,
        method: "GET",
        throw: false,
      });
      return resp.status === 200;
    } catch {
      return false;
    }
  }
}

export class RevisionConflictError extends Error {
  constructor(
    public path: string,
    public expectedRevision: number,
  ) {
    super(`Revision conflict on ${path} (expected ${expectedRevision})`);
    this.name = "RevisionConflictError";
  }
}

function tryParseJson(text: string): { error?: string } | null {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}
