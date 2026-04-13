import { requestUrl, RequestUrlParam } from "obsidian";
import type { StatResult, FileInfo } from "./types";

/**
 * Drive9Client wraps the drive9 REST API using Obsidian's requestUrl
 * (bypasses CORS, works on mobile).
 */
export class Drive9Client {
  private actorId = "";

  constructor(
    private serverUrl: string,
    private apiKey: string,
  ) {}

  updateConfig(serverUrl: string, apiKey: string): void {
    this.serverUrl = serverUrl;
    this.apiKey = apiKey;
  }

  setActorId(actorId: string): void {
    this.actorId = actorId;
  }

  /** Test connectivity and auth. Throws on failure. */
  async ping(): Promise<void> {
    await this.list("/");
  }

  /** HEAD — get file/dir metadata including revision. */
  async stat(path: string): Promise<StatResult> {
    const resp = await this.request("HEAD", `/v1/fs/${encodePath(path)}`);
    return {
      size: parseInt(resp.headers["content-length"] ?? "0", 10),
      isDir: resp.headers["x-dat9-isdir"] === "true",
      revision: parseInt(resp.headers["x-dat9-revision"] ?? "0", 10),
      mtime: parseInt(resp.headers["x-dat9-mtime"] ?? "0", 10),
    };
  }

  /** GET — read file content. */
  async read(path: string): Promise<ArrayBuffer> {
    const resp = await this.request("GET", `/v1/fs/${encodePath(path)}`);
    return resp.arrayBuffer;
  }

  /**
   * PUT — write file content with optional CAS revision check.
   * Server returns {"status":"ok"} without revision, so we stat()
   * after write to get the actual revision for future CAS.
   *
   * Returns { revision: number } on full success, or
   * { revision: null, writeSucceeded: true } if PUT succeeded but
   * post-write stat failed (caller must not treat this as a write failure).
   */
  async write(
    path: string,
    data: ArrayBuffer,
    expectedRevision?: number | null,
  ): Promise<{ revision: number | null; writeSucceeded: boolean }> {
    const headers: Record<string, string> = {};
    if (expectedRevision !== undefined && expectedRevision !== null) {
      headers["X-Dat9-Expected-Revision"] = String(expectedRevision);
    }
    if (this.actorId) {
      headers["X-Dat9-Actor"] = this.actorId;
    }
    await this.request("PUT", `/v1/fs/${encodePath(path)}`, {
      body: data,
      headers,
    });
    // PUT succeeded. Try to get the new revision for future CAS.
    try {
      const st = await this.stat(path);
      return { revision: st.revision, writeSucceeded: true };
    } catch {
      // Post-write stat failed — write DID succeed on the server.
      // Caller must handle this as "committed but revision unknown".
      return { revision: null, writeSucceeded: true };
    }
  }

  /** DELETE — remove a file. */
  async delete(path: string): Promise<void> {
    const headers: Record<string, string> = {};
    if (this.actorId) headers["X-Dat9-Actor"] = this.actorId;
    await this.request("DELETE", `/v1/fs/${encodePath(path)}`, { headers });
  }

  /** POST ?rename — rename/move a file. */
  async rename(oldPath: string, newPath: string): Promise<void> {
    const headers: Record<string, string> = { "X-Dat9-Rename-Source": oldPath };
    if (this.actorId) headers["X-Dat9-Actor"] = this.actorId;
    await this.request("POST", `/v1/fs/${encodePath(newPath)}?rename`, { headers });
  }

  /** POST ?mkdir — create a directory. */
  async mkdir(path: string): Promise<void> {
    const headers: Record<string, string> = {};
    if (this.actorId) headers["X-Dat9-Actor"] = this.actorId;
    await this.request("POST", `/v1/fs/${encodePath(path)}?mkdir`, { headers });
  }

  /** GET ?list=1 — list directory contents. */
  async list(path: string): Promise<FileInfo[]> {
    const resp = await this.request("GET", `/v1/fs/${encodePath(path)}?list=1`);
    const data = resp.json;
    // API may return { entries: [...] } or just an array
    if (Array.isArray(data)) return data as FileInfo[];
    if (data && Array.isArray((data as Record<string, unknown>).entries)) {
      return (data as Record<string, unknown>).entries as FileInfo[];
    }
    return [];
  }

  /**
   * Recursively list all files under a path.
   * Returns flat list of relative paths (no leading slash).
   *
   * Root list errors are propagated (auth/network failures must not
   * be silently treated as "remote empty"). Only subdirectory list
   * errors are swallowed.
   */
  async listRecursive(basePath: string): Promise<FileInfo[]> {
    // Root list must succeed — propagate errors to caller.
    const rootEntries = await this.list(basePath);

    const all: FileInfo[] = [];
    const queue: Array<{ dir: string; entries: FileInfo[] }> = [
      { dir: basePath, entries: rootEntries },
    ];

    while (queue.length > 0) {
      const { dir, entries } = queue.pop()!;
      for (const entry of entries) {
        const fullPath = dir === "/" || dir === ""
          ? entry.name
          : `${dir}/${entry.name}`;
        if (entry.isDir) {
          try {
            const subEntries = await this.list(fullPath);
            queue.push({ dir: fullPath, entries: subEntries });
          } catch {
            // Subdirectory list failures are non-fatal.
          }
        } else {
          all.push({ ...entry, name: fullPath });
        }
      }
    }

    return all;
  }

  private async request(
    method: string,
    urlPath: string,
    opts?: { body?: string | ArrayBuffer; headers?: Record<string, string> },
  ) {
    const url = `${this.serverUrl}${urlPath}`;
    const params: RequestUrlParam = {
      url,
      method,
      headers: {
        ...(this.apiKey ? { Authorization: `Bearer ${this.apiKey}` } : {}),
        ...(opts?.headers ?? {}),
      },
      throw: false,
    };
    if (opts?.body !== undefined) {
      params.body = opts.body;
    }

    const resp = await requestUrl(params);

    if (resp.status >= 400) {
      let msg = `HTTP ${resp.status}`;
      try {
        if (typeof resp.json?.error === "string") {
          msg = resp.json.error;
        }
      } catch {
        // HEAD responses have no body — json access may throw.
      }
      throw new Drive9Error(msg, resp.status);
    }

    return resp;
  }
}

export class Drive9Error extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(message);
    this.name = "Drive9Error";
  }
}

/** Encode path segments for URL (don't encode slashes). */
function encodePath(path: string): string {
  return path
    .split("/")
    .map((seg) => encodeURIComponent(seg))
    .join("/");
}
