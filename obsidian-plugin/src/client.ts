import { requestUrl, RequestUrlParam, Platform } from "obsidian";
import type { StatResult, FileInfo, ProgressFn, SearchResult } from "./types";

/** Files >= this size use multipart upload (matches server threshold). */
const MULTIPART_THRESHOLD = 50_000; // 50 KB

/**
 * Drive9Client wraps the drive9 REST API using Obsidian's requestUrl
 * (bypasses CORS, works on mobile).
 */
export class Drive9Client {
  constructor(
    private serverUrl: string,
    private apiKey: string,
    private actorId = "",
  ) {}

  updateConfig(serverUrl: string, apiKey: string): void {
    this.serverUrl = serverUrl;
    this.apiKey = apiKey;
  }

  setActorId(actorId: string): void {
    this.actorId = actorId;
  }

  getServerUrl(): string {
    return this.serverUrl;
  }

  getAPIKey(): string {
    return this.apiKey;
  }

  getActorId(): string {
    return this.actorId;
  }

  /** Test connectivity and auth. Throws on failure. */
  async ping(): Promise<void> {
    await this.list("/");
  }

  /** POST /v1/provision — create a new tenant. No auth required. */
  async provision(): Promise<{ api_key: string; status: string }> {
    const url = `${this.serverUrl}/v1/provision`;
    const resp = await requestUrl({
      url,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      throw: false,
    });
    if (resp.status >= 400) {
      let msg = `HTTP ${resp.status}`;
      try {
        if (typeof resp.json?.error === "string") {
          msg = resp.json.error;
        }
      } catch { /* no body */ }
      throw new Drive9Error(msg, resp.status);
    }
    return resp.json as { api_key: string; status: string };
  }

  /** GET /v1/status — check tenant provisioning status. Requires auth. */
  async getStatus(): Promise<{ status: string }> {
    const resp = await this.request("GET", "/v1/status");
    return resp.json as { status: string };
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
   * Write file content with optional CAS revision check.
   * Files >= MULTIPART_THRESHOLD use v2 multipart upload;
   * smaller files use direct PUT.
   *
   * Returns { revision: number } on full success, or
   * { revision: null, writeSucceeded: true } if write succeeded but
   * post-write stat failed (caller must not treat this as a write failure).
   */
  async write(
    path: string,
    data: ArrayBuffer,
    expectedRevision?: number | null,
    onProgress?: ProgressFn,
  ): Promise<{ revision: number | null; writeSucceeded: boolean }> {
    if (data.byteLength >= MULTIPART_THRESHOLD) {
      try {
        return await this.writeMultipart(path, data, expectedRevision, onProgress);
      } catch (e) {
        if (e instanceof Drive9Error && e.status === 404) {
          // Server doesn't support v2 uploads — fall through to direct PUT.
        } else {
          throw e;
        }
      }
    }

    const headers: Record<string, string> = this.mutationHeaders();
    if (expectedRevision !== undefined && expectedRevision !== null) {
      headers["X-Dat9-Expected-Revision"] = String(expectedRevision);
    }

    await this.request("PUT", `/v1/fs/${encodePath(path)}`, {
      body: data,
      headers,
    });

    try {
      const st = await this.stat(path);
      return { revision: st.revision, writeSucceeded: true };
    } catch {
      return { revision: null, writeSucceeded: true };
    }
  }

  /**
   * v2 multipart upload: initiate → presign-batch → PUT parts → complete.
   * Parts are uploaded sequentially to keep memory bounded.
   */
  private async writeMultipart(
    path: string,
    data: ArrayBuffer,
    expectedRevision?: number | null,
    onProgress?: ProgressFn,
  ): Promise<{ revision: number | null; writeSucceeded: boolean }> {
    // 1. Initiate
    const initBody: Record<string, unknown> = {
      path,
      total_size: data.byteLength,
    };
    if (expectedRevision !== undefined && expectedRevision !== null) {
      initBody.expected_revision = expectedRevision;
    }

    const initResp = await this.request("POST", "/v2/uploads/initiate", {
      body: JSON.stringify(initBody),
      headers: { ...this.mutationHeaders(), "Content-Type": "application/json" },
    });
    const plan = initResp.json as {
      upload_id: string;
      part_size: number;
      total_parts: number;
    };

    const uploadId = plan.upload_id;
    const partSize = plan.part_size;
    const totalParts = plan.total_parts;

    try {
      // 2. Presign all parts in one batch
      const partEntries = Array.from({ length: totalParts }, (_, i) => ({
        part_number: i + 1,
      }));
      const presignResp = await this.request(
        "POST",
        `/v2/uploads/${uploadId}/presign-batch`,
        {
          body: JSON.stringify({ parts: partEntries }),
          headers: { "Content-Type": "application/json" },
        },
      );
      const presigned = (presignResp.json as { parts: PresignedPart[] }).parts;

      // 3. Upload parts sequentially
      const completedParts: Array<{ number: number; etag: string }> = [];

      for (const part of presigned) {
        const offset = (part.number - 1) * partSize;
        const end = Math.min(offset + part.size, data.byteLength);
        const chunk = data.slice(offset, end);

        const etag = await this.uploadOnePart(uploadId, part, chunk);
        completedParts.push({ number: part.number, etag });

        if (onProgress) {
          onProgress(part.number, totalParts);
        }
      }

      // 4. Complete
      await this.request("POST", `/v2/uploads/${uploadId}/complete`, {
        body: JSON.stringify({ parts: completedParts }),
        headers: { "Content-Type": "application/json" },
      });

      // 5. Stat to get revision
      try {
        const st = await this.stat(path);
        return { revision: st.revision, writeSucceeded: true };
      } catch {
        return { revision: null, writeSucceeded: true };
      }
    } catch (e) {
      // Best-effort abort on failure
      try {
        await this.request("POST", `/v2/uploads/${uploadId}/abort`, {
          headers: { "Content-Type": "application/json" },
        });
      } catch {
        // Abort is best-effort.
      }
      throw e;
    }
  }

  /**
   * PUT a single part to its presigned S3 URL.
   * Returns the ETag from the response header.
   */
  private async uploadOnePart(
    uploadId: string,
    part: PresignedPart,
    data: ArrayBuffer,
    isRetry = false,
  ): Promise<string> {
    const headers: Record<string, string> = {};
    if (part.headers) {
      for (const [k, v] of Object.entries(part.headers)) {
        if (k.toLowerCase() !== "host") {
          headers[k] = v;
        }
      }
    }

    const resp = await requestUrl({
      url: part.url,
      method: "PUT",
      body: data,
      headers,
      throw: false,
    });

    if (resp.status === 403 && !isRetry) {
      // Presigned URL expired — re-presign and retry once
      const fresh = await this.presignOnePart(uploadId, part.number);
      return this.uploadOnePart(uploadId, fresh, data, true);
    }

    if (resp.status >= 300) {
      throw new Drive9Error(`part upload failed: HTTP ${resp.status}`, resp.status);
    }

    return resp.headers["etag"] ?? resp.headers["ETag"] ?? "";
  }

  /** Fetch a fresh presigned URL for a single part (retry path). */
  private async presignOnePart(
    uploadId: string,
    partNumber: number,
  ): Promise<PresignedPart> {
    const resp = await this.request(
      "POST",
      `/v2/uploads/${uploadId}/presign`,
      {
        body: JSON.stringify({ part_number: partNumber }),
        headers: { "Content-Type": "application/json" },
      },
    );
    return resp.json as PresignedPart;
  }

  /** DELETE — remove a file. */
  async delete(path: string): Promise<void> {
    await this.request("DELETE", `/v1/fs/${encodePath(path)}`, {
      headers: this.mutationHeaders(),
    });
  }

  /** POST ?rename — rename/move a file. */
  async rename(oldPath: string, newPath: string): Promise<void> {
    await this.request("POST", `/v1/fs/${encodePath(newPath)}?rename`, {
      headers: {
        ...this.mutationHeaders(),
        "X-Dat9-Rename-Source": oldPath,
      },
    });
  }

  /** POST ?mkdir — create a directory. */
  async mkdir(path: string): Promise<void> {
    await this.request("POST", `/v1/fs/${encodePath(path)}?mkdir`, {
      headers: this.mutationHeaders(),
    });
  }

  /** GET ?list=1 — list directory contents. */
  async list(path: string): Promise<FileInfo[]> {
    const resp = await this.request("GET", `/v1/fs/${encodePath(path)}?list=1`);
    const data = resp.json;
    if (Array.isArray(data)) return data as FileInfo[];
    if (data && Array.isArray((data as Record<string, unknown>).entries)) {
      return (data as Record<string, unknown>).entries as FileInfo[];
    }
    return [];
  }

  /** GET ?grep= — hybrid search (FTS + vector + keyword fallback). */
  async grep(query: string, limit = 20): Promise<SearchResult[]> {
    const q = encodeURIComponent(query);
    const resp = await this.request(
      "GET",
      `/v1/fs/?grep=${q}&limit=${limit}`,
    );
    const data = resp.json;
    if (Array.isArray(data)) return data as SearchResult[];
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
    const result = await this.listRecursiveDetailed(basePath);
    return result.entries;
  }

  /**
   * Recursively list all files under a path and report whether the tree scan
   * completed without any subdirectory list failures.
   */
  async listRecursiveDetailed(basePath: string): Promise<{ entries: FileInfo[]; complete: boolean }> {
    const rootEntries = await this.list(basePath);

    const all: FileInfo[] = [];
    let complete = true;
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
            complete = false;
          }
        } else {
          all.push({ ...entry, name: fullPath });
        }
      }
    }

    return { entries: all, complete };
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

  private mutationHeaders(): Record<string, string> {
    return this.actorId ? { "X-Dat9-Actor": this.actorId } : {};
  }
}

export class Drive9Error extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(sanitizeError(message));
    this.name = "Drive9Error";
  }
}

/** Strip auth material (Bearer tokens, API keys) from error strings. */
export function sanitizeError(msg: string): string {
  return msg
    .replace(/Bearer\s+\S+/gi, "Bearer ***")
    .replace(/Authorization:\s*\S+/gi, "Authorization: ***");
}

/** Presigned part URL from the v2 presign-batch endpoint. */
interface PresignedPart {
  number: number;
  url: string;
  size: number;
  headers?: Record<string, string>;
}

/** Encode path segments for URL (don't encode slashes). */
function encodePath(path: string): string {
  return path
    .split("/")
    .map((seg) => encodeURIComponent(seg))
    .join("/");
}
