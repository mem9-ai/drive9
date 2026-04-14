import { Client } from "./client.js";
import { bodyInit } from "./compat.js";
import { checkError, Drive9Error } from "./error.js";
import type { CompletePart, PresignedPart, UploadPlanV2 } from "./models.js";

const UPLOAD_MAX_CONCURRENCY = 16;

interface StreamState {
  started: boolean;
  closing: boolean;
  completed: boolean;
  aborted: boolean;
  plan?: UploadPlanV2;
  uploaded: Map<number, CompletePart>;
}

export class StreamWriter {
  private client: Client;
  private path: string;
  private totalSize: number;
  private expectedRevision: number;
  private state: StreamState;
  private sem: Semaphore;

  constructor(client: Client, path: string, totalSize: number, expectedRevision = -1) {
    this.client = client;
    this.path = path;
    this.totalSize = totalSize;
    this.expectedRevision = expectedRevision;
    this.state = {
      started: false,
      closing: false,
      completed: false,
      aborted: false,
      uploaded: new Map(),
    };
    this.sem = new Semaphore(UPLOAD_MAX_CONCURRENCY);
  }

  async writePart(partNum: number, data: Uint8Array): Promise<void> {
    if (!this.state.started) {
      try {
        const plan = await this.initiate();
        this.state.plan = plan;
        this.state.started = true;
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes("v2 upload API not available")) {
          throw new Drive9Error("streaming upload requires v2 protocol: v2 upload API not available");
        }
        throw new Drive9Error(`initiate stream upload: ${msg}`);
      }
    }
    if (this.state.completed) {
      throw new Drive9Error("stream writer already completed");
    }
    if (this.state.aborted) {
      throw new Drive9Error("stream writer already aborted");
    }
    if (this.state.closing) {
      throw new Drive9Error("stream writer is closing");
    }

    const plan = this.state.plan!;
    await this.sem.acquire();
    const client = this.client;
    const uploadId = plan.upload_id;
    const p = await presignOnePart(client, uploadId, partNum);
    try {
      const etag = await uploadOnePartV2(client, uploadId, p, data);
      this.state.uploaded.set(partNum, { number: partNum, etag });
    } finally {
      this.sem.release();
    }
  }

  async complete(finalPartNum: number, finalPartData: Uint8Array): Promise<void> {
    if (this.state.completed) {
      throw new Drive9Error("stream writer already completed");
    }
    if (this.state.aborted) {
      throw new Drive9Error("stream writer already aborted");
    }
    this.state.closing = true;

    if (!this.state.started || !this.state.plan) {
      throw new Drive9Error("stream writer was never started");
    }

    if (finalPartData.length > 0) {
      await this.writePart(finalPartNum, finalPartData);
    }

    const plan = this.state.plan;
    if (this.state.uploaded.size === 0) {
      throw new Drive9Error("no parts uploaded in stream upload");
    }
    const maxPart = Math.max(...this.state.uploaded.keys());
    const parts: CompletePart[] = [];
    for (let i = 1; i <= maxPart; i++) {
      const part = this.state.uploaded.get(i);
      if (!part) {
        throw new Drive9Error(`missing part ${i} in stream upload`);
      }
      parts.push(part);
    }

    await completeUploadV2(this.client, plan.upload_id, parts);
    this.state.completed = true;
  }

  async abort(): Promise<void> {
    if (this.state.completed || this.state.aborted) return;
    this.state.aborted = true;
    if (this.state.plan) {
      await abortUploadV2(this.client, this.state.plan.upload_id);
    }
  }

  private async initiate(): Promise<UploadPlanV2> {
    const resp = await fetch(`${this.client.baseUrl}/v2/uploads/initiate`, {
      method: "POST",
      headers: this.client["authHeaders"]({ "Content-Type": "application/json" }),
      body: JSON.stringify({
        path: this.path,
        size: this.totalSize,
        expected_revision: this.expectedRevision,
      }),
    });
    if (resp.status === 404) {
      throw new Drive9Error("v2 upload API not available");
    }
    await checkError(resp);
    return (await resp.json()) as UploadPlanV2;
  }
}

async function presignOnePart(client: Client, uploadId: string, partNumber: number): Promise<PresignedPart> {
  const resp = await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/presign`, {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ part_number: partNumber }),
  });
  await checkError(resp);
  return (await resp.json()) as PresignedPart;
}

async function uploadOnePartV2(
  client: Client,
  uploadId: string,
  part: PresignedPart,
  data: Uint8Array
): Promise<string> {
  const headers: Record<string, string> = {};
  if (part.headers) {
    for (const [k, v] of Object.entries(part.headers)) {
      if (typeof v === "string") headers[k] = v;
    }
  }
  let resp = await fetch(part.url, { method: "PUT", headers, body: bodyInit(data) });
  if (resp.status === 403) {
    const fresh = await presignOnePart(client, uploadId, part.number);
    resp = await fetch(fresh.url, { method: "PUT", headers, body: bodyInit(data) });
  }
  await checkError(resp);
  return resp.headers.get("etag") || "";
}

async function completeUploadV2(client: Client, uploadId: string, parts: CompletePart[]): Promise<void> {
  const resp = await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/complete`, {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ parts }),
  });
  await checkError(resp);
}

async function abortUploadV2(client: Client, uploadId: string): Promise<void> {
  await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/abort`, {
    method: "POST",
    headers: client["authHeaders"](),
  });
}

class Semaphore {
  private permits: number;
  private waiters: (() => void)[] = [];
  constructor(n: number) {
    this.permits = n;
  }
  acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }
  release(): void {
    const next = this.waiters.shift();
    if (next) {
      next();
    } else {
      this.permits++;
    }
  }
}
