import { Client } from "./client.js";
import { bodyInit } from "./compat.js";
import { checkError, Drive9Error } from "./error.js";
import type { CompletePart, PresignedPart, UploadPlanV2, WriteOptions } from "./models.js";
import { Semaphore } from "./utils.js";

const UPLOAD_MAX_CONCURRENCY = 16;

interface StreamState {
  started: boolean;
  closing: boolean;
  completed: boolean;
  aborted: boolean;
  plan?: UploadPlanV2;
  uploaded: Map<number, CompletePart>;
  err?: Drive9Error;
}

export class StreamWriter {
  private client: Client;
  private path: string;
  private totalSize: number;
  private expectedRevision: number;
  private description: string;
  private tags?: Record<string, string>;
  private abortOnError: boolean;
  private state: StreamState;
  private sem: Semaphore;
  private inflight: number;
  private inflightZeroResolvers: (() => void)[];

  constructor(
    client: Client,
    path: string,
    totalSize: number,
    expectedRevision: number | WriteOptions = -1,
    abortOnError = true
  ) {
    this.client = client;
    this.path = path;
    this.totalSize = totalSize;
    if (typeof expectedRevision === "number") {
      this.expectedRevision = expectedRevision;
      this.description = "";
    } else {
      this.expectedRevision = expectedRevision.expectedRevision ?? -1;
      this.description = expectedRevision.description || "";
      this.tags = expectedRevision.tags;
    }
    this.abortOnError = abortOnError;
    this.state = {
      started: false,
      closing: false,
      completed: false,
      aborted: false,
      uploaded: new Map(),
    };
    this.sem = new Semaphore(UPLOAD_MAX_CONCURRENCY);
    this.inflight = 0;
    this.inflightZeroResolvers = [];
  }

  async writePart(partNum: number, data: Uint8Array): Promise<void> {
    if (partNum < 1) {
      throw new Drive9Error("part number must be >= 1");
    }
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
    if (this.state.err) {
      throw this.state.err;
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
    if (plan.total_parts && partNum > plan.total_parts) {
      throw new Drive9Error(
        `part number ${partNum} exceeds total_parts ${plan.total_parts}`
      );
    }
    if (this.state.uploaded.has(partNum)) {
      throw new Drive9Error(`part ${partNum} already uploaded`);
    }

    await this.sem.acquire();
    this.inflight++;
    const client = this.client;
    const uploadId = plan.upload_id;
    try {
      const p = await presignOnePart(client, uploadId, partNum);
      const etag = await uploadOnePartV2(client, uploadId, p, data);
      this.state.uploaded.set(partNum, { number: partNum, etag });
    } catch (e) {
      if (this.abortOnError && !this.state.aborted && !this.state.completed) {
        this.abort().catch(() => {});
      }
      const msg = e instanceof Error ? e.message : String(e);
      this.state.err = new Drive9Error(`upload part ${partNum}: ${msg}`);
      throw this.state.err;
    } finally {
      this.sem.release();
      this.inflight--;
      if (this.inflight === 0) {
        for (const r of this.inflightZeroResolvers) r();
        this.inflightZeroResolvers = [];
      }
    }
  }

  async complete(finalPartNum: number, finalPartData: Uint8Array): Promise<void> {
    if (this.state.completed) {
      throw new Drive9Error("stream writer already completed");
    }
    if (this.state.aborted) {
      throw new Drive9Error("stream writer already aborted");
    }
    if (finalPartData.length > 0) {
      await this.writePart(finalPartNum, finalPartData);
    }

    this.state.closing = true;
    await this.waitInflight();

    if (!this.state.started || !this.state.plan) {
      throw new Drive9Error("stream writer was never started");
    }
    if (this.state.err) {
      throw this.state.err;
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

    await completeUploadV2(this.client, plan.upload_id, parts, this.tags);
    this.state.completed = true;
  }

  async abort(): Promise<void> {
    if (this.state.completed || this.state.aborted) return;
    this.state.aborted = true;
    await this.waitInflight();
    if (this.state.plan) {
      await abortUploadV2(this.client, this.state.plan.upload_id);
    }
  }

  private async waitInflight(): Promise<void> {
    if (this.inflight === 0) return;
    await new Promise<void>((resolve) => this.inflightZeroResolvers.push(resolve));
  }

  private async initiate(): Promise<UploadPlanV2> {
    const payload: { path: string; total_size: number; expected_revision?: number; description?: string } = {
      path: this.path,
      total_size: this.totalSize,
    };
    if (this.expectedRevision >= 0) payload.expected_revision = this.expectedRevision;
    if (this.description) payload.description = this.description;
    const resp = await fetch(`${this.client.baseUrl}/v2/uploads/initiate`, {
      method: "POST",
      headers: this.client["authHeaders"]({ "Content-Type": "application/json" }),
      body: JSON.stringify(payload),
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
  const headers = presignedHeaders(part);
  let resp = await fetch(part.url, { method: "PUT", headers, body: bodyInit(data) });
  if (resp.status === 403) {
    const fresh = await presignOnePart(client, uploadId, part.number);
    resp = await fetch(fresh.url, { method: "PUT", headers: presignedHeaders(fresh), body: bodyInit(data) });
  }
  await checkError(resp);
  return resp.headers.get("etag") || "";
}

function presignedHeaders(part: PresignedPart): Record<string, string> {
  const headers: Record<string, string> = {};
  if (part.headers) {
    for (const [k, v] of Object.entries(part.headers)) {
      if (typeof v === "string") headers[k] = v;
    }
  }
  return headers;
}

async function completeUploadV2(client: Client, uploadId: string, parts: CompletePart[], tags?: Record<string, string>): Promise<void> {
  const resp = await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/complete`, {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ parts, tags }),
  });
  await checkError(resp);
}

async function abortUploadV2(client: Client, uploadId: string): Promise<void> {
  await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/abort`, {
    method: "POST",
    headers: client["authHeaders"](),
  });
}
