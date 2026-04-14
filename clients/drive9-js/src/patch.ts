import { Client } from "./client.js";
import { bodyInit, bufferSource } from "./compat.js";
import { checkError, Drive9Error } from "./error.js";
import type { PatchPartURL, PatchPlan } from "./models.js";

const DEFAULT_PART_SIZE = 8 * 1024 * 1024;

export type ReadPartFn = (partNumber: number, partSize: number, origData?: Uint8Array) => Uint8Array;
export type ProgressFn = (uploaded: number, total: number) => void;

export async function patchFileImpl(
  client: Client,
  path: string,
  newSize: number,
  dirtyParts: number[],
  readPart: ReadPartFn,
  progress?: ProgressFn,
  partSize = DEFAULT_PART_SIZE
): Promise<void> {
  const resp = await fetch(client.fsUrl(path), {
    method: "PATCH",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ new_size: newSize, dirty_parts: dirtyParts, part_size: partSize }),
  });
  await checkError(resp);
  const plan = (await resp.json()) as PatchPlan;
  const total = plan.upload_parts.reduce((sum, p) => sum + p.size, 0);
  let uploaded = 0;

  const parallelism = 16;
  const semaphore = new Semaphore(parallelism);
  const tasks: Promise<void>[] = [];

  for (const part of plan.upload_parts) {
    tasks.push(
      (async () => {
        await semaphore.acquire();
        try {
          await uploadPatchPart(client, part, readPart);
          uploaded += part.size;
          if (progress) progress(uploaded, total);
        } finally {
          semaphore.release();
        }
      })()
    );
  }
  await Promise.all(tasks);
}

async function uploadPatchPart(client: Client, part: PatchPartURL, readPart: ReadPartFn): Promise<void> {
  let origData: Uint8Array | undefined;
  if (part.read_url) {
    const headers: Record<string, string> = {};
    if (part.read_headers) {
      for (const [k, v] of Object.entries(part.read_headers)) {
        if (typeof v === "string") headers[k] = v;
      }
    }
    const resp = await fetch(part.read_url, { headers });
    await checkError(resp);
    origData = new Uint8Array(await resp.arrayBuffer());
  }
  const data = readPart(part.number, part.size, origData);
  const checksum = await sha256Base64(data);
  const headers: Record<string, string> = { "x-amz-checksum-sha256": checksum };
  if (part.headers) {
    for (const [k, v] of Object.entries(part.headers)) {
      if (typeof v === "string" && k.toLowerCase() !== "host") {
        headers[k] = v;
      }
    }
  }
  const resp = await fetch(part.url, { method: "PUT", headers, body: bodyInit(data) });
  await checkError(resp);
}

async function sha256Base64(data: Uint8Array): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", bufferSource(data));
  return Buffer.from(hash).toString("base64");
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
