import { createHash } from "node:crypto";

import type { Client } from "./client.js";
import { bodyInit } from "./compat.js";
import { ConflictError, Drive9Error, StatusError, checkError } from "./error.js";
import type {
  CompletePart,
  ContentLayout,
  PresignedPart,
  StatResult,
  UploadPlanV2,
  WriteOptions,
} from "./models.js";
import { Semaphore } from "./utils.js";

export const ExtentChunkSize = 64 * 1024 * 1024;
export const ExtentBlockSize = 4 * 1024 * 1024;

export interface InternalWriteOptions extends WriteOptions {
  expectedResourceId?: string;
  layout?: ContentLayout;
  targetLayout?: ContentLayout;
}

export interface ExtentReadPlanBlock {
  file_offset: number;
  length: number;
  block_key?: string;
  block_offset: number;
  url?: string;
  zero?: boolean;
}

export interface ExtentReadPlan {
  size: number;
  revision?: number;
  blocks: ExtentReadPlanBlock[];
  next_offset?: number;
}

export interface ExtentDirtyBlock {
  chunk: number;
  off: number;
  size: number;
  checksum: string;
}

export interface ExtentWritePlanBlock {
  chunk: number;
  off: number;
  size: number;
  checksum: string;
  url: string;
  headers?: Record<string, string>;
  exists: boolean;
}

export interface ExtentWritePlan {
  upload_id: string;
  new_size: number;
  expires_at?: string;
  blocks: ExtentWritePlanBlock[];
  checksum_contract?: {
    supported?: string[];
    required?: boolean;
  };
}

export interface ExtentBlockWrite {
  chunk: number;
  off: number;
  data: Uint8Array;
}

export interface CreateExtentFileResult {
  revision: number;
  resource_id?: string;
  native_extent: boolean;
}

const EXTENT_PUT_CONCURRENCY = 16;
const EXTENT_BATCH_PARTS = 32;
const ZERO_EMIT_CHUNK_BYTES = 64 * 1024;

export class ExtentNotServedError extends Drive9Error {
  constructor(message = "extent read plan not served") {
    super(message);
    this.name = "ExtentNotServedError";
  }
}

export class ResourceMismatchError extends Drive9Error {
  constructor(path: string) {
    super(`${path}: path occupant is not the expected resource`);
    this.name = "ResourceMismatchError";
  }
}

export class ExtentPresignUnsupportedError extends Drive9Error {
  constructor(message: string) {
    super(message);
    this.name = "ExtentPresignUnsupportedError";
  }
}

class PresignExpiredError extends Drive9Error {
  constructor(status: number, body: string) {
    super(`extent block range GET: HTTP ${status}: ${body}`);
    this.name = "PresignExpiredError";
  }
}

export function sha256Hex(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

export function sha256Base64(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("base64");
}

export function chunkOffset(fileOffset: number): { chunk: number; off: number } {
  if (!Number.isFinite(fileOffset) || fileOffset < 0) {
    throw new Drive9Error(`invalid file offset ${String(fileOffset)}`);
  }
  return {
    chunk: Math.floor(fileOffset / ExtentChunkSize),
    off: fileOffset % ExtentChunkSize,
  };
}

export function isExtentUploadPlan(plan: UploadPlanV2): boolean {
  return !plan.key && Boolean(plan.checksum_contract?.required);
}

export function isExtentLayoutMismatchError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return msg.toLowerCase().includes("content layout mismatch");
}

export function isExtentPresignUnsupportedError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return msg.toLowerCase().includes("cannot enforce a declared upload length");
}

export function mayBeExtentDirectWriteRejection(err: unknown, expectedRevision: number): boolean {
  if (isExtentLayoutMismatchError(err)) {
    return true;
  }
  if (expectedRevision === 0) {
    return false;
  }
  if (err instanceof StatusError && err.statusCode === 500) {
    const lower = err.message.toLowerCase();
    return lower.includes("internal storage error");
  }
  return false;
}

export function validateRequestedWriteLayout(layout?: ContentLayout): void {
  if (layout === undefined || layout === "" || layout === "single" || layout === "extent") {
    return;
  }
  throw new Drive9Error(`layout must be "single" or "extent", got ${JSON.stringify(layout)}`);
}

function isForbiddenPresignedHeader(key: string): boolean {
  const lower = key.toLowerCase();
  return lower === "host" || lower === "authorization" || lower === "x-dat9-actor";
}

function isExtentBlockConflictStatus(status: number): boolean {
  return status === 403 || status === 409 || status === 412;
}

export async function getExtentReadPlanImpl(
  client: Client,
  path: string,
  start: number,
  end: number
): Promise<ExtentReadPlan> {
  const range = end >= 0 ? `bytes=${start}-${end}` : `bytes=${start}-`;
  const resp = await fetch(client.fsUrl(path), {
    method: "GET",
    headers: client.authHeaders({
      "X-Dat9-Extent-Read-Plan": "1",
      Range: range,
    }),
    redirect: "manual",
  });
  if (resp.status >= 400) {
    await checkError(resp);
  }
  if (resp.headers.get("x-dat9-content-layout") !== "extent") {
    void resp.body?.cancel().catch(() => {});
    throw new ExtentNotServedError();
  }
  if (resp.status >= 300) {
    await checkError(resp);
  }
  return parseExtentReadPlanResponse(resp);
}

export async function parseExtentReadPlanResponse(resp: Response): Promise<ExtentReadPlan> {
  const plan = (await resp.json()) as ExtentReadPlan;
  if (!plan.revision || plan.revision <= 0) {
    const revHeader = resp.headers.get("x-dat9-revision");
    if (revHeader) {
      const parsed = Number(revHeader);
      if (Number.isFinite(parsed) && parsed > 0) {
        plan.revision = parsed;
      }
    }
  }
  return plan;
}

export async function fetchExtentBlockRangeImpl(
  client: Client,
  blockUrl: string,
  offset: number,
  length: number
): Promise<ReadableStream<Uint8Array>> {
  if (!blockUrl) {
    throw new Drive9Error("extent block URL is required");
  }
  if (offset < 0 || length <= 0) {
    throw new Drive9Error(`invalid extent block range ${offset}+${length}`);
  }
  const absUrl = new URL(blockUrl, client.baseUrl).toString();
  const resp = await fetch(absUrl, {
    method: "GET",
    headers: {
      Range: `bytes=${offset}-${offset + length - 1}`,
    },
  });
  if (resp.status === 206) {
    if (!resp.body) throw new Drive9Error("empty extent block response body");
    return resp.body;
  }
  if (resp.status === 200) {
    if (!resp.body) throw new Drive9Error("empty extent block response body");
    return sliceAndCapExactStream(resp.body, offset, length);
  }
  const text = await resp.text().catch(() => "");
  if (resp.status === 403) {
    throw new PresignExpiredError(resp.status, text);
  }
  throw new StatusError(`extent block range GET: HTTP ${resp.status}: ${text}`, resp.status);
}

function sliceAndCapExactStream(
  body: ReadableStream<Uint8Array>,
  offset: number,
  length: number
): ReadableStream<Uint8Array> {
  const reader = body.getReader();
  let skipped = 0;
  let remaining = length;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      while (remaining > 0) {
        const { done, value } = await reader.read();
        if (done) {
          throw new Drive9Error(`unexpected EOF reading extent block slice (${remaining} bytes short)`);
        }
        let buf = value;
        if (skipped < offset) {
          const skip = Math.min(buf.length, offset - skipped);
          buf = buf.subarray(skip);
          skipped += skip;
        }
        if (buf.length === 0) {
          continue;
        }
        const take = Math.min(buf.length, remaining);
        controller.enqueue(buf.subarray(0, take));
        remaining -= take;
        if (remaining <= 0) {
          controller.close();
          void reader.cancel().catch(() => {});
        }
        return;
      }
      controller.close();
      void reader.cancel().catch(() => {});
    },
    cancel() {
      return reader.cancel();
    },
  });
}

export function serveExtentPlanStream(
  client: Client,
  path: string,
  initialPlan: ExtentReadPlan,
  limit: number,
  rangeEnd: number
): ReadableStream<Uint8Array> {
  let plan = initialPlan;
  let bi = 0;
  let zeroLeft = 0;
  let remainingLimit = limit; // < 0 means unbounded
  let curReader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let refreshOffset = -1;
  let refreshCount = 0;

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      while (true) {
        if (remainingLimit === 0) {
          if (curReader) {
            void curReader.cancel().catch(() => {});
            curReader = undefined;
          }
          controller.close();
          return;
        }
        if (zeroLeft > 0) {
          let n = Math.min(zeroLeft, ZERO_EMIT_CHUNK_BYTES);
          if (remainingLimit >= 0 && n > remainingLimit) {
            n = remainingLimit;
          }
          controller.enqueue(new Uint8Array(n));
          zeroLeft -= n;
          if (n > 0) {
            refreshOffset = -1;
            refreshCount = 0;
          }
          if (remainingLimit >= 0) {
            remainingLimit -= n;
          }
          return;
        }
        if (curReader) {
          const { done, value } = await curReader.read();
          if (done) {
            curReader = undefined;
            continue;
          }
          let chunk = value;
          if (remainingLimit >= 0 && chunk.length > remainingLimit) {
            chunk = chunk.subarray(0, remainingLimit);
          }
          if (remainingLimit >= 0) {
            remainingLimit -= chunk.length;
          }
          if (chunk.length > 0) {
            refreshOffset = -1;
            refreshCount = 0;
            controller.enqueue(chunk);
            return;
          }
          continue;
        }
        if (bi < (plan.blocks?.length ?? 0)) {
          const blk = plan.blocks[bi++];
          if (blk.zero) {
            zeroLeft = blk.length;
            continue;
          }
          if (!blk.url) {
            throw new Drive9Error(`extent plan block at ${blk.file_offset} has no URL`);
          }
          const fetchLen = remainingLimit >= 0 ? Math.min(blk.length, remainingLimit) : blk.length;
          if (fetchLen <= 0) {
            continue;
          }
          try {
            const stream = await fetchExtentBlockRangeImpl(client, blk.url, blk.block_offset, fetchLen);
            curReader = stream.getReader();
          } catch (err) {
            if (err instanceof PresignExpiredError) {
              if (refreshOffset !== blk.file_offset) {
                refreshOffset = blk.file_offset;
                refreshCount = 0;
              }
              if (refreshCount < 1) {
                refreshCount++;
                let end = rangeEnd;
                if (remainingLimit >= 0) {
                  const lim = blk.file_offset + remainingLimit - 1;
                  if (end < 0 || lim < end) end = lim;
                }
                const fresh = await getExtentReadPlanImpl(client, path, blk.file_offset, end);
                if (fresh.blocks && fresh.blocks.length > 0) {
                  if (plan.revision && plan.revision > 0 && fresh.revision && fresh.revision > 0 && fresh.revision !== plan.revision) {
                    throw new Drive9Error(`extent file changed during read: revision ${plan.revision} -> ${fresh.revision}`);
                  }
                  plan = fresh;
                  bi = 0;
                  continue;
                }
              }
            }
            throw err;
          }
          continue;
        }
        if (plan.next_offset && plan.next_offset > 0 && plan.next_offset < plan.size) {
          const prevNext = plan.next_offset;
          let end = rangeEnd;
          if (remainingLimit >= 0) {
            const lim = prevNext + remainingLimit - 1;
            if (end < 0 || lim < end) end = lim;
          }
          const nextPlan = await getExtentReadPlanImpl(client, path, prevNext, end);
          if (plan.revision && plan.revision > 0 && nextPlan.revision && nextPlan.revision > 0 && nextPlan.revision !== plan.revision) {
            throw new Drive9Error(`extent file changed during read: revision ${plan.revision} -> ${nextPlan.revision}`);
          }
          if (
            (nextPlan.next_offset && nextPlan.next_offset <= prevNext) ||
            ((!nextPlan.blocks || nextPlan.blocks.length === 0) && !nextPlan.next_offset && prevNext < nextPlan.size)
          ) {
            throw new Drive9Error(`extent read plan continuation did not advance at offset ${prevNext}`);
          }
          plan = nextPlan;
          bi = 0;
          continue;
        }
        controller.close();
        return;
      }
    },
    async cancel() {
      if (curReader) {
        await curReader.cancel().catch(() => {});
        curReader = undefined;
      }
    },
  });
}

export async function initiateExtentPatchImpl(
  client: Client,
  path: string,
  newSize: number,
  dirtyBlocks: ExtentDirtyBlock[],
  expectedRevision: number,
  expectedResourceId?: string
): Promise<ExtentWritePlan> {
  const headers = client.authHeaders({ "Content-Type": "application/json" });
  if (expectedResourceId) {
    headers["X-Dat9-Expected-Resource"] = expectedResourceId;
  }
  const resp = await fetch(client.fsUrl(path), {
    method: "PATCH",
    headers,
    body: JSON.stringify({
      new_size: newSize,
      dirty_blocks: dirtyBlocks,
      expected_revision: expectedRevision,
    }),
  });
  if (resp.status === 412 && expectedResourceId) {
    await resp.text().catch(() => "");
    throw new ResourceMismatchError(path);
  }
  if (resp.status !== 202) {
    await checkError(resp);
    throw new StatusError(`HTTP ${resp.status}`, resp.status);
  }
  return (await resp.json()) as ExtentWritePlan;
}

export async function commitExtentPatchImpl(
  client: Client,
  path: string,
  newSize: number,
  blocks: ExtentBlockWrite[],
  expectedRevision: number,
  expectedResourceId?: string,
  tags?: Record<string, string>
): Promise<number> {
  if (newSize < 0) {
    throw new Drive9Error("extent patch requires non-negative new size");
  }
  const dirty: ExtentDirtyBlock[] = blocks.map((b, i) => {
    if (b.data.length === 0) {
      throw new Drive9Error(`dirty block ${i} is empty`);
    }
    if (b.data.length > ExtentChunkSize) {
      throw new Drive9Error(`dirty block ${i} exceeds the chunk span`);
    }
    return {
      chunk: b.chunk,
      off: b.off,
      size: b.data.length,
      checksum: sha256Hex(b.data),
    };
  });

  let plan = await initiateExtentPatchImpl(client, path, newSize, dirty, expectedRevision, expectedResourceId);
  for (let attempt = 0; ; attempt++) {
    try {
      await putExtentPlanBlocks(plan, dirty, blocks);
      break;
    } catch (err) {
      await abortUploadV2BestEffort(client, plan.upload_id);
      if (attempt > 0 || !(err instanceof StatusError && isExtentBlockConflictStatus(err.statusCode))) {
        throw err;
      }
      plan = await initiateExtentPatchImpl(client, path, newSize, dirty, expectedRevision, expectedResourceId);
    }
  }

  const parts: CompletePart[] = blocks.map((b, i) => ({
    number: i + 1,
    etag: dirty[i].checksum,
    chunk: b.chunk,
    off: b.off,
    size: b.data.length,
  }));
  return completeUploadV2WithRevision(client, plan.upload_id, parts, tags);
}

async function putExtentPlanBlocks(
  plan: ExtentWritePlan,
  dirty: ExtentDirtyBlock[],
  blocks: ExtentBlockWrite[]
): Promise<void> {
  const seenChecksum = new Set<string>();
  const jobs: { url: string; headers?: Record<string, string>; data: Uint8Array }[] = [];
  for (let i = 0; i < blocks.length; i++) {
    const wb = findExtentPlanBlock(plan, dirty[i]);
    if (!wb) {
      throw new Drive9Error(`extent patch plan lacks block ${dirty[i].chunk}/${dirty[i].off}`);
    }
    if (wb.exists || !wb.url) {
      continue;
    }
    if (seenChecksum.has(dirty[i].checksum)) {
      continue;
    }
    seenChecksum.add(dirty[i].checksum);
    jobs.push({ url: wb.url, headers: wb.headers, data: blocks[i].data });
  }
  if (jobs.length === 0) {
    return;
  }
  const sem = new Semaphore(EXTENT_PUT_CONCURRENCY);
  await Promise.all(
    jobs.map(async (job) => {
      await sem.acquire();
      try {
        await putExtentBlock(job.url, job.headers, job.data);
      } finally {
        sem.release();
      }
    })
  );
}

function findExtentPlanBlock(plan: ExtentWritePlan, d: ExtentDirtyBlock): ExtentWritePlanBlock | undefined {
  return (plan.blocks || []).find((b) => b.chunk === d.chunk && b.off === d.off);
}

export async function putExtentBlock(
  url: string,
  prescribedHeaders: Record<string, unknown> | undefined,
  data: Uint8Array
): Promise<string> {
  const headers: Record<string, string> = {};
  let hasHeaders = false;
  if (prescribedHeaders) {
    for (const [k, v] of Object.entries(prescribedHeaders)) {
      if (typeof v === "string" && !isForbiddenPresignedHeader(k)) {
        headers[k] = v;
        hasHeaders = true;
      }
    }
  }
  if (!hasHeaders) {
    headers["x-amz-checksum-sha256"] = sha256Base64(data);
  }
  const resp = await fetch(url, {
    method: "PUT",
    headers,
    body: bodyInit(data),
  });
  if (resp.status >= 300) {
    const text = await resp.text().catch(() => "");
    throw new StatusError(`extent block PUT: HTTP ${resp.status}: ${text}`, resp.status);
  }
  return resp.headers.get("etag") || "";
}

export async function truncateExtentImpl(
  client: Client,
  path: string,
  newSize: number,
  options?: { expectedRevision?: number; expectedResourceId?: string }
): Promise<StatResult> {
  if (newSize < 0) {
    throw new Drive9Error("extent truncate requires non-negative size");
  }
  let st: StatResult | undefined;
  if (options?.expectedRevision && options.expectedRevision > 0) {
    st = {
      size: -1,
      isDir: false,
      revision: options.expectedRevision,
      resource_id: options.expectedResourceId,
      content_layout: "extent",
    };
  } else {
    st = await client.stat(path);
    if (options?.expectedResourceId && st.resource_id && st.resource_id !== options.expectedResourceId) {
      throw new ResourceMismatchError(path);
    }
    if (st.content_layout && st.content_layout !== "extent") {
      throw new ConflictError("content layout mismatch: the file's layout is sticky for the life of the inode");
    }
    if (st.size === newSize) {
      return st;
    }
  }
  const origResId = options?.expectedResourceId ?? st.resource_id;
  let lastErr: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt > 0 || st.revision <= 0) {
      st = await client.stat(path);
      if (origResId && st.resource_id && st.resource_id !== origResId) {
        throw new ResourceMismatchError(path);
      }
      if (st.content_layout && st.content_layout !== "extent") {
        throw new ConflictError("content layout mismatch: the file's layout is sticky for the life of the inode");
      }
      if (st.size === newSize) {
        return st;
      }
    }
    if (st.revision <= 0) {
      throw new Drive9Error(`${path}: missing revision for extent truncate`);
    }
    try {
      const landedRev = await commitExtentPatchImpl(
        client,
        path,
        newSize,
        [],
        st.revision,
        origResId
      );
      if (landedRev > 0) {
        return {
          ...st,
          size: newSize,
          revision: landedRev,
          content_layout: "extent",
        };
      }
      return { ...st, size: newSize, revision: 0, content_layout: "extent" };
    } catch (err) {
      lastErr = err;
      if (!(err instanceof ConflictError) || (options?.expectedRevision && options.expectedRevision > 0)) {
        throw err;
      }
    }
  }
  throw lastErr;
}

export async function createExtentFileImpl(client: Client, path: string): Promise<CreateExtentFileResult> {
  const resp = await fetch(`${client.fsUrl(path)}?create=1&layout=extent`, {
    method: "POST",
    headers: client.authHeaders(),
  });
  if (resp.status === 400 || resp.status === 403) {
    await resp.text().catch(() => "");
    const fallbackResp = await fetch(`${client.fsUrl(path)}?create=1`, {
      method: "POST",
      headers: client.authHeaders(),
    });
    await checkError(fallbackResp);
    const fallbackBody = (await fallbackResp.json().catch(() => undefined)) as
      | { revision?: number; resource_id?: string }
      | undefined;
    return {
      revision: fallbackBody?.revision || 0,
      resource_id: fallbackBody?.resource_id,
      native_extent: false,
    };
  }
  await checkError(resp);
  const body = (await resp.json().catch(() => undefined)) as
    | { revision?: number; resource_id?: string; content_layout?: string }
    | undefined;
  return {
    revision: body?.revision || 0,
    resource_id: body?.resource_id,
    native_extent: body?.content_layout === "extent",
  };
}

export async function deleteFileConditionalImpl(
  client: Client,
  path: string,
  expectedRevision = 0,
  expectedResourceId = ""
): Promise<void> {
  const headers = client.authHeaders();
  if (expectedRevision > 0) {
    headers["X-Dat9-Expected-Revision"] = String(expectedRevision);
  }
  if (expectedResourceId) {
    headers["X-Dat9-Expected-Resource"] = expectedResourceId;
  }
  const resp = await fetch(`${client.fsUrl(path)}?kind=file`, {
    method: "DELETE",
    headers,
  });
  await checkError(resp);
  await resp.text().catch(() => "");
}

export async function presignExtentChecksums(
  client: Client,
  uploadId: string,
  items: { number: number; checksum: string }[]
): Promise<PresignedPart[]> {
  const resp = await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/presign-batch`, {
    method: "POST",
    headers: client.authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      parts: items.map((item) => ({
        part_number: item.number,
        checksum: { algorithm: "sha256", value: item.checksum },
      })),
    }),
  });
  await checkError(resp);
  const out = (await resp.json()) as { parts?: PresignedPart[] };
  const parts = out.parts || [];
  if (parts.length !== items.length) {
    throw new Drive9Error(`presign-batch returned ${parts.length} parts, want ${items.length}`);
  }
  return parts;
}

export async function uploadExtentSequentialImpl(
  client: Client,
  plan: UploadPlanV2,
  data: Uint8Array,
  tags?: Record<string, string>
): Promise<{ revision: number; uploadedParts: number }> {
  const parts: CompletePart[] = [];
  let uploadedParts = 0;

  for (let first = 1; first <= plan.total_parts; first += EXTENT_BATCH_PARTS) {
    const last = Math.min(first + EXTENT_BATCH_PARTS - 1, plan.total_parts);
    const batchSlices: { number: number; slice: Uint8Array; checksum: string }[] = [];
    for (let n = first; n <= last; n++) {
      const start = (n - 1) * plan.part_size;
      const end = Math.min(start + plan.part_size, data.length);
      const slice = data.subarray(start, end);
      batchSlices.push({ number: n, slice, checksum: sha256Hex(slice) });
    }

    let presigned: PresignedPart[];
    try {
      presigned = await presignExtentChecksums(
        client,
        plan.upload_id,
        batchSlices.map((b) => ({ number: b.number, checksum: b.checksum }))
      );
    } catch (err) {
      if (first === 1 && isExtentPresignUnsupportedError(err)) {
        throw new ExtentPresignUnsupportedError(err instanceof Error ? err.message : String(err));
      }
      throw err;
    }

    const sem = new Semaphore(EXTENT_PUT_CONCURRENCY);
    await Promise.all(
      batchSlices.map(async (item, idx) => {
        const part = presigned[idx];
        if (part.skip_upload) {
          return;
        }
        await sem.acquire();
        try {
          try {
            await putExtentBlock(part.url, part.headers, item.slice);
            uploadedParts++;
          } catch (err) {
            if (!(err instanceof StatusError && isExtentBlockConflictStatus(err.statusCode))) {
              throw err;
            }
            const [fresh] = await presignExtentChecksums(client, plan.upload_id, [
              { number: item.number, checksum: item.checksum },
            ]);
            if (!fresh.skip_upload) {
              await putExtentBlock(fresh.url, fresh.headers, item.slice);
              uploadedParts++;
            }
          }
        } finally {
          sem.release();
        }
      })
    );

    for (const item of batchSlices) {
      parts.push({ number: item.number, etag: item.checksum });
    }
  }

  const revision = await completeUploadV2WithRevision(client, plan.upload_id, parts, tags);
  return { revision, uploadedParts };
}

export async function initiateUploadV2WithLayout(
  client: Client,
  path: string,
  size: number,
  expectedRevision: number,
  description: string,
  layout = "",
  expectedResourceId?: string
): Promise<UploadPlanV2> {
  const payload: {
    path: string;
    total_size: number;
    expected_revision?: number;
    description?: string;
    layout?: string;
  } = {
    path,
    total_size: size,
    description,
  };
  if (expectedRevision >= 0) {
    payload.expected_revision = expectedRevision;
  }
  if (layout) {
    payload.layout = layout;
  }
  const headers = client.authHeaders({ "Content-Type": "application/json" });
  if (expectedResourceId) {
    headers["X-Dat9-Expected-Resource"] = expectedResourceId;
  }
  const resp = await fetch(`${client.baseUrl}/v2/uploads/initiate`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  if (resp.status === 412 && expectedResourceId) {
    await resp.text().catch(() => "");
    throw new ResourceMismatchError(path);
  }
  if (resp.status === 404) {
    await resp.text().catch(() => "");
    throw new Drive9Error("v2 upload API not available");
  }
  await checkError(resp);
  return (await resp.json()) as UploadPlanV2;
}

export async function completeUploadV2WithRevision(
  client: Client,
  uploadId: string,
  parts: CompletePart[],
  tags?: Record<string, string>
): Promise<number> {
  const resp = await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/complete`, {
    method: "POST",
    headers: client.authHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({ parts, tags }),
  });
  await checkError(resp);
  const body = (await resp.json().catch(() => undefined)) as { revision?: number } | undefined;
  return body?.revision || 0;
}

export async function abortUploadV2BestEffort(client: Client, uploadId: string): Promise<void> {
  try {
    await fetch(`${client.baseUrl}/v2/uploads/${uploadId}/abort`, {
      method: "POST",
      headers: client.authHeaders(),
    });
  } catch {
    // Best-effort cleanup.
  }
}

export async function writeExtentBytesImpl(
  client: Client,
  path: string,
  data: Uint8Array,
  options: {
    expectedRevision: number;
    expectedResourceId?: string;
    tags?: Record<string, string>;
    description?: string;
  }
): Promise<{ revision: number; partSize: number; totalParts: number; uploadedParts: number }> {
  let plan: UploadPlanV2;
  try {
    plan = await initiateUploadV2WithLayout(
      client,
      path,
      data.length,
      options.expectedRevision,
      options.description || "",
      "extent",
      options.expectedResourceId
    );
  } catch (err) {
    if (
      data.length === 0 &&
      !options.tags &&
      !options.description &&
      err instanceof StatusError &&
      err.statusCode === 400 &&
      err.message.toLowerCase().includes("total_size must be positive")
    ) {
      if (options.expectedRevision === 0) {
        const created = await createExtentFileImpl(client, path);
        if (!created.native_extent) {
          throw new Drive9Error(`server does not support 0-byte extent file creation on ${path}`);
        }
        return { revision: created.revision, partSize: 0, totalParts: 0, uploadedParts: 0 };
      }
      if (options.expectedRevision > 0) {
        const rev = await commitExtentPatchImpl(client, path, 0, [], options.expectedRevision, options.expectedResourceId);
        return { revision: rev, partSize: 0, totalParts: 0, uploadedParts: 0 };
      }
      try {
        const updated = await truncateExtentImpl(client, path, 0, {
          expectedResourceId: options.expectedResourceId,
        });
        if (updated.content_layout && updated.content_layout !== "extent") {
          throw new ConflictError("content layout mismatch: the file's layout is sticky for the life of the inode");
        }
        return { revision: updated.revision, partSize: 0, totalParts: 0, uploadedParts: 0 };
      } catch (truncErr) {
        const isMissing =
          (truncErr instanceof StatusError && truncErr.statusCode === 404) ||
          (truncErr instanceof Drive9Error && truncErr.message.toLowerCase().includes("not found"));
        if (!isMissing) {
          throw truncErr;
        }
        if (options.expectedResourceId) {
          throw new ResourceMismatchError(path);
        }
        const created = await createExtentFileImpl(client, path);
        if (!created.native_extent) {
          throw new Drive9Error(`server does not support 0-byte extent file creation on ${path}`);
        }
        return { revision: created.revision, partSize: 0, totalParts: 0, uploadedParts: 0 };
      }
    }
    throw err;
  }
  if (plan.key) {
    await abortUploadV2BestEffort(client, plan.upload_id);
    throw new Drive9Error(`server ignored layout=extent on ${path}`);
  }
  try {
    const { revision, uploadedParts } = await uploadExtentSequentialImpl(client, plan, data, options.tags);
    return {
      revision,
      partSize: plan.part_size,
      totalParts: plan.total_parts,
      uploadedParts,
    };
  } catch (err) {
    await abortUploadV2BestEffort(client, plan.upload_id);
    throw err;
  }
}

export async function appendExtentImpl(
  client: Client,
  path: string,
  stat: StatResult,
  data: Uint8Array,
  expectedRevision: number,
  options?: InternalWriteOptions
): Promise<void> {
  const baseSize = stat.size;
  const finalSize = baseSize + data.length;
  const blocks: ExtentBlockWrite[] = [];

  for (let fileOff = baseSize; fileOff < finalSize; ) {
    const chunkEnd = (Math.floor(fileOff / ExtentChunkSize) + 1) * ExtentChunkSize;
    let blkEnd = finalSize;
    if (finalSize - fileOff >= ExtentBlockSize) {
      const gridEnd = (Math.floor(fileOff / ExtentBlockSize) + 1) * ExtentBlockSize;
      if (gridEnd < blkEnd) {
        blkEnd = gridEnd;
      }
    }
    if (blkEnd > chunkEnd) {
      blkEnd = chunkEnd;
    }
    const { chunk, off } = chunkOffset(fileOff);
    const streamOff = fileOff - baseSize;
    blocks.push({
      chunk,
      off,
      data: data.subarray(streamOff, streamOff + (blkEnd - fileOff)),
    });
    fileOff = blkEnd;
  }

  await commitExtentPatchImpl(
    client,
    path,
    finalSize,
    blocks,
    expectedRevision,
    options?.expectedResourceId ?? stat.resource_id,
    options?.tags
  );
}

export async function writeDirectOnlyImpl(
  client: Client,
  path: string,
  data: Uint8Array,
  opts: {
    expectedRevision: number;
    expectedResourceId?: string;
    tags?: Record<string, string>;
    description?: string;
  }
): Promise<number> {
  const headers: [string, string][] = Object.entries(
    client.authHeaders({ "Content-Type": "application/octet-stream" })
  );
  if (opts.expectedRevision >= 0) {
    headers.push(["X-Dat9-Expected-Revision", String(opts.expectedRevision)]);
  }
  if (opts.expectedResourceId) {
    headers.push(["X-Dat9-Expected-Resource", opts.expectedResourceId]);
  }
  if (opts.description) {
    headers.push(["X-Dat9-Description", opts.description]);
  }
  for (const [key, value] of Object.entries(opts.tags || {})) {
    headers.push(["X-Dat9-Tag", `${key}=${value}`]);
  }
  const resp = await fetch(client.fsUrl(path), {
    method: "PUT",
    headers,
    body: bodyInit(data),
  });
  if (resp.status === 412 && opts.expectedResourceId) {
    await resp.text().catch(() => "");
    throw new ResourceMismatchError(path);
  }
  await checkError(resp);
  const body = (await resp.json().catch(() => undefined)) as { revision?: number } | undefined;
  return body?.revision || 0;
}
