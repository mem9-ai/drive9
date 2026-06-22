import type { Client } from "./client.js";
import { Drive9Error, StatusError, checkError } from "./error.js";
import type { PatchPartURL, PatchPlan, StatResult, WriteOptions } from "./models.js";
import { uploadPatchPart } from "./patch.js";

const DEFAULT_APPEND_PART_SIZE = 8 * 1024 * 1024;
const MAX_REWRITE_APPEND_BYTES = 8 * 1024 * 1024;

interface AppendPlan extends PatchPlan {
  base_size: number;
}

export async function appendStreamImpl(
  client: Client,
  path: string,
  stream: ReadableStream<Uint8Array> | Uint8Array,
  size: number,
  options?: WriteOptions
): Promise<void> {
  if (size < 0) {
    throw new Drive9Error("append size must be non-negative");
  }

  const data = stream instanceof Uint8Array ? stream : await streamToBytes(stream);
  if (data.length !== size) {
    throw new Drive9Error(`appendStream size mismatch: got ${data.length}, want ${size}`);
  }

  let stat: StatResult;
  try {
    stat = await client.stat(path);
  } catch (err) {
    if (!isNotFound(err)) throw err;
    await client.writeStream(path, data, size, { ...options, expectedRevision: options?.expectedRevision ?? 0 });
    return;
  }

  if (stat.isDir) {
    throw new Drive9Error(`is a directory: ${path}`);
  }
  if (size === 0) {
    return;
  }

  const finalSize = stat.size + size;
  if (finalSize < stat.size) {
    throw new Drive9Error("append size overflows file size");
  }

  const expectedRevision = options?.expectedRevision ?? stat.revision;
  let plan: AppendPlan;
  try {
    plan = await initiateAppend(client, path, size, expectedRevision);
  } catch (err) {
    if (shouldRewriteAppend(err)) {
      await appendByBoundedRewrite(client, path, stat, data, expectedRevision, options);
      return;
    }
    throw err;
  }

  const uploadParts = plan.upload_parts || [];
  validateAppendPlan(plan, uploadParts, data.length);
  for (const part of uploadParts) {
    if (appendPartOverlapsExisting(plan.base_size, plan.part_size, part) && !part.read_url) {
      throw new Drive9Error(`append part ${part.number} overlaps existing data but is missing read_url`);
    }
    await uploadPatchPart(client, part, (_partNumber, _partSize, origData) =>
      appendPartData(plan.base_size, plan.part_size, part, data, origData)
    );
  }

  await completeUpload(client, plan.upload_id, options?.tags);
}

async function initiateAppend(client: Client, path: string, appendSize: number, expectedRevision: number): Promise<AppendPlan> {
  const body: { append_size: number; part_size: number; expected_revision?: number } = {
    append_size: appendSize,
    part_size: DEFAULT_APPEND_PART_SIZE,
  };
  if (expectedRevision >= 0) {
    body.expected_revision = expectedRevision;
  }

  const resp = await fetch(`${client.fsUrl(path)}?append`, {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify(body),
  });
  if (resp.status !== 202) {
    await checkError(resp);
    throw new StatusError(`HTTP ${resp.status}`, resp.status);
  }
  return (await resp.json()) as AppendPlan;
}

async function appendByBoundedRewrite(
  client: Client,
  path: string,
  stat: StatResult,
  appendData: Uint8Array,
  expectedRevision: number,
  options?: WriteOptions
): Promise<void> {
  const limit = Math.max(client.cachedSmallFileThreshold(), MAX_REWRITE_APPEND_BYTES);
  const plannedSize = stat.size + appendData.length;
  if (plannedSize > limit) {
    throw new Drive9Error(
      `native append is unavailable for ${path}; refusing read-modify-write append of ${plannedSize} bytes above ${limit} bytes`
    );
  }

  const existing = await client.read(path);
  if (existing.length + appendData.length > limit) {
    throw new Drive9Error(
      `native append is unavailable for ${path}; refusing read-modify-write append of ${existing.length + appendData.length} bytes above ${limit} bytes`
    );
  }
  const merged = new Uint8Array(existing.length + appendData.length);
  merged.set(existing);
  merged.set(appendData, existing.length);
  await client.writeStream(path, merged, merged.length, { ...options, expectedRevision });
}

function appendPartData(
  baseSize: number,
  partSize: number,
  part: PatchPartURL,
  appendData: Uint8Array,
  origData?: Uint8Array
): Uint8Array {
  const partStart = (part.number - 1) * partSize;
  const partEnd = partStart + part.size;
  const existingEnd = Math.min(baseSize, partEnd);
  const expectedOrigLen = Math.max(0, existingEnd - partStart);
  if (expectedOrigLen > 0 && !origData) {
    throw new Drive9Error(`append part ${part.number} overlaps existing data but original data was not read`);
  }
  if (origData && origData.length !== expectedOrigLen) {
    throw new Drive9Error(`append part ${part.number} original data length ${origData.length}, want ${expectedOrigLen}`);
  }

  const appendStart = Math.max(0, partStart - baseSize);
  const appendEnd = Math.min(appendData.length, partEnd - baseSize);
  const appendSlice = appendData.subarray(appendStart, appendEnd);
  const origLen = origData?.length || 0;
  if (origLen + appendSlice.length !== part.size) {
    throw new Drive9Error(`append part ${part.number} size mismatch: got ${origLen + appendSlice.length}, want ${part.size}`);
  }

  const out = new Uint8Array(part.size);
  if (origData) out.set(origData);
  out.set(appendSlice, origLen);
  return out;
}

function validateAppendPlan(plan: AppendPlan, uploadParts: PatchPartURL[], appendSize: number): void {
  const ranges = uploadParts
    .map((part) => appendRange(plan.base_size, plan.part_size, part, appendSize))
    .filter((range): range is { start: number; end: number } => range.end > range.start)
    .sort((a, b) => a.start - b.start);

  let cursor = 0;
  for (const range of ranges) {
    if (range.start !== cursor) {
      throw new Drive9Error(`append plan does not cover append bytes at offset ${cursor}`);
    }
    cursor = range.end;
  }
  if (cursor !== appendSize) {
    throw new Drive9Error(`append plan covers ${cursor} append bytes, want ${appendSize}`);
  }
}

function appendRange(baseSize: number, partSize: number, part: PatchPartURL, appendSize: number): { start: number; end: number } {
  const partStart = (part.number - 1) * partSize;
  const partEnd = partStart + part.size;
  return {
    start: Math.max(0, partStart - baseSize),
    end: Math.min(appendSize, partEnd - baseSize),
  };
}

async function completeUpload(client: Client, uploadId: string, tags?: Record<string, string>): Promise<void> {
  const body = tags != null ? JSON.stringify({ tags }) : undefined;
  const resp = await fetch(`${client.baseUrl}/v1/uploads/${uploadId}/complete`, {
    method: "POST",
    headers: client["authHeaders"](tags != null ? { "Content-Type": "application/json" } : undefined),
    body,
  });
  await checkError(resp);
}

async function streamToBytes(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const resp = new Response(stream);
  return new Uint8Array(await resp.arrayBuffer());
}

function appendPartOverlapsExisting(baseSize: number, partSize: number, part: PatchPartURL): boolean {
  if (baseSize <= 0 || partSize <= 0 || part.number <= 0) {
    return false;
  }
  return (part.number - 1) * partSize < baseSize;
}

function shouldRewriteAppend(err: unknown): boolean {
  if (!(err instanceof StatusError)) return false;
  const msg = err.message.toLowerCase();
  if (err.statusCode === 404 || err.statusCode === 405) {
    return true;
  }
  if (err.statusCode !== 400) {
    return false;
  }
  return msg.includes("file is not s3-stored") || msg.includes("s3 not configured") || msg.includes("unknown post action");
}

function isNotFound(err: unknown): boolean {
  if (err instanceof StatusError && err.statusCode === 404) return true;
  return err instanceof Error && err.message.toLowerCase().includes("not found");
}
