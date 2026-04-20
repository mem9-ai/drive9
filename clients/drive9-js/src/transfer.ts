import { Client } from "./client.js";
import { bodyInit } from "./compat.js";
import { Drive9Error, StatusError, checkError } from "./error.js";
import type { CompletePart, PresignedPart, UploadMeta, UploadPlan, UploadPlanV2 } from "./models.js";
import { Semaphore } from "./utils.js";

const PART_SIZE = 8 * 1024 * 1024;
const UPLOAD_MAX_CONCURRENCY = 16;
const UPLOAD_MAX_BUFFER_BYTES = 256 * 1024 * 1024;

function uploadParallelism(partSize: number): number {
  const byMemory = Math.max(1, Math.floor(UPLOAD_MAX_BUFFER_BYTES / partSize));
  return Math.min(byMemory, UPLOAD_MAX_CONCURRENCY);
}

// CRC32C lookup table (Castagnoli polynomial)
const CRC32C_TABLE = new Int32Array(256);
(function initTable() {
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = crc & 1 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
    }
    CRC32C_TABLE[i] = crc;
  }
})();

export function computeCrc32c(data: Uint8Array): string {
  let crc = ~0;
  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ data[i]) & 0xff];
  }
  crc = ~crc >>> 0;
  return Buffer.from([
    (crc >>> 24) & 0xff,
    (crc >>> 16) & 0xff,
    (crc >>> 8) & 0xff,
    crc & 0xff,
  ]).toString("base64");
}

async function sha256Base64(data: Uint8Array): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Buffer.from(hash).toString("base64");
}

async function streamToUint8Array(stream: ReadableStream<Uint8Array>, size: number): Promise<Uint8Array> {
  const result = new Uint8Array(size);
  let offset = 0;
  const reader = stream.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    result.set(value, offset);
    offset += value.length;
  }
  return result.slice(0, offset);
}

export async function readStreamImpl(client: Client, path: string): Promise<ReadableStream<Uint8Array>> {
  const resp = await fetch(client.fsUrl(path), {
    method: "GET",
    headers: client["authHeaders"](),
    redirect: "manual",
  });
  const status = resp.status;
  if (status === 302 || status === 307) {
    const location = resp.headers.get("location") || resp.headers.get("Location");
    if (!location) throw new Drive9Error("302 without Location header");
    const resp2 = await fetch(location, { method: "GET" });
    await checkError(resp2);
    if (!resp2.body) throw new Drive9Error("empty response body");
    return resp2.body;
  }
  await checkError(resp);
  if (!resp.body) throw new Drive9Error("empty response body");
  return resp.body;
}

export async function readStreamRangeImpl(
  client: Client,
  path: string,
  offset: number,
  length: number
): Promise<ReadableStream<Uint8Array>> {
  if (length <= 0) {
    return new ReadableStream({
      start(controller) {
        controller.close();
      },
    });
  }
  const resp = await fetch(client.fsUrl(path), {
    method: "GET",
    headers: client["authHeaders"](),
    redirect: "manual",
  });
  const status = resp.status;
  if (status === 302 || status === 307) {
    const location = resp.headers.get("location") || resp.headers.get("Location");
    if (!location) throw new Drive9Error("302 without Location header");
    const resp2 = await fetch(location, {
      method: "GET",
      headers: { Range: `bytes=${offset}-${offset + length - 1}` },
    });
    if (resp2.status === 416) {
      return new ReadableStream({ start(c) { c.close(); } });
    }
    await checkError(resp2);
    if (!resp2.body) throw new Drive9Error("empty response body");
    if (resp2.status === 206) return resp2.body;
    // 200: server ignored range; slice locally
    return sliceStream(resp2.body, offset, length);
  }
  if (resp.status >= 300) {
    await checkError(resp);
    throw new StatusError(`HTTP ${resp.status}`, resp.status);
  }
  if (!resp.body) throw new Drive9Error("empty response body");
  return sliceStream(resp.body, offset, length);
}

async function sliceStream(body: ReadableStream<Uint8Array>, offset: number, length: number): Promise<ReadableStream<Uint8Array>> {
  const reader = body.getReader();
  let skipped = 0;
  let emitted = 0;
  return new ReadableStream({
    pull(controller) {
      return reader.read().then(({ done, value }) => {
        if (done) {
          controller.close();
          return;
        }
        let buf = value;
        if (skipped < offset) {
          const toSkip = Math.min(buf.length, offset - skipped);
          buf = buf.subarray(toSkip);
          skipped += toSkip;
        }
        if (emitted < length) {
          const toEmit = Math.min(buf.length, length - emitted);
          if (toEmit > 0) {
            controller.enqueue(buf.subarray(0, toEmit));
            emitted += toEmit;
          }
        }
        if (emitted >= length) {
          controller.close();
          reader.cancel().catch(() => {});
        }
      });
    },
    cancel() {
      return reader.cancel();
    },
  });
}

export async function writeStreamImpl(
  client: Client,
  path: string,
  stream: ReadableStream<Uint8Array> | Uint8Array,
  size: number,
  expectedRevision = -1
): Promise<void> {
  const threshold = client["smallFileThreshold"];
  let data: Uint8Array;
  if (stream instanceof Uint8Array) {
    data = stream;
  } else {
    data = await streamToUint8Array(stream, size);
  }
  if (size < threshold) {
    return client.write(path, data, expectedRevision);
  }
  try {
    await writeStreamV2(client, path, data, expectedRevision);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes("v2 upload API not available")) {
      await writeStreamV1(client, path, data, expectedRevision);
    } else {
      throw err;
    }
  }
}

async function writeStreamV1(
  client: Client,
  path: string,
  data: Uint8Array,
  expectedRevision: number
): Promise<void> {
  const checksums = computePartChecksums(data, PART_SIZE);
  const plan = await initiateUpload(client, path, data.length, checksums, expectedRevision);
  await uploadPartsV1(client, plan, data);
  await completeUpload(client, plan.upload_id);
}

async function writeStreamV2(
  client: Client,
  path: string,
  data: Uint8Array,
  expectedRevision: number
): Promise<void> {
  const plan = await initiateUploadV2(client, path, data.length, expectedRevision);
  try {
    const parts = await uploadPartsV2(client, plan, data);
    await completeUploadV2(client, plan.upload_id, parts);
  } catch (err) {
    await abortUploadV2(client, plan.upload_id);
    throw err;
  }
}

function computePartChecksums(data: Uint8Array, partSize: number): string[] {
  const count = Math.max(1, Math.ceil(data.length / partSize));
  const out: string[] = [];
  for (let i = 0; i < count; i++) {
    const start = i * partSize;
    const end = Math.min(start + partSize, data.length);
    out.push(computeCrc32c(data.subarray(start, end)));
  }
  return out;
}

async function initiateUpload(
  client: Client,
  path: string,
  size: number,
  checksums: string[],
  expectedRevision: number
): Promise<UploadPlan> {
  try {
    return await initiateUploadByBody(client, path, size, checksums, expectedRevision);
  } catch (err) {
    const status = err instanceof StatusError ? err.statusCode : 0;
    const msg = err instanceof Error ? err.message : String(err);
    if (status === 404 || status === 405) {
      return initiateUploadLegacy(client, path, size, checksums, expectedRevision);
    }
    if (status === 400 && msg.toLowerCase().includes("unknown upload action")) {
      return initiateUploadLegacy(client, path, size, checksums, expectedRevision);
    }
    throw err;
  }
}

async function initiateUploadByBody(
  client: Client,
  path: string,
  size: number,
  checksums: string[],
  expectedRevision: number
): Promise<UploadPlan> {
  const resp = await fetch(`${client.baseUrl}/v1/uploads/initiate`, {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ path, size, part_checksums: checksums, expected_revision: expectedRevision }),
  });
  if (resp.status === 202) {
    return (await resp.json()) as UploadPlan;
  }
  const text = await resp.text().catch(() => "");
  throw new StatusError(text || `HTTP ${resp.status}`, resp.status);
}

async function initiateUploadLegacy(
  client: Client,
  path: string,
  size: number,
  checksums: string[],
  expectedRevision: number
): Promise<UploadPlan> {
  const headers = client["authHeaders"]({ "Content-Type": "application/octet-stream" });
  headers["X-Dat9-Content-Length"] = String(size);
  if (checksums.length) headers["X-Dat9-Part-Checksums"] = checksums.join(",");
  if (expectedRevision >= 0) headers["X-Dat9-Expected-Revision"] = String(expectedRevision);
  const resp = await fetch(client.fsUrl(path), { method: "PUT", headers });
  await checkError(resp);
  return (await resp.json()) as UploadPlan;
}

async function uploadPartsV1(client: Client, plan: UploadPlan, data: Uint8Array): Promise<void> {
  const parallelism = uploadParallelism(plan.part_size);
  const semaphore = new Semaphore(parallelism);
  const tasks: Promise<void>[] = [];
  for (const part of plan.parts) {
    const offset = (part.number - 1) * plan.part_size;
    const chunk = data.subarray(offset, offset + part.size);
    tasks.push(
      (async () => {
        await semaphore.acquire();
        try {
          await uploadOnePart(client, part.url, chunk, part.checksum_crc32c);
        } finally {
          semaphore.release();
        }
      })()
    );
  }
  await Promise.all(tasks);
}

async function uploadOnePart(
  client: Client,
  url: string,
  data: Uint8Array,
  checksumCrc32c?: string
): Promise<void> {
  const checksum = checksumCrc32c || computeCrc32c(data);
  const resp = await fetch(url, {
    method: "PUT",
    headers: { "x-amz-checksum-crc32c": checksum },
    body: bodyInit(data),
  });
  await checkError(resp);
}

async function completeUpload(client: Client, uploadId: string): Promise<void> {
  const resp = await fetch(`${client.baseUrl}/v1/uploads/${uploadId}/complete`, {
    method: "POST",
    headers: client["authHeaders"](),
  });
  await checkError(resp);
}

async function initiateUploadV2(
  client: Client,
  path: string,
  size: number,
  expectedRevision: number
): Promise<UploadPlanV2> {
  const resp = await fetch(`${client.baseUrl}/v2/uploads/initiate`, {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ path, size, expected_revision: expectedRevision }),
  });
  if (resp.status === 404) {
    throw new Drive9Error("v2 upload API not available");
  }
  await checkError(resp);
  return (await resp.json()) as UploadPlanV2;
}

async function uploadPartsV2(client: Client, plan: UploadPlanV2, data: Uint8Array): Promise<CompletePart[]> {
  const psize = plan.part_size;
  const totalParts = Math.max(1, Math.ceil(data.length / psize));
  const parallelism = uploadParallelism(psize);
  const semaphore = new Semaphore(parallelism);
  const tasks: Promise<CompletePart>[] = [];
  const results: (CompletePart | undefined)[] = new Array(totalParts).fill(undefined);

  for (let i = 1; i <= totalParts; i++) {
    const num = i;
    tasks.push(
      (async () => {
        await semaphore.acquire();
        try {
          const offset = (num - 1) * psize;
          const chunk = data.subarray(offset, Math.min(offset + psize, data.length));
          const presigned = await presignOnePart(client, plan.upload_id, num);
          const etag = await uploadOnePartV2(client, plan.upload_id, presigned, chunk);
          const part: CompletePart = { number: num, etag };
          results[num - 1] = part;
          return part;
        } finally {
          semaphore.release();
        }
      })()
    );
  }
  await Promise.all(tasks);
  return results.filter((p): p is CompletePart => p !== undefined);
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

export async function resumeUploadImpl(
  client: Client,
  path: string,
  data: Uint8Array
): Promise<void> {
  const meta = await queryUpload(client, path);
  const checksums = computePartChecksums(data, PART_SIZE);
  const plan = await requestResume(client, meta.upload_id, checksums);
  if (plan.parts.length === 0) {
    await completeUpload(client, meta.upload_id);
    return;
  }
  await uploadMissingParts(client, plan, data, meta.parts_total);
  await completeUpload(client, meta.upload_id);
}

async function queryUpload(client: Client, path: string): Promise<UploadMeta> {
  const resp = await fetch(`${client.baseUrl}/v1/uploads?path=${encodeURIComponent(path)}&status=UPLOADING`, {
    headers: client["authHeaders"](),
  });
  await checkError(resp);
  const body = (await resp.json()) as { uploads?: UploadMeta[] };
  const uploads = body.uploads || [];
  const first = uploads[0];
  if (!first) throw new Drive9Error(`no active upload for ${path}`);
  return first;
}

async function requestResume(client: Client, uploadId: string, checksums: string[]): Promise<UploadPlan> {
  try {
    return await requestResumeByBody(client, uploadId, checksums);
  } catch (err) {
    const status = err instanceof StatusError ? err.statusCode : 0;
    const msg = err instanceof Error ? err.message : String(err);
    if (status === 400 && msg.toLowerCase().includes("missing x-dat9-part-checksums header")) {
      return requestResumeLegacy(client, uploadId, checksums);
    }
    throw err;
  }
}

async function requestResumeByBody(client: Client, uploadId: string, checksums: string[]): Promise<UploadPlan> {
  const resp = await fetch(`${client.baseUrl}/v1/uploads/${uploadId}/resume`, {
    method: "POST",
    headers: client["authHeaders"]({ "Content-Type": "application/json" }),
    body: JSON.stringify({ part_checksums: checksums }),
  });
  const status = resp.status;
  if (status === 202) {
    return (await resp.json()) as UploadPlan;
  }
  const text = await resp.text().catch(() => "");
  throw new StatusError(text || `HTTP ${status}`, status);
}

async function requestResumeLegacy(client: Client, uploadId: string, checksums: string[]): Promise<UploadPlan> {
  const headers = client["authHeaders"]({ "Content-Type": "application/octet-stream" });
  if (checksums.length) headers["X-Dat9-Part-Checksums"] = checksums.join(",");
  const resp = await fetch(`${client.baseUrl}/v1/uploads/${uploadId}/resume`, {
    method: "POST",
    headers,
  });
  if (resp.status === 410) {
    throw new Drive9Error(`upload ${uploadId} has expired`);
  }
  await checkError(resp);
  return (await resp.json()) as UploadPlan;
}

async function uploadMissingParts(client: Client, plan: UploadPlan, data: Uint8Array, partsTotal: number): Promise<void> {
  const stdPartSize = Math.floor(data.length / partsTotal) || PART_SIZE;
  const parallelism = uploadParallelism(stdPartSize);
  const semaphore = new Semaphore(parallelism);
  const tasks: Promise<void>[] = [];
  for (const part of plan.parts) {
    const offset = (part.number - 1) * stdPartSize;
    const chunk = data.subarray(offset, offset + part.size);
    tasks.push(
      (async () => {
        await semaphore.acquire();
        try {
          await uploadOnePart(client, part.url, chunk, part.checksum_crc32c);
        } finally {
          semaphore.release();
        }
      })()
    );
  }
  await Promise.all(tasks);
}

