import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { gunzipSync } from "node:zlib";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import {
  Client,
  ConflictError,
  Drive9Error,
  ExtentBlockSize,
  ExtentChunkSize,
  ExtentNotServedError,
  ResourceMismatchError,
  chunkOffset,
} from "../src/index.js";
import { Drive9Fs } from "../src/just-bash.js";

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const text = new TextEncoder();
const decode = (b: Uint8Array) => new TextDecoder().decode(b);
const sha256Hex = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

describe("Extent layout v2 support in TypeScript SDK", () => {
  it("surfaces storage_type and content_layout across stat, statMetadata, and statMetadataCompat", async () => {
    server.use(
      http.head("http://localhost:9009/v1/fs/ext.bin", () => {
        return new HttpResponse(null, {
          status: 200,
          headers: {
            "Content-Length": "16",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "5",
            "X-Dat9-Resource-ID": "res-ext-1",
            "X-Dat9-Storage-Type": "extent",
            "X-Dat9-Content-Layout": "extent",
          },
        });
      }),
      http.get("http://localhost:9009/v1/fs/ext.bin", ({ request }) => {
        if (new URL(request.url).searchParams.has("stat")) {
          return HttpResponse.json({
            size: 16,
            isdir: false,
            resource_id: "res-ext-1",
            revision: 5,
            content_type: "application/octet-stream",
            storage_type: "extent",
            content_layout: "extent",
            semantic_text: "",
            tags: { kind: "dataset" },
          });
        }
        return HttpResponse.text("unexpected", { status: 500 });
      }),
      http.get("http://localhost:9009/v1/fs/ext-legacy-stat.bin", () => {
        return HttpResponse.text("not json", { status: 405 });
      }),
      http.head("http://localhost:9009/v1/fs/ext-legacy-stat.bin", () => {
        return new HttpResponse(null, {
          status: 200,
          headers: {
            "Content-Length": "32",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "2",
            "X-Dat9-Storage-Type": "extent",
            "X-Dat9-Content-Layout": "extent",
          },
        });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    const st = await client.stat("/ext.bin");
    expect(st.storage_type).toBe("extent");
    expect(st.content_layout).toBe("extent");
    expect(st.resource_id).toBe("res-ext-1");

    const meta = await client.statMetadata("/ext.bin");
    expect(meta.storage_type).toBe("extent");
    expect(meta.content_layout).toBe("extent");
    expect(meta.tags.kind).toBe("dataset");

    const compat = await client.statMetadataCompat("/ext-legacy-stat.bin");
    expect(compat.degraded).toBe(true);
    expect(compat.storage_type).toBe("extent");
    expect(compat.content_layout).toBe("extent");
  });

  it("reads extent files with sparse holes, 200-OK block slicing, continuations, and 403 presign refresh", async () => {
    const holeLen = 70_000; // > 64 KiB zero chunk size
    const blockA = text.encode("AAAA");
    const blockBFull = text.encode("xxBBBBzz"); // block_offset=2, length=4 => "BBBB"
    const blockC = text.encode("CCCC");
    const totalSize = 4 + holeLen + 4 + 4;
    let expiredOnce = false;

    server.use(
      http.get("http://localhost:9009/v1/fs/sparse.bin", ({ request }) => {
        expect(request.headers.get("x-dat9-extent-read-plan")).toBe("1");
        const rng = request.headers.get("range") || "bytes=0-";
        if (rng.startsWith("bytes=0-")) {
          return HttpResponse.json(
            {
              size: totalSize,
              revision: 9,
              blocks: [
                { file_offset: 0, length: 4, block_offset: 0, url: "http://localhost:9009/obj/a" },
                { file_offset: 4, length: holeLen, block_offset: 0, zero: true },
                { file_offset: 4 + holeLen, length: 4, block_offset: 2, url: "http://localhost:9009/obj/b-200" },
              ],
              next_offset: 4 + holeLen + 4,
            },
            {
              status: 206,
              headers: {
                "X-Dat9-Content-Layout": "extent",
                "X-Dat9-Storage-Type": "extent",
              },
            }
          );
        }
        if (rng.startsWith(`bytes=${4 + holeLen + 4}-`)) {
          return HttpResponse.json(
            {
              size: totalSize,
              revision: 9,
              blocks: [
                {
                  file_offset: 4 + holeLen + 4,
                  length: 4,
                  block_offset: 0,
                  url: expiredOnce ? "http://localhost:9009/obj/c-fresh" : "http://localhost:9009/obj/c-expired",
                },
              ],
            },
            {
              status: 206,
              headers: { "X-Dat9-Content-Layout": "extent" },
            }
          );
        }
        if (rng === `bytes=2-${5 + holeLen}`) {
          return HttpResponse.json(
            {
              size: totalSize,
              revision: 9,
              blocks: [
                { file_offset: 2, length: 2, block_offset: 2, url: "http://localhost:9009/obj/a" },
                { file_offset: 4, length: holeLen, block_offset: 0, zero: true },
                { file_offset: 4 + holeLen, length: 2, block_offset: 2, url: "http://localhost:9009/obj/b-200" },
              ],
            },
            {
              status: 206,
              headers: { "X-Dat9-Content-Layout": "extent" },
            }
          );
        }
        return HttpResponse.text(`unexpected range ${rng}`, { status: 400 });
      }),
      http.get("http://localhost:9009/obj/a", ({ request }) => {
        const rng = request.headers.get("range") || "";
        const m = /^bytes=(\d+)-(\d+)$/.exec(rng);
        if (!m) return HttpResponse.arrayBuffer(blockA.buffer.slice(0));
        const slice = blockA.subarray(Number(m[1]), Number(m[2]) + 1);
        return new HttpResponse(Uint8Array.from(slice).buffer, { status: 206 });
      }),
      http.get("http://localhost:9009/obj/b-200", () => {
        // Simulates a plain HTTP object server that ignores Range and returns 200 OK.
        return new HttpResponse(Uint8Array.from(blockBFull).buffer, { status: 200 });
      }),
      http.get("http://localhost:9009/obj/c-expired", () => {
        expiredOnce = true;
        return new HttpResponse("SignatureDoesNotMatch", { status: 403 });
      }),
      http.get("http://localhost:9009/obj/c-fresh", () => {
        return new HttpResponse(Uint8Array.from(blockC).buffer, { status: 206 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    const full = await client.read("/sparse.bin");
    expect(full.length).toBe(totalSize);
    expect(decode(full.subarray(0, 4))).toBe("AAAA");
    expect(full.subarray(4, 4 + holeLen).every((b) => b === 0)).toBe(true);
    expect(decode(full.subarray(4 + holeLen, 4 + holeLen + 4))).toBe("BBBB");
    expect(decode(full.subarray(4 + holeLen + 4))).toBe("CCCC");
    expect(expiredOnce).toBe(true);

    const sub = await client.readAt("/sparse.bin", 2, 4 + holeLen);
    expect(sub.length).toBe(4 + holeLen);
    expect(decode(sub.subarray(0, 2))).toBe("AA");
    expect(sub.subarray(2, 2 + holeLen).every((b) => b === 0)).toBe(true);
    expect(decode(sub.subarray(2 + holeLen))).toBe("BB");

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-ext-dl-"));
    try {
      const dlFile = path.join(tmpDir, "sparse.bin");
      await client.downloadToFile("/sparse.bin", dlFile);
      expect(new Uint8Array(fs.readFileSync(dlFile))).toEqual(full);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("supports extent files in archive() and downloadDir()", async () => {
    const extBytes = new Uint8Array(12);
    extBytes.set(text.encode("HEAD"), 0);
    extBytes.set(text.encode("TAIL"), 8);

    server.use(
      http.head("http://localhost:9009/v1/fs/tree", () => {
        return new HttpResponse(null, {
          status: 200,
          headers: { "X-Dat9-IsDir": "true", "X-Dat9-Revision": "1" },
        });
      }),
      http.get("http://localhost:9009/v1/fs/tree", ({ request }) => {
        if (new URL(request.url).searchParams.has("list")) {
          return HttpResponse.json({
            entries: [{ name: "ext.bin", size: 12, isDir: false, mode: 0o644, hasMode: true }],
          });
        }
        return HttpResponse.text("unexpected", { status: 400 });
      }),
      http.get("http://localhost:9009/v1/fs/tree/ext.bin", ({ request }) => {
        expect(request.headers.get("x-dat9-extent-read-plan")).toBe("1");
        return HttpResponse.json(
          {
            size: 12,
            revision: 3,
            blocks: [
              { file_offset: 0, length: 4, block_offset: 0, url: "http://localhost:9009/obj/head" },
              { file_offset: 4, length: 4, block_offset: 0, zero: true },
              { file_offset: 8, length: 4, block_offset: 0, url: "http://localhost:9009/obj/tail" },
            ],
          },
          { status: 206, headers: { "X-Dat9-Content-Layout": "extent" } }
        );
      }),
      http.get("http://localhost:9009/obj/head", () => new HttpResponse(text.encode("HEAD"), { status: 206 })),
      http.get("http://localhost:9009/obj/tail", () => new HttpResponse(text.encode("TAIL"), { status: 206 }))
    );

    const client = new Client("http://localhost:9009", "test-key");
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-ext-tree-"));
    try {
      const dst = path.join(tmpDir, "dl");
      await client.downloadDir("/tree", dst);
      expect(new Uint8Array(fs.readFileSync(path.join(dst, "ext.bin")))).toEqual(extBytes);

      const archiveStream = await client.archive("/tree");
      const tarGz = new Uint8Array(await new Response(archiveStream).arrayBuffer());
      const tar = gunzipSync(tarGz);
      // Verify "HEAD\0\0\0\0TAIL" appears inside the uncompressed tar payload.
      expect(Buffer.from(tar).includes(Buffer.from(extBytes))).toBe(true);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("defaults large writeStream uploads to layout=extent with dedupe and create-once conflict recovery", async () => {
    const partSize = 64;
    const part1 = new Uint8Array(partSize).fill(0x61); // 'a'
    const part2 = new Uint8Array(partSize).fill(0x61); // duplicate of part1 -> skip_upload
    const part3 = new Uint8Array(20).fill(0x62); // 'b' -> conflict then skip_upload on re-presign
    const payload = new Uint8Array(part1.length + part2.length + part3.length);
    payload.set(part1, 0);
    payload.set(part2, part1.length);
    payload.set(part3, part1.length + part2.length);

    let initiateBody: Record<string, unknown> | undefined;
    let put1Count = 0;
    let put3Attempts = 0;
    let completeBody: { parts?: Array<{ number: number; etag: string }>; tags?: Record<string, string> } | undefined;

    server.use(
      http.post("http://localhost:9009/v2/uploads/initiate", async ({ request }) => {
        expect(request.headers.get("x-dat9-expected-resource")).toBe("res-99");
        initiateBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json(
          {
            upload_id: "ext-up-1",
            key: "",
            part_size: partSize,
            total_parts: 3,
            checksum_contract: { supported: ["SHA-256"], required: true },
          },
          { status: 202 }
        );
      }),
      http.post("http://localhost:9009/v2/uploads/ext-up-1/presign-batch", async ({ request }) => {
        const body = (await request.json()) as {
          parts: Array<{ part_number: number; checksum: { algorithm: string; value: string } }>;
        };
        if (body.parts.length === 3) {
          return HttpResponse.json({
            parts: [
              {
                number: 1,
                url: "http://localhost:9009/obj/put-1",
                size: partSize,
                headers: { "x-amz-checksum-sha256": "presigned-1" },
              },
              {
                number: 2,
                url: "",
                size: partSize,
                skip_upload: true,
              },
              {
                number: 3,
                url: "http://localhost:9009/obj/put-3-conflict",
                size: 20,
                headers: { "If-None-Match": "*" },
              },
            ],
          });
        }
        // Re-presign after create-once 412 conflict on part 3: peer already landed the block.
        expect(body.parts).toHaveLength(1);
        expect(body.parts[0].part_number).toBe(3);
        return HttpResponse.json({
          parts: [{ number: 3, url: "", size: 20, skip_upload: true }],
        });
      }),
      http.put("http://localhost:9009/obj/put-1", async ({ request }) => {
        put1Count++;
        expect(request.headers.get("x-amz-checksum-sha256")).toBe("presigned-1");
        expect(new Uint8Array(await request.arrayBuffer())).toEqual(part1);
        return new HttpResponse(null, { status: 200, headers: { ETag: '"etag-1"' } });
      }),
      http.put("http://localhost:9009/obj/put-3-conflict", () => {
        put3Attempts++;
        return new HttpResponse("PreconditionFailed", { status: 412 });
      }),
      http.post("http://localhost:9009/v2/uploads/ext-up-1/complete", async ({ request }) => {
        completeBody = (await request.json()) as typeof completeBody;
        return HttpResponse.json({ status: "ok", revision: 12 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key").withSmallFileThreshold(32);
    const summary = await client.writeStreamWithSummary("/large-ext.bin", payload, payload.length, {
      expectedRevision: 4,
      expectedResourceId: "res-99",
      description: "extent dataset",
      tags: { tier: "hot" },
    });

    expect(initiateBody).toMatchObject({
      path: "/large-ext.bin",
      total_size: payload.length,
      expected_revision: 4,
      description: "extent dataset",
      layout: "extent",
    });
    expect(summary.mode).toBe("extent_v2");
    expect(summary.part_size_bytes).toBe(partSize);
    expect(summary.total_parts).toBe(3);
    expect(summary.uploaded_parts).toBe(1);
    expect(put1Count).toBe(1);
    expect(put3Attempts).toBe(1);
    expect(completeBody).toEqual({
      parts: [
        { number: 1, etag: sha256Hex(part1) },
        { number: 2, etag: sha256Hex(part2) },
        { number: 3, etag: sha256Hex(part3) },
      ],
      tags: { tier: "hot" },
    });
  });

  it("degrades seamlessly to classic multipart_v2 on legacy servers, sticky single targets, and Azure stores", async () => {
    const data = new Uint8Array(128).fill(0x78);
    const client = new Client("http://localhost:9009", "test-key").withSmallFileThreshold(32);

    // Case 1: Server ignores layout=extent and returns a classic V2 plan (key != "").
    let initiateCount1 = 0;
    server.use(
      http.post("http://localhost:9009/v2/uploads/initiate", () => {
        initiateCount1++;
        return HttpResponse.json(
          { upload_id: "classic-1", key: "tenant/classic-1", part_size: 128, total_parts: 1 },
          { status: 202 }
        );
      }),
      http.post("http://localhost:9009/v2/uploads/classic-1/presign", () => {
        return HttpResponse.json({ number: 1, url: "http://localhost:9009/s3/part-1", size: 128 });
      }),
      http.put("http://localhost:9009/s3/part-1", () => new HttpResponse(null, { status: 200, headers: { ETag: '"e1"' } })),
      http.post("http://localhost:9009/v2/uploads/classic-1/complete", () => HttpResponse.json({ status: "ok" }))
    );
    const s1 = await client.writeStreamWithSummary("/legacy-srv.bin", data, data.length);
    expect(s1.mode).toBe("multipart_v2");
    expect(initiateCount1).toBe(1); // drove the already-initiated session without re-initiating

    server.resetHandlers();

    // Case 2: Existing destination has sticky content_layout="single" -> 409 mismatch -> retries classic V2.
    const seenLayouts2: Array<string | undefined> = [];
    server.use(
      http.post("http://localhost:9009/v2/uploads/initiate", async ({ request }) => {
        const body = (await request.json()) as { layout?: string };
        seenLayouts2.push(body.layout);
        if (body.layout === "extent") {
          return HttpResponse.json(
            { error: "content layout mismatch: the file's layout is sticky for the life of the inode" },
            { status: 409 }
          );
        }
        return HttpResponse.json(
          { upload_id: "classic-2", key: "tenant/classic-2", part_size: 128, total_parts: 1 },
          { status: 202 }
        );
      }),
      http.post("http://localhost:9009/v2/uploads/classic-2/presign", () => {
        return HttpResponse.json({ number: 1, url: "http://localhost:9009/s3/part-2", size: 128 });
      }),
      http.put("http://localhost:9009/s3/part-2", () => new HttpResponse(null, { status: 200, headers: { ETag: '"e2"' } })),
      http.post("http://localhost:9009/v2/uploads/classic-2/complete", () => HttpResponse.json({ status: "ok" }))
    );
    const s2 = await client.writeStreamWithSummary("/existing-single.bin", data, data.length);
    expect(s2.mode).toBe("multipart_v2");
    expect(seenLayouts2).toEqual(["extent", undefined]);

    server.resetHandlers();

    // Case 3: Azure Blob store rejects length-bounded extent presign -> aborts extent upload and falls back to classic V2.
    let abortedExtent = false;
    server.use(
      http.post("http://localhost:9009/v2/uploads/initiate", async ({ request }) => {
        const body = (await request.json()) as { layout?: string };
        if (body.layout === "extent") {
          return HttpResponse.json(
            {
              upload_id: "azure-ext-1",
              key: "",
              part_size: 128,
              total_parts: 1,
              checksum_contract: { supported: ["SHA-256"], required: true },
            },
            { status: 202 }
          );
        }
        return HttpResponse.json(
          { upload_id: "azure-classic-1", key: "tenant/az-1", part_size: 128, total_parts: 1 },
          { status: 202 }
        );
      }),
      http.post("http://localhost:9009/v2/uploads/azure-ext-1/presign-batch", () => {
        return HttpResponse.json(
          { error: "store cannot enforce a declared upload length at write time" },
          { status: 400 }
        );
      }),
      http.post("http://localhost:9009/v2/uploads/azure-ext-1/abort", () => {
        abortedExtent = true;
        return HttpResponse.json({ status: "aborted" });
      }),
      http.post("http://localhost:9009/v2/uploads/azure-classic-1/presign", () => {
        return HttpResponse.json({ number: 1, url: "http://localhost:9009/az/part-1", size: 128 });
      }),
      http.put("http://localhost:9009/az/part-1", () => new HttpResponse(null, { status: 200, headers: { ETag: '"az1"' } })),
      http.post("http://localhost:9009/v2/uploads/azure-classic-1/complete", () => HttpResponse.json({ status: "ok" }))
    );
    const s3 = await client.writeStreamWithSummary("/azure.bin", data, data.length);
    expect(s3.mode).toBe("multipart_v2");
    expect(abortedExtent).toBe(true);
  });

  it("overwrites sticky extent files on small/0B write, writeStream, and Drive9Fs.writeFile", async () => {
    let directPutCount = 0;
    const initiatedSizes: number[] = [];

    server.use(
      http.put("http://localhost:9009/v1/fs/sticky-ext.bin", () => {
        directPutCount++;
        // Matches handleWrite on tidbcloud/fs when rejectExtentLayoutTarget fires.
        return HttpResponse.json({ error: "internal storage error" }, { status: 500 });
      }),
      http.head("http://localhost:9009/v1/fs/", () => {
        return new HttpResponse(null, { status: 200, headers: { "X-Dat9-IsDir": "true", "X-Dat9-Revision": "1" } });
      }),
      http.head("http://localhost:9009/v1/fs/sticky-ext.bin", () => {
        return new HttpResponse(null, {
          status: 200,
          headers: {
            "Content-Length": "4096",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "6",
            "X-Dat9-Content-Layout": "extent",
          },
        });
      }),
      http.post("http://localhost:9009/v2/uploads/initiate", async ({ request }) => {
        const body = (await request.json()) as { total_size: number; layout?: string };
        expect(body.layout).toBe("extent");
        initiatedSizes.push(body.total_size);
        return HttpResponse.json(
          {
            upload_id: `ext-ow-${initiatedSizes.length}`,
            key: "",
            part_size: ExtentBlockSize,
            total_parts: body.total_size === 0 ? 0 : 1,
            checksum_contract: { supported: ["SHA-256"], required: true },
          },
          { status: 202 }
        );
      }),
      http.post("http://localhost:9009/v2/uploads/:id/presign-batch", () => {
        return HttpResponse.json({
          parts: [{ number: 1, url: "http://localhost:9009/obj/ow", size: 5 }],
        });
      }),
      http.put("http://localhost:9009/obj/ow", () => new HttpResponse(null, { status: 200 })),
      http.post("http://localhost:9009/v2/uploads/:id/complete", () => {
        return HttpResponse.json({ status: "ok", revision: 7 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    const rev = await client.writeWithRevision("/sticky-ext.bin", text.encode("small"));
    expect(rev).toBe(7);
    expect(directPutCount).toBe(1);

    // 0-byte small writeStream with targetLayout="extent" skips direct PUT completely.
    const zeroSummary = await client.writeStreamWithSummary("/sticky-ext.bin", new Uint8Array(0), 0, {
      targetLayout: "extent",
    });
    expect(zeroSummary.mode).toBe("extent_v2");
    expect(zeroSummary.total_parts).toBe(0);
    expect(directPutCount).toBe(1); // unchanged!

    // Drive9Fs (just-bash) resolves target via HEAD first and forwards targetLayout="extent" -> zero failed PUTs.
    const bashFs = new Drive9Fs(client);
    await bashFs.writeFile("/sticky-ext.bin", "hello");
    expect(directPutCount).toBe(1); // still unchanged!
    expect(initiatedSizes).toEqual([5, 0, 5]);

    // Explicit invalid layout is rejected upfront.
    await expect(
      client.write("/sticky-ext.bin", text.encode("x"), { layout: "bogus" })
    ).rejects.toThrow(/layout must be "single" or "extent"/);
  });

  it("appends to extent files in O(delta) via PATCH dirty_blocks across chunk boundaries", async () => {
    const baseSize = ExtentChunkSize - 3; // 3 bytes before chunk 0->1 boundary
    const delta = text.encode("ABCDEFG"); // 3 bytes in chunk 0, 4 bytes in chunk 1
    let fullReadCalled = false;
    let patchBody: { new_size: number; dirty_blocks: Array<{ chunk: number; off: number; size: number; checksum: string }>; expected_revision: number } | undefined;
    let completeParts: unknown;

    server.use(
      http.head("http://localhost:9009/v1/fs/ext-log.bin", () => {
        return new HttpResponse(null, {
          status: 200,
          headers: {
            "Content-Length": String(baseSize),
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "11",
            "X-Dat9-Resource-ID": "res-log",
            "X-Dat9-Content-Layout": "extent",
          },
        });
      }),
      http.get("http://localhost:9009/v1/fs/ext-log.bin", () => {
        fullReadCalled = true;
        return HttpResponse.text("unexpected full read", { status: 500 });
      }),
      http.patch("http://localhost:9009/v1/fs/ext-log.bin", async ({ request }) => {
        expect(request.headers.get("x-dat9-expected-resource")).toBe("res-log");
        patchBody = (await request.json()) as typeof patchBody;
        return HttpResponse.json(
          {
            upload_id: "ext-append-1",
            new_size: patchBody!.new_size,
            blocks: patchBody!.dirty_blocks.map((d, idx) => ({
              ...d,
              url: `http://localhost:9009/obj/app-${idx}`,
              exists: false,
            })),
          },
          { status: 202 }
        );
      }),
      http.put("http://localhost:9009/obj/app-0", async ({ request }) => {
        expect(decode(new Uint8Array(await request.arrayBuffer()))).toBe("ABC");
        return new HttpResponse(null, { status: 200 });
      }),
      http.put("http://localhost:9009/obj/app-1", async ({ request }) => {
        expect(decode(new Uint8Array(await request.arrayBuffer()))).toBe("DEFG");
        return new HttpResponse(null, { status: 200 });
      }),
      http.post("http://localhost:9009/v2/uploads/ext-append-1/complete", async ({ request }) => {
        completeParts = await request.json();
        return HttpResponse.json({ status: "ok", revision: 12 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await client.append("/ext-log.bin", delta, { tags: { log: "true" } });
    expect(fullReadCalled).toBe(false);
    expect(patchBody).toEqual({
      new_size: baseSize + delta.length,
      expected_revision: 11,
      dirty_blocks: [
        { chunk: 0, off: ExtentChunkSize - 3, size: 3, checksum: sha256Hex(text.encode("ABC")) },
        { chunk: 1, off: 0, size: 4, checksum: sha256Hex(text.encode("DEFG")) },
      ],
    });
    expect(completeParts).toEqual({
      parts: [
        { number: 1, etag: sha256Hex(text.encode("ABC")), chunk: 0, off: ExtentChunkSize - 3, size: 3 },
        { number: 2, etag: sha256Hex(text.encode("DEFG")), chunk: 1, off: 0, size: 4 },
      ],
      tags: { log: "true" },
    });
  });

  it("supports direct extent APIs and StreamWriter extent fallback", async () => {
    expect(chunkOffset(ExtentChunkSize * 2 + 17)).toEqual({ chunk: 2, off: 17 });

    let patchAttempts = 0;
    let truncateAttempts = 0;
    const swInitiates: Array<string | undefined> = [];

    server.use(
      // Non-extent file on getExtentReadPlan -> ExtentNotServedError
      http.get("http://localhost:9009/v1/fs/classic.bin", () => {
        return new HttpResponse("raw bytes", { status: 200 });
      }),
      // commitExtentPatch with create-once 409 conflict re-plan
      http.patch("http://localhost:9009/v1/fs/patch.bin", async ({ request }) => {
        patchAttempts++;
        const body = (await request.json()) as { new_size: number; dirty_blocks: Array<{ chunk: number; off: number; size: number; checksum: string }> };
        return HttpResponse.json(
          {
            upload_id: `p-${patchAttempts}`,
            new_size: body.new_size,
            blocks: body.dirty_blocks.map((d) => ({
              ...d,
              url: patchAttempts === 1 ? "http://localhost:9009/obj/conflict" : "",
              exists: patchAttempts > 1,
            })),
          },
          { status: 202 }
        );
      }),
      http.put("http://localhost:9009/obj/conflict", () => new HttpResponse("Conflict", { status: 409 })),
      http.post("http://localhost:9009/v2/uploads/p-1/abort", () => HttpResponse.json({ status: "aborted" })),
      http.post("http://localhost:9009/v2/uploads/p-2/complete", () => HttpResponse.json({ status: "ok", revision: 21 })),

      // truncateExtent CAS retry + ResourceMismatchError
      http.head("http://localhost:9009/v1/fs/trunc.bin", () => {
        return new HttpResponse(null, {
          status: 200,
          headers: {
            "Content-Length": "100",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": String(10 + truncateAttempts),
            "X-Dat9-Resource-ID": "res-trunc",
            "X-Dat9-Content-Layout": "extent",
          },
        });
      }),
      http.patch("http://localhost:9009/v1/fs/trunc.bin", ({ request }) => {
        if (request.headers.get("x-dat9-expected-resource") === "wrong-res") {
          return HttpResponse.json({ error: "resource mismatch" }, { status: 412 });
        }
        truncateAttempts++;
        if (truncateAttempts === 1) {
          return HttpResponse.json({ error: "revision mismatch" }, { status: 409 });
        }
        return HttpResponse.json({ upload_id: "trunc-up", new_size: 32, blocks: [] }, { status: 202 });
      }),
      http.post("http://localhost:9009/v2/uploads/trunc-up/complete", () => {
        return HttpResponse.json({ status: "ok", revision: 12 });
      }),

      // createExtentFile + deleteFileConditional
      http.post("http://localhost:9009/v1/fs/zero.bin", ({ request }) => {
        const q = new URL(request.url).searchParams;
        expect(q.get("create")).toBe("1");
        expect(q.get("layout")).toBe("extent");
        return HttpResponse.json({ revision: 1, resource_id: "res-zero", content_layout: "extent" }, { status: 201 });
      }),
      http.delete("http://localhost:9009/v1/fs/zero.bin", ({ request }) => {
        expect(new URL(request.url).searchParams.get("kind")).toBe("file");
        expect(request.headers.get("x-dat9-expected-revision")).toBe("1");
        expect(request.headers.get("x-dat9-expected-resource")).toBe("res-zero");
        return HttpResponse.json({ status: "ok" });
      }),

      // StreamWriter auto-retry on sticky extent target
      http.post("http://localhost:9009/v2/uploads/initiate", async ({ request }) => {
        const body = (await request.json()) as { layout?: string };
        swInitiates.push(body.layout);
        if (!body.layout) {
          return HttpResponse.json({ error: "content layout mismatch" }, { status: 409 });
        }
        return HttpResponse.json(
          {
            upload_id: "sw-ext-1",
            key: "",
            part_size: 4,
            total_parts: 2,
            checksum_contract: { supported: ["SHA-256"], required: true },
          },
          { status: 202 }
        );
      }),
      http.post("http://localhost:9009/v2/uploads/sw-ext-1/presign-batch", async ({ request }) => {
        const body = (await request.json()) as { parts: Array<{ part_number: number }> };
        return HttpResponse.json({
          parts: [{ number: body.parts[0].part_number, url: `http://localhost:9009/obj/sw-${body.parts[0].part_number}`, size: 4 }],
        });
      }),
      http.put("http://localhost:9009/obj/sw-1", () => new HttpResponse(null, { status: 200 })),
      http.put("http://localhost:9009/obj/sw-2", () => new HttpResponse(null, { status: 200 })),
      http.post("http://localhost:9009/v2/uploads/sw-ext-1/complete", () => HttpResponse.json({ status: "ok", revision: 30 }))
    );

    const client = new Client("http://localhost:9009", "test-key");
    await expect(client.getExtentReadPlan("/classic.bin", 0, 10)).rejects.toBeInstanceOf(ExtentNotServedError);

    const rev = await client.commitExtentPatch("/patch.bin", 4, [{ chunk: 0, off: 0, data: text.encode("DATA") }], 5);
    expect(rev).toBe(21);
    expect(patchAttempts).toBe(2);

    const st = await client.truncateExtent("/trunc.bin", 32);
    expect(st.size).toBe(32);
    expect(st.revision).toBe(12);
    expect(truncateAttempts).toBe(2);

    await expect(
      client.truncateExtent("/trunc.bin", 0, { expectedRevision: 12, expectedResourceId: "wrong-res" })
    ).rejects.toBeInstanceOf(ResourceMismatchError);

    const created = await client.createExtentFile("/zero.bin");
    expect(created).toEqual({ revision: 1, resource_id: "res-zero", native_extent: true });
    await client.deleteFileConditional("/zero.bin", created.revision, created.resource_id);

    const sw = client.newStreamWriter("/sw-target.bin", 8);
    await sw.writePart(1, text.encode("1234"));
    await sw.complete(2, text.encode("5678"));
    expect(swInitiates).toEqual([undefined, "extent"]);

    // Verify unused imports / error types remain constructible.
    expect(new ConflictError("conflict")).toBeInstanceOf(Drive9Error);
  });
});
