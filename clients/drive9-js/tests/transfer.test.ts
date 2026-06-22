import { describe, expect, it } from "vitest";
import { Client } from "../src/index.js";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { createHash } from "node:crypto";

const server = setupServer();
server.listen({ onUnhandledRequest: "error" });

describe("Transfer", () => {
  it("writeStream falls back to PUT for small files", async () => {
    let putCalled = false;
    server.use(
      http.get("http://localhost:9009/v1/status", () => HttpResponse.json({ inline_threshold: 50000 })),
      http.put("http://localhost:9009/v1/fs/small.bin", () => {
        putCalled = true;
        return HttpResponse.text("ok");
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    await client.warm();
    const data = new TextEncoder().encode("tiny");
    await client.writeStream("/small.bin", data, data.length);
    expect(putCalled).toBe(true);
  });

  it("writeStream preserves direct PUT for cold small uploads", async () => {
    let putCalled = false;
    let initiateCalled = false;
    server.use(
      http.put("http://localhost:9009/v1/fs/cold-small.bin", async ({ request }) => {
        putCalled = true;
        expect(await request.text()).toBe("tiny");
        return HttpResponse.text("ok");
      }),
      http.post("http://localhost:9009/v2/uploads/initiate", () => {
        initiateCalled = true;
        return HttpResponse.text("unexpected multipart initiate", { status: 500 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    const summary = await client.writeStreamWithSummary("/cold-small.bin", new TextEncoder().encode("tiny"), 4);
    expect(summary.mode).toBe("direct_put");
    expect(putCalled).toBe(true);
    expect(initiateCalled).toBe(false);
  });

  it("readStream handles 302 redirect", async () => {
    server.use(
      http.get("http://localhost:9009/v1/fs/large.bin", ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.has("follow")) {
          return HttpResponse.arrayBuffer(new TextEncoder().encode("s3data"));
        }
        return HttpResponse.json({}, { status: 302, headers: { Location: "http://localhost:9009/v1/fs/large.bin?follow=1" } });
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.readStream("/large.bin");
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    const all = new Uint8Array(chunks.reduce((a, b) => a + b.length, 0));
    let offset = 0;
    for (const c of chunks) {
      all.set(c, offset);
      offset += c.length;
    }
    expect(new TextDecoder().decode(all)).toBe("s3data");
  });

  it("readStreamRange returns sliced stream for small files", async () => {
    server.use(
      http.get("http://localhost:9009/v1/fs/small.txt", () => {
        return HttpResponse.arrayBuffer(new TextEncoder().encode("hello world"));
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.readStreamRange("/small.txt", 6, 5);
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    const all = new Uint8Array(chunks.reduce((a, b) => a + b.length, 0));
    let offset = 0;
    for (const c of chunks) {
      all.set(c, offset);
      offset += c.length;
    }
    expect(new TextDecoder().decode(all)).toBe("world");
  });

  it("patchFile only sends checksum when it is presigned", async () => {
    let firstChecksum: string | null = null;
    let secondChecksum: string | null = null;
    let completeCalled = false;
    server.use(
      http.patch("http://localhost:9009/v1/fs/file.bin", () => {
        return HttpResponse.json(
          {
            upload_id: "patch-js",
            part_size: 8,
            upload_parts: [
              { number: 1, url: "http://localhost:9009/patch/1", size: 8, headers: {} },
              {
                number: 2,
                url: "http://localhost:9009/patch/2",
                size: 8,
                headers: { "x-amz-checksum-sha256": "placeholder" },
              },
            ],
            copied_parts: [],
          },
          { status: 202 }
        );
      }),
      http.put("http://localhost:9009/patch/1", ({ request }) => {
        firstChecksum = request.headers.get("x-amz-checksum-sha256");
        return HttpResponse.text("ok");
      }),
      http.put("http://localhost:9009/patch/2", ({ request }) => {
        secondChecksum = request.headers.get("x-amz-checksum-sha256");
        return HttpResponse.text("ok");
      }),
      http.post("http://localhost:9009/v1/uploads/patch-js/complete", () => {
        completeCalled = true;
        return HttpResponse.text("ok");
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await client.patchFile("/file.bin", 16, [1, 2], (part) => new TextEncoder().encode(`part-${part}`), undefined, 8);

    const expectedSecond = createHash("sha256").update("part-2").digest("base64");
    expect(firstChecksum).toBeNull();
    expect(secondChecksum).toBe(expectedSecond);
    expect(completeCalled).toBe(true);
  });

  it("resumeUpload slices missing parts with the server part size", async () => {
    let uploaded = "";
    let completeCalled = false;
    server.use(
      http.get("http://localhost:9009/v1/uploads", ({ request }) => {
        const url = new URL(request.url);
        expect(url.searchParams.get("path")).toBe("/resume.bin");
        expect(url.searchParams.get("status")).toBe("UPLOADING");
        return HttpResponse.json({
          uploads: [
            {
              upload_id: "resume-js",
              parts_total: 2,
              status: "UPLOADING",
              expires_at: new Date(Date.now() + 60000).toISOString(),
            },
          ],
        });
      }),
      http.post("http://localhost:9009/v1/uploads/resume-js/resume", () => {
        return HttpResponse.json({
          upload_id: "resume-js",
          part_size: 8,
          parts: [{ number: 2, url: "http://localhost:9009/resume/2", size: 7, headers: {} }],
        });
      }),
      http.put("http://localhost:9009/resume/2", async ({ request }) => {
        uploaded = new TextDecoder().decode(await request.arrayBuffer());
        return HttpResponse.text("ok");
      }),
      http.post("http://localhost:9009/v1/uploads/resume-js/complete", () => {
        completeCalled = true;
        return HttpResponse.text("ok");
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await client.resumeUpload("/resume.bin", new TextEncoder().encode("abcdefghijklmno"));

    expect(uploaded).toBe("ijklmno");
    expect(completeCalled).toBe(true);
  });
});
