import { describe, expect, it } from "vitest";
import { Client } from "../src/index.js";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";

const server = setupServer();
server.listen({ onUnhandledRequest: "error" });

describe("Transfer", () => {
  it("writeStream falls back to PUT for small files", async () => {
    let putCalled = false;
    server.use(
      http.put("http://localhost:9009/v1/fs/small.bin", () => {
        putCalled = true;
        return HttpResponse.text("ok");
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    const data = new TextEncoder().encode("tiny");
    await client.writeStream("/small.bin", data, data.length);
    expect(putCalled).toBe(true);
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
});
