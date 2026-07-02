import { describe, expect, it } from "vitest";
import { Client, Drive9Error } from "../src/index.js";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

const server = setupServer();
server.listen({ onUnhandledRequest: "error" });

const BASE = "http://localhost:9009";

// statHandler returns a HEAD response with isDir set.
function statHandler(isDir: boolean): ReturnType<typeof http.head> {
  return () =>
    new HttpResponse(null, {
      status: 200,
      headers: {
        "Content-Length": "0",
        "X-Dat9-IsDir": isDir ? "true" : "false",
      },
    });
}

// fileBodyHandler returns file bytes for a GET on a file path.
function fileBodyHandler(body: string): ReturnType<typeof http.get> {
  return () => HttpResponse.arrayBuffer(new TextEncoder().encode(body));
}

describe("downloadDir", () => {
  it("downloads a nested tree with files and empty dirs", async () => {
    // Remote tree:
    //   /tree/
    //     a.txt      "alpha"
    //     sub/
    //       b.txt    "bravo"
    //     empty/
    server.use(
      http.head(`${BASE}/v1/fs/tree`, statHandler(true)),
      http.get(`${BASE}/v1/fs/tree`, ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("list") === "1") {
          return HttpResponse.json({
            entries: [
              { name: "a.txt", size: 5, isDir: false },
              { name: "sub", size: 0, isDir: true },
              { name: "empty", size: 0, isDir: true },
            ],
          });
        }
        return HttpResponse.arrayBuffer(new TextEncoder().encode("alpha"));
      }),
      http.get(`${BASE}/v1/fs/tree/sub`, ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("list") === "1") {
          return HttpResponse.json({
            entries: [{ name: "b.txt", size: 5, isDir: false }],
          });
        }
        return HttpResponse.arrayBuffer(new TextEncoder().encode("bravo"));
      }),
      http.get(`${BASE}/v1/fs/tree/empty`, () =>
        HttpResponse.json({ entries: [] })
      ),
      http.get(`${BASE}/v1/fs/tree/a.txt`, fileBodyHandler("alpha")),
      http.get(`${BASE}/v1/fs/tree/sub/b.txt`, fileBodyHandler("bravo"))
    );

    const client = new Client(BASE, "test-key");
    const dst = fs.mkdtempSync(path.join(os.tmpdir(), "d9-dl-"));
    await client.downloadDir("/tree", dst);

    expect(fs.readFileSync(path.join(dst, "a.txt"), "utf8")).toBe("alpha");
    expect(fs.readFileSync(path.join(dst, "sub", "b.txt"), "utf8")).toBe("bravo");
    expect(fs.statSync(path.join(dst, "empty")).isDirectory()).toBe(true);
  });

  it("rejects a file source", async () => {
    server.use(
      http.head(`${BASE}/v1/fs/file.txt`, statHandler(false))
    );

    const client = new Client(BASE, "test-key");
    const dst = fs.mkdtempSync(path.join(os.tmpdir(), "d9-dl-"));
    await expect(client.downloadDir("/file.txt", dst)).rejects.toThrow(
      /is a file/
    );
  });

  it("rejects when dst exists as a file", async () => {
    server.use(
      http.head(`${BASE}/v1/fs/srcdir`, statHandler(true)),
      http.get(`${BASE}/v1/fs/srcdir`, ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("list") === "1") {
          return HttpResponse.json({ entries: [] });
        }
        return new HttpResponse(null, { status: 404 });
      })
    );

    const client = new Client(BASE, "test-key");
    const dst = fs.mkdtempSync(path.join(os.tmpdir(), "d9-dl-"));
    const dstFile = path.join(dst, "target");
    fs.writeFileSync(dstFile, "preexisting");

    await expect(client.downloadDir("/srcdir", dstFile)).rejects.toThrow(
      /not a directory/
    );
  });

  it("rejects when a descendant file already exists", async () => {
    server.use(
      http.head(`${BASE}/v1/fs/conflict`, statHandler(true)),
      http.get(`${BASE}/v1/fs/conflict`, ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("list") === "1") {
          return HttpResponse.json({
            entries: [{ name: "a.txt", size: 5, isDir: false }],
          });
        }
        return new HttpResponse(null, { status: 404 });
      })
    );

    const client = new Client(BASE, "test-key");
    const dst = fs.mkdtempSync(path.join(os.tmpdir(), "d9-dl-"));
    const localFile = path.join(dst, "a.txt");
    fs.writeFileSync(localFile, "local-content");

    await expect(client.downloadDir("/conflict", dst)).rejects.toThrow(
      /already exists/
    );

    // Pre-existing file must NOT have been truncated.
    expect(fs.readFileSync(localFile, "utf8")).toBe("local-content");
  });

  it("downloads into a pre-existing empty local directory", async () => {
    server.use(
      http.head(`${BASE}/v1/fs/merge`, statHandler(true)),
      http.get(`${BASE}/v1/fs/merge`, ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("list") === "1") {
          return HttpResponse.json({
            entries: [{ name: "x.txt", size: 1, isDir: false }],
          });
        }
        return new HttpResponse(null, { status: 404 });
      }),
      http.get(`${BASE}/v1/fs/merge/x.txt`, fileBodyHandler("x"))
    );

    const client = new Client(BASE, "test-key");
    const dst = fs.mkdtempSync(path.join(os.tmpdir(), "d9-dl-"));
    await client.downloadDir("/merge", dst);

    expect(fs.readFileSync(path.join(dst, "x.txt"), "utf8")).toBe("x");
  });
});