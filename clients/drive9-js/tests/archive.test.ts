import { describe, expect, it } from "vitest";
import { Client } from "../src/index.js";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import * as zlib from "node:zlib";

const server = setupServer();
server.listen({ onUnhandledRequest: "error" });

interface MockFile {
  path: string;
  body: string;
}

// Seed a mock remote tree from a flat map of {path: body}. Returns the per-dir
// listings to wire into the list endpoint.
function buildTree(files: MockFile[]): { dirs: Record<string, { name: string; size: number; isDir: boolean }[]>; bodies: Record<string, string> } {
  const dirs: Record<string, { name: string; size: number; isDir: boolean }[]> = {};
  const bodies: Record<string, string> = {};
  const dirSeen: Record<string, Set<string>> = {};
  for (const f of files) {
    bodies[f.path] = f.body;
    let dir = "/";
    const rel = f.path.replace(/^\/+/, "");
    const segs = rel.split("/");
    for (let i = 0; i < segs.length; i++) {
      const seg = segs[i];
      const isLast = i === segs.length - 1;
      if (!dirSeen[dir]) dirSeen[dir] = new Set();
      if (!dirSeen[dir].has(seg)) {
        dirSeen[dir].add(seg);
        dirs[dir] = dirs[dir] || [];
        dirs[dir].push({ name: seg, size: isLast ? f.body.length : 0, isDir: !isLast });
      }
      if (isLast) break;
      const child = dir === "/" ? `/${seg}` : `${dir}/${seg}`;
      if (!dirs[child]) dirs[child] = [];
      dir = child;
    }
  }
  return { dirs, bodies };
}

function mountTree(root: string, files: MockFile[]) {
  const { dirs, bodies } = buildTree(files.map((f) => ({ path: f.path, body: f.body })));
  server.use(
    http.get(`http://localhost:9009/v1/fs${root === "/" ? "" : root}`, ({ request }) => {
      const url = new URL(request.url);
      if (!url.searchParams.has("list")) {
        // GET file content
        const body = bodies[root];
        if (body === undefined) return HttpResponse.json({}, { status: 404 });
        return HttpResponse.text(body);
      }
      const entries = dirs[root] || [];
      return HttpResponse.json({ entries });
    }),
    http.get(/http:\/\/localhost:9009\/v1\/fs\/.+/, ({ request }) => {
      const url = new URL(request.url);
      const p = url.pathname.replace("/v1/fs", "");
      if (url.searchParams.has("list")) {
        const entries = dirs[p] || [];
        return HttpResponse.json({ entries });
      }
      const body = bodies[p];
      if (body === undefined) return HttpResponse.json({}, { status: 404 });
      return HttpResponse.text(body);
    })
  );
}

// Minimal ustar reader: returns entry names (dirs get trailing slash).
function readTarGz(buf: Buffer): string[] {
  const decompressed = zlib.gunzipSync(buf);
  const names: string[] = [];
  let offset = 0;
  while (offset + 512 <= decompressed.length) {
    const header = decompressed.subarray(offset, offset + 512);
    // All-zero block → EOF.
    let allZero = true;
    for (let i = 0; i < 512; i++) {
      if (header[i] !== 0) { allZero = false; break; }
    }
    if (allZero) break;
    const name = header.subarray(0, 100).toString("utf8").replace(/\0+$/, "");
    const typeflag = String.fromCharCode(header[156]);
    const sizeStr = header.subarray(124, 136).toString("utf8").replace(/\0+$/, "").trim();
    const size = parseInt(sizeStr, 8) || 0;
    const entryName = typeflag === "5" ? name.replace(/\/?$/, "/") : name;
    names.push(entryName);
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  return names.sort();
}

async function streamToBuffer(stream: ReadableStream<Uint8Array>): Promise<Buffer> {
  const reader = stream.getReader();
  const chunks: Buffer[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks);
}

describe("archive", () => {
  it("produces a tar.gz with the remote tree", async () => {
    mountTree("/proj", [
      { path: "/proj/README.md", body: "hello world\n" },
      { path: "/proj/src/main.go", body: "package main\n" },
      { path: "/proj/src/util/util.go", body: "package util\n" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.archive("/proj");
    const buf = await streamToBuffer(stream);
    const names = readTarGz(buf);
    expect(names).toContain("proj/");
    expect(names).toContain("proj/README.md");
    expect(names).toContain("proj/src/main.go");
    expect(names).toContain("proj/src/util/util.go");
  });

  it("excludes node_modules via --exclude", async () => {
    mountTree("/proj", [
      { path: "/proj/src/app.go", body: "package src\n" },
      { path: "/proj/node_modules/react/index.js", body: "module.exports\n" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.archive("/proj", { exclude: ["**/node_modules/**"] });
    const names = readTarGz(await streamToBuffer(stream));
    for (const n of names) {
      expect(n.includes("node_modules")).toBe(false);
    }
    expect(names).toContain("proj/src/app.go");
  });

  it("coding-agent profile skips default excludes", async () => {
    mountTree("/proj", [
      { path: "/proj/main.go", body: "package main\n" },
      { path: "/proj/dist/bundle.js", body: "bundle\n" },
      { path: "/proj/node_modules/react/x.js", body: "x\n" },
      { path: "/proj/.git/HEAD", body: "ref: main\n" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.archive("/proj", { profile: "coding-agent" });
    const names = readTarGz(await streamToBuffer(stream));
    expect(names).toContain("proj/main.go");
    for (const n of names) {
      expect(n.includes("node_modules") || n.includes(".git/") || n.includes("dist/")).toBe(false);
    }
  });

  it("include whitelist keeps only matching paths", async () => {
    mountTree("/proj", [
      { path: "/proj/src/app.go", body: "package src\n" },
      { path: "/proj/docs/guide.md", body: "# guide\n" },
      { path: "/proj/README.md", body: "# readme\n" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.archive("/proj", { include: ["docs/**", "README.md"] });
    const names = readTarGz(await streamToBuffer(stream));
    expect(names).toContain("proj/docs/guide.md");
    expect(names).toContain("proj/README.md");
    for (const n of names) {
      expect(n.endsWith(".go")).toBe(false);
    }
  });

  it("flat mode strips hierarchy", async () => {
    mountTree("/proj", [
      { path: "/proj/src/deep/nested/a.go", body: "package nested\n" },
      { path: "/proj/b.go", body: "package main\n" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.archive("/proj", { flat: true });
    const names = readTarGz(await streamToBuffer(stream));
    for (const n of names) {
      expect(n.endsWith("/")).toBe(false); // no dir entries
      expect(n.includes("/")).toBe(false); // basenames only
    }
    expect(names).toContain("a.go");
    expect(names).toContain("b.go");
  });

  it("zip format via archiveToFile", async () => {
    mountTree("/proj", [
      { path: "/proj/a.txt", body: "AAA" },
      { path: "/proj/b/c.txt", body: "CCC" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    const out = `/tmp/drive9-ts-zip-${Date.now()}.zip`;
    await client.archiveToFile("/proj", out, { format: "zip" });
    const fs = await import("node:fs");
    const data = fs.readFileSync(out);
    // Validate the zip is well-formed and has the expected entries.
    const { execSync } = await import("node:child_process");
    const list = execSync(`python3 -c 'import zipfile,sys; print("\\n".join(sorted(zipfile.ZipFile(sys.argv[1]).namelist())))' ${out}`, { encoding: "utf8" });
    expect(list).toContain("proj/a.txt");
    expect(list).toContain("proj/b/c.txt");
    // testzip returns None when valid.
    const valid = execSync(`python3 -c 'import zipfile,sys; print("valid" if zipfile.ZipFile(sys.argv[1]).testzip() is None else "corrupt")' ${out}`, { encoding: "utf8" }).trim();
    expect(valid).toBe("valid");
    fs.unlinkSync(out);
  });

  it("flat mode rejects duplicate basenames", async () => {
    mountTree("/proj", [
      { path: "/proj/src/config.json", body: "{}\n" },
      { path: "/proj/test/config.json", body: "{}\n" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    await expect(client.archive("/proj", { flat: true })).rejects.toThrow(/collision/);
  });

  it("include nested file does not prune parent directory", async () => {
    // Reproduces B2: --include "src/app.go" must walk into src/ even though
    // "src" itself does not match the include pattern, otherwise the leaf is
    // never visited and the archive is empty.
    mountTree("/proj", [
      { path: "/proj/src/app.go", body: "package main\n" },
      { path: "/proj/src/util/util.go", body: "package util\n" },
      { path: "/proj/other/notes.txt", body: "notes\n" },
    ]);
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.archive("/proj", { include: ["src/app.go"] });
    const names = readTarGz(await streamToBuffer(stream));
    expect(names).toContain("proj/src/app.go");
    expect(names.some((n) => n.endsWith("notes.txt"))).toBe(false);
  });

  it("long entry names use a PAX header instead of truncating", async () => {
    // A path > 100 bytes must survive via a PAX 'x' extended header. Use
    // python's tarfile to parse since it honors PAX records (the minimal
    // ustar reader in this test file does not).
    const longDir =
      "/proj/very/deeply/nested/path/that/definitely/exceeds/the/ustar/one/hundred/byte/name/field/limit";
    const longFile = `${longDir}/module.ts`;
    mountTree("/proj", [{ path: longFile, body: "export {}\n" }]);
    const client = new Client("http://localhost:9009", "test-key");
    const stream = await client.archive("/proj");
    const buf = await streamToBuffer(stream);
    const fs = await import("node:fs");
    const tmp = `/tmp/drive9-ts-longname-${Date.now()}.tar.gz`;
    fs.writeFileSync(tmp, buf);
    try {
      const { execSync } = await import("node:child_process");
      const list = execSync(
        `python3 -c 'import tarfile,sys; [print(m.name) for m in tarfile.open(sys.argv[1],"r:gz")]' ${tmp}`,
        { encoding: "utf8" }
      );
      // The full long path must appear (archiveName prepends the archive root
      // "proj/", and longFile already starts with "/proj/...", so the in-archive
      // name is longFile with its leading "/" stripped).
      const expectedName = longFile.slice(1);
      expect(list).toContain(expectedName);
      // Sanity: the name is genuinely > 100 bytes so the PAX path was needed.
      expect(expectedName.length).toBeGreaterThan(100);
    } finally {
      fs.unlinkSync(tmp);
    }
  });
});