import { http, HttpResponse } from "msw";

interface Node {
  directory: boolean;
  data: Uint8Array;
  mode: number;
  revision: number;
  id: string;
}

/**
 * Small HTTP fixture for the public Drive9 contract, deliberately retaining its
 * differences from a POSIX filesystem: raw symlink reads, automatic parents and
 * aliasing server-side copies. Tests exercise the real SDK across this boundary.
 */
export function drive9HTTP() {
  const base = "http://drive9.test";
  const nodes = new Map<string, Node>();
  const calls: Array<{ method: string; path: string; query: string }> = [];
  let nextID = 0;
  const node = (directory: boolean, data = new Uint8Array(), mode = directory ? 0o755 : 0o644): Node =>
    ({ directory, data, mode, revision: 1, id: String(++nextID) });
  nodes.set("/", node(true));

  function parents(path: string) {
    const parts = path.split("/").filter(Boolean);
    for (let i = 1; i < parts.length; i++) {
      const parent = "/" + parts.slice(0, i).join("/");
      if (!nodes.has(parent)) nodes.set(parent, node(true));
    }
  }
  function put(path: string, data: string | Uint8Array, mode = 0o644) {
    parents(path);
    nodes.set(path, node(false, typeof data === "string" ? new TextEncoder().encode(data) : data, mode));
  }
  const error = (status: number, message: string) => HttpResponse.json({ error: message }, { status });
  const source = (request: Request, name: string) => {
    const value = request.headers.get(name)!;
    return request.headers.get("X-Dat9-Path-Encoding") === "base64url"
      ? Buffer.from(value, "base64url").toString("utf8") : value;
  };

  const handlers = [
    http.get(`${base}/v1/status`, () => HttpResponse.json({
      inline_threshold: 1024 * 1024,
      max_upload_bytes: 64 * 1024 * 1024,
    })),
    http.all(`${base}/v1/fs/*`, async ({ request }) => {
      const url = new URL(request.url);
      const path = decodeURIComponent(url.pathname.slice("/v1/fs".length)).replace(/\/+$/, "") || "/";
      const query = url.searchParams;
      calls.push({ method: request.method, path, query: url.search });
      if (request.headers.get("Authorization") !== "Bearer test-key") return error(401, "unauthorized");
      const entry = nodes.get(path);
      if (request.method === "HEAD") {
        if (!entry) return new HttpResponse(null, { status: 404 });
        return new HttpResponse(null, { headers: {
          "Content-Length": String(entry.data.length),
          "X-Dat9-IsDir": String(entry.directory),
          "X-Dat9-Mode": String(entry.mode),
          "X-Dat9-Revision": String(entry.revision),
          "X-Dat9-Mtime": "1700000000",
          "X-Dat9-Resource-ID": entry.id,
        } });
      }
      if (request.method === "GET") {
        if (!entry) return error(404, "not found");
        if (query.has("list")) {
          if (!entry.directory) return error(400, "not a directory");
          const prefix = path === "/" ? "/" : path + "/";
          const entries = [...nodes].filter(([key]) => key.startsWith(prefix) && key !== path && !key.slice(prefix.length).includes("/"))
            .map(([key, value]) => ({
              name: key.slice(prefix.length), isDir: value.directory, size: value.data.length,
              mode: value.mode, hasMode: true, mtime: 1700000000,
            }));
          return HttpResponse.json({ entries });
        }
        if (entry.directory) return error(400, "is a directory");
        return HttpResponse.arrayBuffer(Uint8Array.from(entry.data).buffer);
      }
      if (request.method === "PUT") {
        const expected = request.headers.get("X-Dat9-Expected-Revision");
        if (expected !== null && Number(expected) !== (entry?.revision ?? 0)) {
          return HttpResponse.json({ error: "revision mismatch", server_revision: entry?.revision ?? 0 }, { status: 409 });
        }
        const data = new Uint8Array(await request.arrayBuffer());
        if (entry) {
          entry.data = data;
          entry.revision++;
        } else put(path, data);
        return HttpResponse.json({ revision: nodes.get(path)!.revision });
      }
      if (request.method === "DELETE") {
        if (!entry) return error(404, "not found");
        const children = [...nodes.keys()].filter((key) => key.startsWith(path + "/"));
        if (children.length && !query.has("recursive")) return error(409, "directory not empty");
        for (const child of children) nodes.delete(child);
        nodes.delete(path);
        return HttpResponse.json({ status: "ok" });
      }
      if (request.method === "POST") {
        if (query.has("append")) return error(400, "file is not s3-stored");
        if (query.has("mkdir")) {
          if (entry) return error(409, "path conflict");
          parents(path);
          nodes.set(path, node(true));
        } else if (query.has("symlink")) {
          if (entry) return error(409, "path conflict");
          const { target } = await request.json() as { target: string };
          put(path, target, 0o120777);
        } else if (query.has("chmod")) {
          if (!entry) return error(404, "not found");
          const { mode } = await request.json() as { mode: number };
          entry.mode = mode;
        } else if (query.has("rename")) {
          const src = source(request, "X-Dat9-Rename-Source").replace(/\/+$/, "");
          const original = nodes.get(src);
          if (!original) return error(404, "not found");
          parents(path);
          for (const [key, value] of [...nodes]) {
            if (key === src || key.startsWith(src + "/")) {
              nodes.set(path + key.slice(src.length), value);
              nodes.delete(key);
            }
          }
        } else if (query.has("hardlink") || query.has("copy")) {
          if (entry) return error(409, "path conflict");
          const src = source(request, query.has("copy") ? "X-Dat9-Copy-Source" : "X-Dat9-Hardlink-Source");
          const original = nodes.get(src);
          if (!original) return error(404, "not found");
          parents(path);
          nodes.set(path, original);
        } else return error(400, "unsupported operation");
        return HttpResponse.json({ status: "ok" });
      }
      return error(405, "unsupported method");
    }),
  ];
  return { base, nodes, calls, put, handlers };
}
