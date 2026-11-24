import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";

import {
  Client,
  ConflictError,
  InvalidLayerViewRequestError,
  LAYER_FENCE_HEADER,
  MaxBatchReadSmallPaths,
  MaxBatchStatPaths,
  MaxLayerViewBatchWriteItems,
  UnsupportedInLayerViewError,
} from "../src/index.js";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const text = new TextEncoder();

describe("TypeScript SDK parity surface", () => {
  it("batch writes preserve explicit modes and reject unsupported tags", async () => {
    const c = new Client("http://localhost:9009", "key");
    server.use(http.post("http://localhost:9009/v1/fs\\:batch-write", async ({ request }) => {
      const body = await request.json() as { items: Array<{ path: string; hasMode?: boolean; mode?: number }> };
      expect(body.items.map(({ mode, hasMode }) => [mode, hasMode])).toEqual([[0, true], [0o600, true], [undefined, undefined]]);
      return HttpResponse.json({ results: body.items.map(({ path }) => ({ path, status: 200 })) });
    }));
    const data = new Uint8Array(1);
    expect(await c.batchWrite([
      { path: "/zero", data, mode: 0 },
      { path: "/private", data, mode: 0o600 },
      { path: "/inherit", data },
    ])).toHaveLength(3);
    await expect(c.batchWrite([{ path: "/tagged", data, tags: { env: "test" } }])).rejects.toThrow(/does not support tags/);
  });

  it("resolves relative object locations from the view URL under a deployment prefix", async () => {
    const layerId = "layer_01j8z6kh2m9q7w3r5t6y8v0p1a";
    const c = new Client("http://localhost:9009/deploy", "key").withView({ layerId });
    server.use(
      http.get(`http://localhost:9009/deploy/v1/views/${layerId}/v1/fs/blob`, () =>
        new HttpResponse(null, { status: 307, headers: { Location: "../../../../../s3/blob" } })),
      http.get("http://localhost:9009/deploy/s3/blob", ({ request }) => request.headers.has("range")
        ? new HttpResponse("b", { status: 206 }) : new HttpResponse("abc"))
    );
    expect(new TextDecoder().decode(await c.read("/blob"))).toBe("abc");
    expect(await new Response(await c.readStreamRange("/blob", 1, 1)).text()).toBe("b");
  });

  it("loads the current context from DRIVE9_CONFIG and treats missing type as owner", () => {
    const dir = mkdtempSync(join(tmpdir(), "drive9-js-config-"));
    const configPath = join(dir, "config.json");
    writeFileSync(
      configPath,
      JSON.stringify({
        server: "https://fallback.example",
        current_context: "default",
        contexts: {
          default: { api_key: "owner-key" },
          usable: { type: "fs_scoped", api_key: "scoped-key" },
        },
      })
    );

    const prev = {
      config: process.env.DRIVE9_CONFIG,
      server: process.env.DRIVE9_SERVER,
      base: process.env.DRIVE9_BASE,
      key: process.env.DRIVE9_API_KEY,
    };
    process.env.DRIVE9_CONFIG = configPath;
    delete process.env.DRIVE9_SERVER;
    delete process.env.DRIVE9_BASE;
    delete process.env.DRIVE9_API_KEY;
    try {
      const fromConfig = Client.defaultClient();
      expect(fromConfig.baseUrl).toBe("https://fallback.example");
      expect(fromConfig.apiKey).toBe("owner-key");

      process.env.DRIVE9_BASE = "https://env.example";
      process.env.DRIVE9_API_KEY = "env-key";
      const fromEnv = Client.defaultClient();
      expect(fromEnv.baseUrl).toBe("https://env.example");
      expect(fromEnv.apiKey).toBe("env-key");
    } finally {
      restoreEnv("DRIVE9_CONFIG", prev.config);
      restoreEnv("DRIVE9_SERVER", prev.server);
      restoreEnv("DRIVE9_BASE", prev.base);
      restoreEnv("DRIVE9_API_KEY", prev.key);
    }
  });

  it("does not fall back to a different context when current_context is unusable", () => {
    const dir = mkdtempSync(join(tmpdir(), "drive9-js-config-"));
    const configPath = join(dir, "config.json");
    writeFileSync(
      configPath,
      JSON.stringify({
        server: "https://fallback.example",
        current_context: "delegated",
        contexts: {
          delegated: { type: "delegated", server: "https://delegated.example", token: "jwt" },
          usable: { type: "fs_scoped", api_key: "scoped-key" },
        },
      })
    );

    const prev = {
      config: process.env.DRIVE9_CONFIG,
      server: process.env.DRIVE9_SERVER,
      base: process.env.DRIVE9_BASE,
      key: process.env.DRIVE9_API_KEY,
    };
    process.env.DRIVE9_CONFIG = configPath;
    delete process.env.DRIVE9_SERVER;
    delete process.env.DRIVE9_BASE;
    delete process.env.DRIVE9_API_KEY;
    try {
      const client = Client.defaultClient();
      expect(client.baseUrl).toBe("https://fallback.example");
      expect(client.apiKey).toBeUndefined();
    } finally {
      restoreEnv("DRIVE9_CONFIG", prev.config);
      restoreEnv("DRIVE9_SERVER", prev.server);
      restoreEnv("DRIVE9_BASE", prev.base);
      restoreEnv("DRIVE9_API_KEY", prev.key);
    }
  });

  it("sends write options and returns committed revisions", async () => {
    server.use(
      http.put("http://localhost:9009/v1/fs/with-meta.txt", async ({ request }) => {
        expect(request.headers.get("x-dat9-expected-revision")).toBe("3");
        expect(request.headers.get("x-dat9-description")).toBe("ts sdk note");
        const tagHeader = request.headers.get("x-dat9-tag") || "";
        expect(tagHeader).toContain("kind=note");
        expect(tagHeader).toContain("owner=agent");
        expect(await request.text()).toBe("hello");
        return HttpResponse.json({ revision: 4 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    const revision = await client.writeWithRevision("/with-meta.txt", text.encode("hello"), {
      expectedRevision: 3,
      description: "ts sdk note",
      tags: { kind: "note", owner: "agent" },
    });
    expect(revision).toBe(4);
  });

  it("append uses the native append plan for existing files", async () => {
    let fullReadCalled = false;
    let rewritePutCalled = false;
    let uploaded = "";
    let completeCalled = false;
    server.use(
      http.head("http://localhost:9009/v1/fs/log.txt", () =>
        new HttpResponse(null, {
          headers: {
            "Content-Length": "10",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "8",
          },
        })
      ),
      http.post("http://localhost:9009/v1/fs/log.txt", async ({ request }) => {
        expect(new URL(request.url).searchParams.has("append")).toBe(true);
        expect(await request.json()).toMatchObject({
          append_size: 6,
          part_size: 8 * 1024 * 1024,
          expected_revision: 8,
        });
        return HttpResponse.json(
          {
            base_size: 10,
            upload_id: "append-123",
            part_size: 8,
            upload_parts: [
              {
                number: 2,
                url: "http://localhost:9009/s3/append/2",
                size: 8,
                headers: { "X-Upload-Token": "append" },
                read_url: "http://localhost:9009/s3/read/2",
                read_headers: { Range: "bytes=8-9" },
              },
            ],
            copied_parts: [1],
          },
          { status: 202 }
        );
      }),
      http.get("http://localhost:9009/s3/read/2", ({ request }) => {
        expect(request.headers.get("range")).toBe("bytes=8-9");
        return HttpResponse.arrayBuffer(text.encode("ij"));
      }),
      http.put("http://localhost:9009/s3/append/2", async ({ request }) => {
        expect(request.headers.get("x-upload-token")).toBe("append");
        uploaded = new TextDecoder().decode(await request.arrayBuffer());
        return HttpResponse.text("ok");
      }),
      http.post("http://localhost:9009/v1/uploads/append-123/complete", () => {
        completeCalled = true;
        return HttpResponse.json({ status: "ok" });
      }),
      http.get("http://localhost:9009/v1/fs/log.txt", () => {
        fullReadCalled = true;
        return HttpResponse.text("unexpected full read", { status: 500 });
      }),
      http.put("http://localhost:9009/v1/fs/log.txt", async ({ request }) => {
        rewritePutCalled = true;
        return HttpResponse.text("unexpected rewrite", { status: 500 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await client.append("/log.txt", text.encode("KLMNOP"));
    expect(uploaded).toBe("ijKLMNOP");
    expect(completeCalled).toBe(true);
    expect(fullReadCalled).toBe(false);
    expect(rewritePutCalled).toBe(false);
  });

  it("append creates missing paths with create-only revision semantics", async () => {
    server.use(
      http.get("http://localhost:9009/v1/status", () => HttpResponse.json({ inline_threshold: 50000 })),
      http.head("http://localhost:9009/v1/fs/new-log.txt", () => new HttpResponse(null, { status: 404 })),
      http.post("http://localhost:9009/v1/fs/new-log.txt", () => HttpResponse.text("unexpected append initiate", { status: 500 })),
      http.put("http://localhost:9009/v1/fs/new-log.txt", async ({ request }) => {
        expect(request.headers.get("x-dat9-expected-revision")).toBe("0");
        expect(await request.text()).toBe("hello");
        return HttpResponse.json({ revision: 1 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await client.warm();
    await client.append("/new-log.txt", text.encode("hello"));
  });

  it("append falls back to a bounded rewrite for small non-S3 files", async () => {
    server.use(
      http.get("http://localhost:9009/v1/status", () => HttpResponse.json({ inline_threshold: 50000 })),
      http.head("http://localhost:9009/v1/fs/small-log.txt", () =>
        new HttpResponse(null, {
          headers: {
            "Content-Length": "5",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "8",
          },
        })
      ),
      http.post("http://localhost:9009/v1/fs/small-log.txt", () =>
        HttpResponse.json({ error: "file is not S3-stored: /small-log.txt" }, { status: 400 })
      ),
      http.get("http://localhost:9009/v1/fs/small-log.txt", () => HttpResponse.arrayBuffer(text.encode("hello"))),
      http.put("http://localhost:9009/v1/fs/small-log.txt", async ({ request }) => {
        expect(request.headers.get("x-dat9-expected-revision")).toBe("8");
        expect(await request.text()).toBe("hello world");
        return HttpResponse.json({ revision: 9 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await client.warm();
    await client.append("/small-log.txt", text.encode(" world"));
  });

  it("append rejects large rewrites when the native append API is unavailable", async () => {
    let fullReadCalled = false;
    let rewritePutCalled = false;
    server.use(
      http.head("http://localhost:9009/v1/fs/large-db9.txt", () =>
        new HttpResponse(null, {
          headers: {
            "Content-Length": String(9 * 1024 * 1024),
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "8",
          },
        })
      ),
      http.post("http://localhost:9009/v1/fs/large-db9.txt", () =>
        HttpResponse.json({ error: "file is not S3-stored: /large-db9.txt" }, { status: 400 })
      ),
      http.get("http://localhost:9009/v1/fs/large-db9.txt", () => {
        fullReadCalled = true;
        return HttpResponse.text("unexpected full read", { status: 500 });
      }),
      http.put("http://localhost:9009/v1/fs/large-db9.txt", () => {
        rewritePutCalled = true;
        return HttpResponse.text("unexpected rewrite", { status: 500 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await expect(client.append("/large-db9.txt", text.encode("x"))).rejects.toThrow(/refusing read-modify-write append/);
    expect(fullReadCalled).toBe(false);
    expect(rewritePutCalled).toBe(false);
  });

  it("covers batch stat, batch read-small, enriched stat, and compat fallback", async () => {
    expect(MaxBatchStatPaths).toBe(256);
    expect(MaxBatchReadSmallPaths).toBe(128);

    server.use(
      http.post("http://localhost:9009/v1/fs:batch-stat", async ({ request }) => {
        expect(await request.json()).toEqual({ paths: ["/a.txt"] });
        return HttpResponse.json({ results: [{ path: "/a.txt", status: 200, size: 5, isDir: false, revision: 9 }] });
      }),
      http.post("http://localhost:9009/v1/fs:batch-read-small", async ({ request }) => {
        expect(await request.json()).toEqual({ paths: ["/a.txt"], max_bytes: 16 });
        return HttpResponse.json({
          results: [{ path: "/a.txt", status: 200, data: Buffer.from("hello").toString("base64"), size: 5 }],
        });
      }),
      http.get("http://localhost:9009/v1/fs/a.txt", ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.has("stat")) {
          return HttpResponse.json({
            size: 5,
            isdir: false,
            revision: 9,
            content_type: "text/plain",
            semantic_text: "hello",
            tags: { kind: "note" },
          });
        }
        return HttpResponse.text("unexpected", { status: 500 });
      }),
      http.get("http://localhost:9009/v1/fs/legacy.txt", () => HttpResponse.text("legacy")),
      http.head("http://localhost:9009/v1/fs/legacy.txt", () =>
        new HttpResponse(null, {
          headers: {
            "Content-Length": "6",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "2",
            "X-Dat9-Mtime": "100",
          },
        })
      )
    );

    const client = new Client("http://localhost:9009", "test-key");
    expect(await client.batchStat(["/a.txt"])).toEqual([{ path: "/a.txt", status: 200, size: 5, isDir: false, revision: 9 }]);
    const small = await client.batchReadSmall(["/a.txt"], 16);
    expect(new TextDecoder().decode(small[0].data)).toBe("hello");
    expect((await client.statMetadata("/a.txt")).tags.kind).toBe("note");
    expect((await client.statMetadataCompat("/legacy.txt")).degraded).toBe(true);
  });

  it("issues and revokes scoped filesystem tokens", async () => {
    server.use(
      http.post("http://localhost:9009/v1/tokens", async ({ request }) => {
        expect(await request.json()).toEqual({
          subject: "agent-1",
          ttl_seconds: 60,
          scopes: [{ prefix: "/scratch/", ops: ["read", "write"] }],
        });
        return HttpResponse.json({
          token: "dat9_scoped",
          token_id: "key_1",
          subject: "agent-1",
          scope_kind: "fs_scoped",
          scopes: [{ prefix: "/scratch/", ops: ["read", "write"] }],
        });
      }),
      http.delete("http://localhost:9009/v1/tokens/key_1", () => HttpResponse.text("ok")),
      http.post("http://localhost:9009/v1/tokens/revoke", async ({ request }) => {
        expect(await request.json()).toEqual({ api_key: "dat9_scoped" });
        return HttpResponse.text("ok");
      })
    );

    const client = new Client("http://localhost:9009", "owner-key");
    const token = await client.issueScopedToken({
      subject: "agent-1",
      ttl_seconds: 60,
      scopes: [{ prefix: "/scratch/", ops: ["read", "write"] }],
    });
    expect(token.token_id).toBe("key_1");
    await client.revokeScopedToken("key_1");
    await client.revokeScopedTokenByAPIKey("dat9_scoped");
  });

  it("covers vault grants and owner read helpers", async () => {
    server.use(
      http.post("http://localhost:9009/v1/vault/grants", async ({ request }) => {
        expect(await request.json()).toEqual({ agent: "agent", scope: ["secret:read"], perm: "read", ttl_seconds: 60 });
        return HttpResponse.json({ token: "vt_1", grant_id: "grant_1", expires_at: "2026-01-01T00:00:00Z", scope: ["secret:read"], perm: "read" });
      }),
      http.delete("http://localhost:9009/v1/vault/grants/grant_1", async ({ request }) => {
        expect(await request.json()).toEqual({ revoked_by: "owner", reason: "done" });
        return HttpResponse.text("ok");
      }),
      http.get("http://localhost:9009/v1/vault/secrets/aws/value", () => HttpResponse.json({ key: "value" })),
      http.get("http://localhost:9009/v1/vault/secrets/aws/value/key", () => HttpResponse.text("value"))
    );

    const client = new Client("http://localhost:9009", "owner-key");
    const grant = await client.issueVaultGrant({ agent: "agent", scope: ["secret:read"], perm: "read", ttl_seconds: 60 });
    expect(grant.grant_id).toBe("grant_1");
    await client.revokeVaultGrant("grant_1", { revoked_by: "owner", reason: "done" });
    expect((await client.readVaultSecretAsOwner("aws")).key).toBe("value");
    expect(await client.readVaultSecretFieldAsOwner("aws", "key")).toBe("value");
  });

  it("covers v36 LayerFS control-plane and mount-lease endpoint shapes", async () => {
    const layerId = "layer_01j8z6kh2m9q7w3r5t6y8v0p1a";
    const childId = "layer_01j8z6kh2m9q7w3r5t6y8v0p1b";

    server.use(
      http.post("http://localhost:9009/v1/layers", async ({ request }) => {
        expect(await request.json()).toEqual({ base_root_path: "/workspace", name: "work-a", ttl_seconds: 3600 });
        return HttpResponse.json({
          layer_id: layerId,
          base_root_path: "/workspace",
          name: "work-a",
          parent_layer_id: "",
          actor_id: "a",
          state: "active",
          snap_gen: 3,
          fence: 0,
          exclusive_mount_id: "",
          mount_lease_ttl_ms: 0,
          mount_expired: false,
          created_at: "now",
          updated_at: "now",
        });
      }),
      http.get("http://localhost:9009/v1/layers", ({ request }) => {
        const after = new URL(request.url).searchParams.get("after");
        if (!after) {
          return HttpResponse.json({
            layers: [{ layer_id: layerId, base_root_path: "/workspace", name: "work-a", parent_layer_id: "", actor_id: "a", state: "active", snap_gen: 3, fence: 0, exclusive_mount_id: "", mount_lease_ttl_ms: 0, mount_expired: false, created_at: "now", updated_at: "now" }],
            next_after: layerId,
          });
        }
        return HttpResponse.json({ layers: [] });
      }),
      http.get("http://localhost:9009/v1/layers/tag%3Aenv%3Dci", () =>
        HttpResponse.json({
          layer_id: layerId,
          base_root_path: "/workspace",
          name: "work-a",
          parent_layer_id: "",
          actor_id: "a",
          state: "active",
          snap_gen: 3,
          fence: 0,
          exclusive_mount_id: "",
          mount_lease_ttl_ms: 0,
          mount_expired: false,
          created_at: "now",
          updated_at: "now",
        })
      ),
      http.post(`http://localhost:9009/v1/layers/${layerId}/fork`, async ({ request }) => {
        expect(request.headers.get("authorization")).toBe("Bearer key");
        expect(await request.json()).toEqual({ name: "child", actor_id: "agent-1" });
        return HttpResponse.json({
          layer_id: childId,
          base_root_path: "/workspace",
          name: "child",
          parent_layer_id: layerId,
          actor_id: "agent-1",
          state: "active",
          snap_gen: 3,
          fence: 0,
          exclusive_mount_id: "",
          mount_lease_ttl_ms: 0,
          mount_expired: false,
          created_at: "now",
          updated_at: "now",
        });
      }),
      http.post(`http://localhost:9009/v1/layers/${layerId}/bind-mount`, async ({ request }) => {
        expect(await request.json()).toEqual({ mount_id: "m1", operation_id: "op1", expected_fence: 0 });
        return HttpResponse.json({ layer_id: layerId, mount_id: "m1", operation_id: "op1", fence: 1, mount_lease_until: "soon", mount_lease_ttl_ms: 60000 });
      }),
      http.post(`http://localhost:9009/v1/layers/${layerId}/renew-mount`, async ({ request }) => {
        expect(await request.json()).toEqual({ mount_id: "m1", fence: 1 });
        return HttpResponse.json({ layer_id: layerId, mount_id: "m1", operation_id: "op1", fence: 1, mount_lease_until: "later", mount_lease_ttl_ms: 60000 });
      }),
      http.post(`http://localhost:9009/v1/layers/${layerId}/unbind-mount`, async ({ request }) => {
        expect(await request.json()).toEqual({ mount_id: "m1", kind: "fence", fence: 1 });
        return HttpResponse.json({ layer_id: layerId, fence: 2, unbound: true });
      }),
      http.post(`http://localhost:9009/v1/layers/${layerId}/merge`, () =>
        HttpResponse.json({ layer_id: layerId, status: "merged", applied_paths: 4 })
      ),
      http.delete(`http://localhost:9009/v1/layers/${childId}`, ({ request }) => {
        expect(new URL(request.url).searchParams.get("force")).toBe("true");
        return HttpResponse.json({ layer_id: childId, status: "deleted" });
      })
    );

    const client = new Client("http://localhost:9009", "key");
    expect((await client.createLayer({ base_root_path: "/workspace", name: "work-a", ttl_seconds: 3600 })).layer_id).toBe(layerId);
    expect((await client.listAllLayers()).map((l) => l.layer_id)).toEqual([layerId]);
    expect((await client.getLayer("tag:env=ci")).layer_id).toBe(layerId);
    const child = await client.forkLayer(layerId, { name: "child", actor_id: "agent-1" });
    expect(child.parent_layer_id).toBe(layerId);
    expect((await client.bindLayerMount(layerId, { mount_id: "m1", operation_id: "op1", expected_fence: 0 })).fence).toBe(1);
    expect((await client.renewLayerMount(layerId, { mount_id: "m1", fence: 1 })).fence).toBe(1);
    expect((await client.unbindLayerMount(layerId, { mount_id: "m1", kind: "fence", fence: 1 })).unbound).toBe(true);
    expect((await client.mergeLayer(layerId)).applied_paths).toBe(4);
    expect((await client.deleteLayer(childId, { force: true })).status).toBe("deleted");
  });

  it("routes view-bound data plane via /v1/views/{layer_id}, enforces fence/Set D/batchWrite cap, and strips redirect credentials", async () => {
    const layerId = "layer_01j8z6kh2m9q7w3r5t6y8v0p1a";
    let s3SawAuth: string | null = null;
    let s3SawFence: string | null = null;
    let fencedLocally = false;

    server.use(
      http.get("http://localhost:9009/v1/status", ({ request }) => {
        expect(request.headers.get(LAYER_FENCE_HEADER)).toBeNull();
        return HttpResponse.json({ status: "ok", inline_threshold: 50000 });
      }),
      http.put(`http://localhost:9009/v1/views/${layerId}/v1/fs/workspace/a.txt`, ({ request }) => {
        expect(request.headers.get(LAYER_FENCE_HEADER)).toBe("5");
        return HttpResponse.json({ revision: 2 });
      }),
      http.get(`http://localhost:9009/v1/views/${layerId}/v1/fs/workspace`, ({ request }) => {
        expect(request.headers.get(LAYER_FENCE_HEADER)).toBe("5");
        const cursor = new URL(request.url).searchParams.get("cursor");
        if (!cursor) {
          return HttpResponse.json({ entries: [{ name: "a.txt", size: 2, isDir: false }], next_cursor: "c2" });
        }
        expect(cursor).toBe("c2");
        return HttpResponse.json({ entries: [{ name: "b.txt", size: 3, isDir: false }] });
      }),
      http.get(`http://localhost:9009/v1/views/${layerId}/v1/fs/workspace/s3.bin`, () =>
        new HttpResponse(null, { status: 302, headers: { Location: "http://s3.mock.local:9009/obj/1?sig=abc" } })
      ),
      http.get("http://s3.mock.local:9009/obj/1", ({ request }) => {
        s3SawAuth = request.headers.get("authorization");
        s3SawFence = request.headers.get(LAYER_FENCE_HEADER);
        return new HttpResponse(text.encode("from-s3"), { status: 200 });
      }),
      http.get(`http://localhost:9009/v1/views/${layerId}/v1/fs/workspace/evil.bin`, () =>
        new HttpResponse(null, { status: 302, headers: { Location: "http://localhost:9009/v1/status" } })
      ),
      http.delete(`http://localhost:9009/v1/layers/${layerId}`, () =>
        HttpResponse.json({ error: "fs layer is mounted", code: "LAYER_MOUNTED_BUSY" }, { status: 409 })
      )
    );

    const base = new Client("http://localhost:9009", "secret-token");
    expect(() => base.withView({ layerId: "not-canonical" })).toThrow(InvalidLayerViewRequestError);
    expect(() => base.withView({ layerId, fence: 2 })).toThrow(InvalidLayerViewRequestError);

    const vc = base.withView({
      layerId,
      fence: 5,
      writeGuard: () => {
        if (fencedLocally) throw new Error("self-fenced");
      },
    });
    expect(vc.viewBinding()).toEqual({ layerId, fence: 5 });

    // Set A passes through without X-FS-Layer-Fence.
    await vc.status();

    // Set C mutating request carries X-FS-Layer-Fence: 5.
    expect(await vc.writeWithRevision("/workspace/a.txt", text.encode("hi"))).toBe(2);

    // Cursor pagination in list().
    expect((await vc.list("/workspace")).map((e) => e.name)).toEqual(["a.txt", "b.txt"]);

    // Redirected object download strips Authorization and X-FS-Layer-Fence, and blocks redirects to /v1/*.
    expect(new TextDecoder().decode(await vc.read("/workspace/s3.bin"))).toBe("from-s3");
    expect(s3SawAuth).toBeNull();
    expect(s3SawFence).toBeNull();
    await expect(vc.read("/workspace/evil.bin")).rejects.toBeInstanceOf(InvalidLayerViewRequestError);

    await expect(vc.exec({ argv: ["pwd"] })).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.runtimeCapabilities()).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.listGitWorkspaces()).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.createJournal({ title: "test" })).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.revokeScopedToken("token")).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.listVaultSecrets()).rejects.toBeInstanceOf(UnsupportedInLayerViewError);

    // Set D & local caps reject before hitting network.
    await expect(vc.grep("q", "/workspace")).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.find("/workspace")).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.sql("SELECT 1")).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.append("/workspace/a.txt", text.encode("!"))).rejects.toBeInstanceOf(UnsupportedInLayerViewError);
    await expect(vc.write("/workspace/a.txt", text.encode("hi"), { description: "nope" })).rejects.toBeInstanceOf(UnsupportedInLayerViewError);

    const tooMany = Array.from({ length: MaxLayerViewBatchWriteItems + 1 }, (_, i) => ({
      path: `/workspace/${i}.txt`,
      data: text.encode("x"),
    }));
    await expect(vc.batchWrite(tooMany)).rejects.toThrow(/exceeds limit of 100/);

    // Local self-fence halts mutating requests while leaving reads intact.
    fencedLocally = true;
    server.use(...["batch-stat", "batch-read-small"].map((action) =>
      http.post(`http://localhost:9009/v1/views/${layerId}/v1/fs\\:${action}`, () =>
        HttpResponse.json({ results: [{ path: "/file", status: 200 }] }))
    ));
    expect(await vc.batchStat(["/file"])).toHaveLength(1);
    expect(await vc.batchReadSmall(["/file"])).toHaveLength(1);

    await expect(vc.write("/workspace/a.txt", text.encode("blocked"))).rejects.toThrow("self-fenced");
    expect((await vc.list("/workspace")).length).toBe(2);

    await expect(base.deleteLayer(layerId)).rejects.toMatchObject({
      name: "ConflictError",
      message: "fs layer is mounted",
      statusCode: 409,
    } satisfies Partial<ConflictError>);
  });

  it("covers git workspace API endpoint shapes", async () => {
    server.use(
      http.post("http://localhost:9009/v1/git-workspaces", async ({ request }) => {
        expect(await request.json()).toMatchObject({ root_path: "/repo/", repo_url: "https://example/repo.git" });
        return HttpResponse.json({ workspace_id: "ws_1", root_path: "/repo/", repo_url: "https://example/repo.git", remote_name: "origin", branch_name: "main", base_commit: "a", head_commit: "b", mode: "blobless", workspace_kind: "primary", common_workspace_id: "", worktree_name: "", gitdir_rel: ".git", status: "active", created_at: "now", updated_at: "now" });
      }),
      http.post("http://localhost:9009/v1/git-workspaces/ws_1/object-packs", async ({ request }) => {
        expect(await request.json()).toMatchObject({ content: Buffer.from("pack").toString("base64") });
        return HttpResponse.json({ workspace_id: "ws_1", pack_id: "pack_1", checksum_sha256: "sha", size_bytes: 4, created_at: "now" });
      }),
      http.get("http://localhost:9009/v1/git-workspaces/ws_1/overlay", () => HttpResponse.json({ entries: [] }))
    );

    const client = new Client("http://localhost:9009", "key");
    expect((await client.upsertGitWorkspace({ root_path: "/repo/", repo_url: "https://example/repo.git" })).workspace_id).toBe("ws_1");
    expect((await client.putGitObjectPack("ws_1", { content: text.encode("pack") })).pack_id).toBe("pack_1");
    expect(await client.listGitOverlayEntries("ws_1")).toEqual([]);
  });

  it("covers journal APIs, including NDJSON reads", async () => {
    server.use(
      http.post("http://localhost:9009/v1/journals", () =>
        HttpResponse.json({ journal_id: "jrn_1", kind: "agent", created_at: "now" })
      ),
      http.post("http://localhost:9009/v1/journals/jrn_1/entries", async ({ request }) => {
        expect(request.headers.get("idempotency-key")).toBe("append_1");
        expect(await request.json()).toEqual([{ type: "tool.call.completed" }]);
        return HttpResponse.json({ journal_id: "jrn_1", append_id: "append_1", first_seq: 1, last_seq: 1, count: 1, head_hash: "sha", idempotent: true });
      }),
      http.get("http://localhost:9009/v1/journals/jrn_1/entries", () =>
        HttpResponse.text(JSON.stringify({ journal_id: "jrn_1", seq: 1, entry_id: "e1", type: "tool.call.completed", schema_version: 1, occurred_at: "now", observed_at: "now", source: "self_reported", prev_hash: "", entry_hash: "sha" }) + "\n")
      ),
      http.get("http://localhost:9009/v1/journal-entries", ({ request }) => {
        expect(new URL(request.url).searchParams.get("include")).toBe("entry");
        return HttpResponse.text(JSON.stringify({ journal_id: "jrn_1", seq: 1, cursor: "c1" }) + "\n");
      }),
      http.get("http://localhost:9009/v1/journals/jrn_1/verify", () =>
        HttpResponse.json({ ok: true, journal_id: "jrn_1", entries: 1, head_hash: "sha", hash_chain_ok: true })
      )
    );

    const client = new Client("http://localhost:9009", "key");
    expect((await client.createJournal({ journal_id: "jrn_1" })).journal_id).toBe("jrn_1");
    expect((await client.appendJournalEntries("jrn_1", "append_1", [{ type: "tool.call.completed" }])).idempotent).toBe(true);
    expect((await client.readJournalEntries("jrn_1"))[0].seq).toBe(1);
    expect((await client.searchJournal({ entries: true }))[0].cursor).toBe("c1");
    expect((await client.verifyJournal("jrn_1")).ok).toBe(true);
  });

  it("parses one SSE event and stops on abort", async () => {
    const abort = new AbortController();
    server.use(
      http.get("http://localhost:9009/v1/events", ({ request }) => {
        expect(request.headers.get("x-dat9-actor")).toBe("mount-1");
        return new HttpResponse(
          `event: file_changed\ndata: ${JSON.stringify({ seq: 1, path: "/a.txt", op: "write", ts: 1 })}\n\n`,
          { headers: { "Content-Type": "text/event-stream" } }
        );
      })
    );

    const client = new Client("http://localhost:9009", "key");
    let seenPath = "";
    await client.watchEvents(
      "mount-1",
      (change) => {
        seenPath = change?.path || "";
        abort.abort();
      },
      { signal: abort.signal, initialBackoffMs: 1 }
    );
    expect(seenPath).toBe("/a.txt");
  });
});

function restoreEnv(name: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[name];
  } else {
    process.env[name] = value;
  }
}
