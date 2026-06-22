import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";

import { Client, MaxBatchReadSmallPaths, MaxBatchStatPaths } from "../src/index.js";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const text = new TextEncoder();

describe("TypeScript SDK parity surface", () => {
  it("loads owner or fs_scoped credentials from DRIVE9_CONFIG and lets env override individual fields", () => {
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
      const fromConfig = Client.defaultClient();
      expect(fromConfig.baseUrl).toBe("https://fallback.example");
      expect(fromConfig.apiKey).toBe("scoped-key");

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

  it("append rewrites with the current revision as a CAS guard", async () => {
    server.use(
      http.head("http://localhost:9009/v1/fs/log.txt", () =>
        new HttpResponse(null, {
          headers: {
            "Content-Length": "5",
            "X-Dat9-IsDir": "false",
            "X-Dat9-Revision": "8",
          },
        })
      ),
      http.get("http://localhost:9009/v1/fs/log.txt", () => HttpResponse.arrayBuffer(text.encode("hello"))),
      http.put("http://localhost:9009/v1/fs/log.txt", async ({ request }) => {
        expect(request.headers.get("x-dat9-expected-revision")).toBe("8");
        expect(await request.text()).toBe("hello world");
        return HttpResponse.json({ revision: 9 });
      })
    );

    const client = new Client("http://localhost:9009", "test-key");
    await client.append("/log.txt", text.encode(" world"));
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

  it("covers layer API endpoint shapes", async () => {
    server.use(
      http.post("http://localhost:9009/v1/layers", () =>
        HttpResponse.json({ layer_id: "layer_1", base_root_path: "/", name: "test", state: "open", durability_mode: "async", actor_id: "a", durable_seq: 0, created_at: "now", updated_at: "now" })
      ),
      http.get("http://localhost:9009/v1/layers/layer_1/diff", ({ request }) => {
        expect(new URL(request.url).searchParams.get("replay")).toBe("1");
        return HttpResponse.json({ entries: [{ layer_id: "layer_1", path: "/a.txt", parent_path: "/", name: "a.txt", op: "upsert", kind: "file", base_inode_id: "", base_revision: 1, storage_type: "inline", storage_ref: "", storage_ref_hash: "", storage_encryption_mode: "", storage_encryption_key_id: "", checksum_sha256: "", size_bytes: 1, mode: 420, entry_seq: 1, created_at: "now", updated_at: "now" }] });
      }),
      http.post("http://localhost:9009/v1/layers/layer_1/entries", async ({ request }) => {
        const body = (await request.json()) as { content: string };
        expect(body.content).toBe(Buffer.from("x").toString("base64"));
        return HttpResponse.json({ layer_id: "layer_1", path: "/a.txt", parent_path: "/", name: "a.txt", op: "upsert", kind: "file", base_inode_id: "", base_revision: 1, storage_type: "inline", storage_ref: "", storage_ref_hash: "", storage_encryption_mode: "", storage_encryption_key_id: "", checksum_sha256: "", size_bytes: 1, mode: 420, entry_seq: 1, created_at: "now", updated_at: "now" });
      }),
      http.post("http://localhost:9009/v1/layers/layer_1/commit", () => HttpResponse.json({ status: "committed", layer_id: "layer_1", applied: 1 }))
    );

    const client = new Client("http://localhost:9009", "key");
    expect((await client.createFSLayer({ base_root_path: "/" })).layer_id).toBe("layer_1");
    expect((await client.replayFSLayer("layer_1")).length).toBe(1);
    expect((await client.upsertFSLayerEntry("layer_1", { path: "/a.txt", content: text.encode("x") })).path).toBe("/a.txt");
    expect((await client.commitFSLayer("layer_1")).applied).toBe(1);
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
