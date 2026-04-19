import { describe, expect, it, vi } from "vitest";
import { Client, ConflictError, Drive9Error, StatusError } from "../src/index.js";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";

const server = setupServer();
server.listen({ onUnhandledRequest: "error" });

describe("Client basic ops", () => {
  it("writes and reads", async () => {
    server.use(
      http.put("http://localhost:9009/v1/fs/hello.txt", async ({ request }) => {
        expect(request.headers.get("authorization")).toBe("Bearer test-key");
        return HttpResponse.text("ok");
      }),
      http.get("http://localhost:9009/v1/fs/hello.txt", () => {
        return HttpResponse.arrayBuffer(new TextEncoder().encode("hello world"));
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    await client.write("/hello.txt", new TextEncoder().encode("hello world"));
    const data = await client.read("/hello.txt");
    expect(new TextDecoder().decode(data)).toBe("hello world");
  });

  it("lists directory", async () => {
    server.use(
      http.get("http://localhost:9009/v1/fs/data/?list=1", () => {
        return HttpResponse.json({
          entries: [
            { name: "a.txt", size: 1, isDir: false },
            { name: "b.txt", size: 2, isDir: false },
          ],
        });
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    const entries = await client.list("/data/");
    expect(entries.length).toBe(2);
    expect(entries[0].name).toBe("a.txt");
  });

  it("stats a file", async () => {
    server.use(
      http.head("http://localhost:9009/v1/fs/test.txt", () => {
        return new HttpResponse(null, {
          status: 200,
          headers: {
            "Content-Length": "4",
            "X-Dat9-Revision": "7",
            "X-Dat9-IsDir": "false",
          },
        });
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    const info = await client.stat("/test.txt");
    expect(info.size).toBe(4);
    expect(info.revision).toBe(7);
    expect(info.isDir).toBe(false);
  });

  it("throws ConflictError on 409", async () => {
    server.use(
      http.put("http://localhost:9009/v1/fs/conflict.txt", () => {
        return HttpResponse.json({ error: "revision mismatch" }, { status: 409 });
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    await expect(client.write("/conflict.txt", new TextEncoder().encode("x"))).rejects.toThrow(
      ConflictError
    );
  });

  it("throws StatusError on 500", async () => {
    server.use(
      http.put("http://localhost:9009/v1/fs/err.txt", () => {
        return HttpResponse.json({ error: "boom" }, { status: 500 });
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    await expect(client.write("/err.txt", new TextEncoder().encode("x"))).rejects.toThrow(
      StatusError
    );
  });

  it("copies and renames", async () => {
    server.use(
      http.post("http://localhost:9009/v1/fs/dst.txt", ({ request }) => {
        expect(request.headers.get("x-dat9-copy-source")).toBe("/src.txt");
        return HttpResponse.text("ok");
      }),
      http.post("http://localhost:9009/v1/fs/new.txt", ({ request }) => {
        expect(request.headers.get("x-dat9-rename-source")).toBe("/old.txt");
        return HttpResponse.text("ok");
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    await client.copy("/src.txt", "/dst.txt");
    await client.rename("/old.txt", "/new.txt");
  });

  it("deletes and mkdir", async () => {
    server.use(
      http.delete("http://localhost:9009/v1/fs/del.txt", () => HttpResponse.text("ok")),
      http.post("http://localhost:9009/v1/fs/dir/?mkdir", () => HttpResponse.text("ok"))
    );
    const client = new Client("http://localhost:9009", "test-key");
    await client.delete("/del.txt");
    await client.mkdir("/dir/");
  });

  it("sql, grep, find", async () => {
    server.use(
      http.post("http://localhost:9009/v1/sql", () => HttpResponse.json([{ id: 1 }])),
      http.get("http://localhost:9009/v1/fs/?grep=hello", () =>
        HttpResponse.json([{ path: "/a.txt", name: "a.txt", size_bytes: 5 }])
      ),
      http.get("http://localhost:9009/v1/fs/?find=&type=file", () =>
        HttpResponse.json([{ path: "/b.txt", name: "b.txt", size_bytes: 3 }])
      )
    );
    const client = new Client("http://localhost:9009", "test-key");
    const sql = await client.sql("SELECT 1");
    expect(sql).toEqual([{ id: 1 }]);

    const grep = await client.grep("hello", "/", 0);
    expect(grep.length).toBe(1);

    const find = await client.find("/", { type: "file" });
    expect(find.length).toBe(1);
  });
});

describe("Config loading", () => {
  it("defaultClient loads config without panic", () => {
    const prevHome = process.env.HOME;
    process.env.HOME = "/nonexistent-home-" + Math.random();
    delete process.env.DRIVE9_SERVER;
    delete process.env.DRIVE9_API_KEY;
    const client = Client.defaultClient();
    expect(client.baseUrl).toBe("https://api.drive9.ai");
    expect(client.apiKey).toBeUndefined();
    process.env.HOME = prevHome;
  });

  it("env vars override config", () => {
    const prev = { s: process.env.DRIVE9_SERVER, k: process.env.DRIVE9_API_KEY };
    process.env.DRIVE9_SERVER = "http://env.drive9.ai";
    process.env.DRIVE9_API_KEY = "env-key";
    const client = Client.defaultClient();
    expect(client.baseUrl).toBe("http://env.drive9.ai");
    expect(client.apiKey).toBe("env-key");
    process.env.DRIVE9_SERVER = prev.s;
    process.env.DRIVE9_API_KEY = prev.k;
  });
});

describe("Vault", () => {
  it("createVaultSecret and listVaultSecrets", async () => {
    server.use(
      http.post("http://localhost:9009/v1/vault/secrets", async ({ request }) => {
        const body = (await request.json()) as { name: string };
        expect(body.name).toBe("aws");
        return HttpResponse.json({ name: "aws", secret_type: "kv", revision: 1, created_by: "x", created_at: "2024-01-01T00:00:00Z", updated_at: "2024-01-01T00:00:00Z" });
      }),
      http.get("http://localhost:9009/v1/vault/secrets", () => {
        return HttpResponse.json({ secrets: [{ name: "aws", secret_type: "kv", revision: 1, created_by: "x", created_at: "2024-01-01T00:00:00Z", updated_at: "2024-01-01T00:00:00Z" }] });
      })
    );
    const client = new Client("http://localhost:9009", "test-key");
    const secret = await client.createVaultSecret("aws", { key: "val" });
    expect(secret.name).toBe("aws");
    const list = await client.listVaultSecrets();
    expect(list.length).toBe(1);
  });

  it("issueVaultToken wire shape (spec 083aab8 line 133)", async () => {
    // Native assertion of the terminal wire shape:
    //   request  = {agent, scope[], perm, ttl_seconds, label_hint?}
    //   response = {token, grant_id, expires_at, scope[], perm, ttl}
    let capturedBody: Record<string, unknown> | undefined;
    server.use(
      http.post("http://localhost:9009/v1/vault/tokens", async ({ request }) => {
        capturedBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json(
          {
            token: "vault_abc",
            grant_id: "grt_123",
            expires_at: "2026-04-14T00:00:00Z",
            scope: ["aws-prod", "db-prod/password"],
            perm: "read",
            ttl: 3600,
          },
          { status: 201 }
        );
      }),
      http.delete("http://localhost:9009/v1/vault/tokens/grt_123", () => HttpResponse.text("ok"))
    );
    const client = new Client("http://localhost:9009", "test-key");
    const grant = await client.issueVaultToken(
      "deploy-agent",
      ["aws-prod", "db-prod/password"],
      "read",
      3600,
      "nightly"
    );
    expect(grant.token).toBe("vault_abc");
    expect(grant.grant_id).toBe("grt_123");
    expect(grant.scope).toEqual(["aws-prod", "db-prod/password"]);
    expect(grant.perm).toBe("read");
    expect(grant.ttl).toBe(3600);

    expect(capturedBody).toBeDefined();
    expect(capturedBody!.agent).toBe("deploy-agent");
    expect(capturedBody!.scope).toEqual(["aws-prod", "db-prod/password"]);
    expect(capturedBody!.perm).toBe("read");
    expect(capturedBody!.ttl_seconds).toBe(3600);
    expect(capturedBody!.label_hint).toBe("nightly");
    // Terminal-state reshape removed agent_id/task_id per spec §20.
    expect(capturedBody!.agent_id).toBeUndefined();
    expect(capturedBody!.task_id).toBeUndefined();

    await client.revokeVaultToken(grant.grant_id);
  });

  it("audit event wire shape (spec §16)", async () => {
    // Native assertion that audit events carry {grant_id, agent}
    // instead of the legacy {token_id, agent_id, task_id} trio.
    server.use(
      http.get("http://localhost:9009/v1/vault/audit", () =>
        HttpResponse.json({
          events: [
            {
              event_id: "e1",
              event_type: "secret.read",
              timestamp: "2026-04-14T00:00:00Z",
              grant_id: "grt_123",
              agent: "deploy-agent",
              secret_name: "aws-prod",
              field_name: "access_key",
              adapter: "api",
            },
          ],
        })
      )
    );
    const client = new Client("http://localhost:9009", "test-key");
    const events = await client.queryVaultAudit("aws-prod", 5);
    expect(events.length).toBe(1);
    const ev = events[0];
    expect(ev.grant_id).toBe("grt_123");
    expect(ev.agent).toBe("deploy-agent");
    expect(ev.secret_name).toBe("aws-prod");
    expect(ev.field_name).toBe("access_key");
    expect(ev.adapter).toBe("api");
    // The dataclass-equivalent assertion: legacy fields do not exist on
    // VaultAuditEvent per the terminal type.
    // @ts-expect-error — legacy field removed from VaultAuditEvent
    expect(ev.token_id).toBeUndefined();
    // @ts-expect-error — legacy field removed from VaultAuditEvent
    expect(ev.agent_id).toBeUndefined();
    // @ts-expect-error — legacy field removed from VaultAuditEvent
    expect(ev.task_id).toBeUndefined();
  });
});
