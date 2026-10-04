import { createHash } from "crypto";

import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

import { Client, RuntimeStatusError, RuntimeUnsupportedError } from "../src/index.js";

const server = setupServer();

function validRuntimeCapabilities() {
  return {
    enabled: true,
    protocol_version: "runtime.v1",
    durability: "durable_per_operation",
    workspace_consistency: "per_file_capture",
    execution_effects: "workspace_and_external_no_automatic_replay",
    file_actions: ["read", "write", "edit", "delete", "list", "find", "grep"],
    profiles: ["go-build"],
    default_profile: "go-build",
    supports_arbitrary_command_replay: false,
    supports_sandbox_crud: false,
    supports_mounted_mode: false,
    limits: {
      max_tenant_queued_operations: 1000,
      max_principal_queued_operations: 100,
      max_tenant_active_operations: 64,
      max_principal_active_operations: 16,
      max_workspace_entries: 100_000,
      max_workspace_bytes: 10 * 2 ** 30,
      max_single_file_bytes: 1 << 30,
      max_path_bytes: 4096,
      max_path_depth: 128,
      max_manifest_bytes: 32 << 20,
      max_read_result_bytes: 4 << 20,
      max_list_entries: 10_000,
      max_search_file_bytes: 16 << 20,
      max_search_result_bytes: 4 << 20,
      max_operation_result_bytes: 8 << 20,
      max_log_bytes: 16 << 20,
      max_changed_bytes: 64 << 20,
      max_execution_seconds: 24 * 60 * 60,
      capture_deadline_seconds: 60,
      checkpoint_deadline_seconds: 30,
    },
  };
}

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => {
  vi.restoreAllMocks();
  server.resetHandlers();
});
afterAll(() => server.close());

describe("Runtime SDK", () => {
  it("fails closed before submit when Runtime is unavailable", async () => {
    let submitted = false;
    server.use(
      http.get("http://localhost:9009/v1/runtime/capabilities", () => new HttpResponse(null, { status: 404 })),
      http.post("http://localhost:9009/v1/runtime/executions", () => {
        submitted = true;
        return HttpResponse.json({}, { status: 500 });
      })
    );
    const client = new Client("http://localhost:9009", "owner-key");
    await expect(
      client.submitRuntimeExecution("key", { workspace_ref: "runtime://workspace", execution: { shell: "true" } })
    ).rejects.toBeInstanceOf(RuntimeUnsupportedError);
    expect(submitted).toBe(false);
  });

  it("rejects oversized Runtime JSON responses before buffering the body", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("{}", { headers: { "Content-Length": String((16 << 20) + 1) } })
    );
    await expect(
      new Client("http://localhost:9009", "owner-key").runtimeCapabilities()
    ).rejects.toThrow(`exceeds ${16 << 20} bytes`);
  });

  it("preserves the idempotency key and structured Runtime errors", async () => {
    server.use(
      http.get("http://localhost:9009/v1/runtime/capabilities", () =>
        HttpResponse.json(validRuntimeCapabilities())
      ),
      http.post("http://localhost:9009/v1/runtime/executions", ({ request }) => {
        expect(request.headers.get("idempotency-key")).toBe("stable-key");
        return HttpResponse.json({ error: "recovery_required", message: "workspace blocked" }, { status: 409 });
      })
    );
    const client = new Client("http://localhost:9009", "owner-key");
    try {
      await client.submitRuntimeExecution("stable-key", {
        workspace_ref: "runtime://workspace",
        execution: { shell: "true" },
      });
      throw new Error("expected RuntimeStatusError");
    } catch (error) {
      expect(error).toBeInstanceOf(RuntimeStatusError);
      expect((error as RuntimeStatusError).code).toBe("recovery_required");
    }
  });

  it("fails closed before submit when required limits are missing", async () => {
    let submitted = false;
    const capabilities = validRuntimeCapabilities();
    capabilities.limits.max_search_result_bytes = 0;
    server.use(
      http.get("http://localhost:9009/v1/runtime/capabilities", () =>
        HttpResponse.json(capabilities)
      ),
      http.post("http://localhost:9009/v1/runtime/executions", () => {
        submitted = true;
        return HttpResponse.json({}, { status: 500 });
      })
    );

    await expect(
      new Client("http://localhost:9009", "owner-key").submitRuntimeExecution("key", {
        workspace_ref: "runtime://workspace",
        execution: { shell: "true" },
      })
    ).rejects.toThrow("missing or invalid limits");
    expect(submitted).toBe(false);
  });

  it("fails closed before submit when logs exceed the artifact reader bound", async () => {
    let submitted = false;
    const capabilities = validRuntimeCapabilities();
    capabilities.limits.max_log_bytes = (1 << 30) + 1;
    server.use(
      http.get("http://localhost:9009/v1/runtime/capabilities", () => HttpResponse.json(capabilities)),
      http.post("http://localhost:9009/v1/runtime/executions", () => {
        submitted = true;
        return HttpResponse.json({}, { status: 500 });
      })
    );

    await expect(
      new Client("http://localhost:9009", "owner-key").submitRuntimeExecution("key", {
        workspace_ref: "runtime://workspace",
        execution: { shell: "true" },
      })
    ).rejects.toThrow("missing or invalid limits");
    expect(submitted).toBe(false);
  });

  it("fails closed before submit when the default profile is not advertised", async () => {
    let submitted = false;
    const capabilities = validRuntimeCapabilities();
    capabilities.default_profile = "other";
    server.use(
      http.get("http://localhost:9009/v1/runtime/capabilities", () => HttpResponse.json(capabilities)),
      http.post("http://localhost:9009/v1/runtime/executions", () => {
        submitted = true;
        return HttpResponse.json({}, { status: 500 });
      })
    );
    await expect(
      new Client("http://localhost:9009", "owner-key").submitRuntimeExecution("key", {
        workspace_ref: "runtime://workspace",
        execution: { shell: "true" },
      })
    ).rejects.toThrow("profiles");
    expect(submitted).toBe(false);
  });

  it("fails closed before submit for incompatible effect boundaries", async () => {
    let submitted = false;
    const capabilities = validRuntimeCapabilities();
    capabilities.supports_arbitrary_command_replay = true;
    server.use(
      http.get("http://localhost:9009/v1/runtime/capabilities", () =>
        HttpResponse.json(capabilities)
      ),
      http.post("http://localhost:9009/v1/runtime/executions", () => {
        submitted = true;
        return HttpResponse.json({}, { status: 500 });
      })
    );

    await expect(
      new Client("http://localhost:9009", "owner-key").submitRuntimeExecution("key", {
        workspace_ref: "runtime://workspace",
        execution: { shell: "true" },
      })
    ).rejects.toThrow("effect boundaries");
    expect(submitted).toBe(false);
  });

  it("requires file actions to be advertised before submit", async () => {
    let submitted = false;
    const capabilities = validRuntimeCapabilities();
    capabilities.file_actions = ["read"];
    server.use(
      http.get("http://localhost:9009/v1/runtime/capabilities", () =>
        HttpResponse.json(capabilities)
      ),
      http.post("http://localhost:9009/v1/runtime/file-operations", () => {
        submitted = true;
        return HttpResponse.json({}, { status: 500 });
      })
    );

    await expect(
      new Client("http://localhost:9009", "owner-key").submitRuntimeFileOperation("key", {
        workspace_ref: "runtime://workspace",
        operation: { action: "write", path: "/file" },
      })
    ).rejects.toThrow('file action "write" is unavailable');
    expect(submitted).toBe(false);
  });

  it("reconnects event streams from the last durable cursor", async () => {
    let window = 0;
    server.use(
      http.get("http://localhost:9009/v1/runtime/executions/exec_1/events", ({ request }) => {
        window += 1;
        const after = new URL(request.url).searchParams.get("after");
        if (window === 1) {
          expect(after).toBe(null);
          return HttpResponse.text('data: {"operation_id":"exec_1","cursor":2,"kind":"running"}\n\n');
        }
        expect(after).toBe("2");
        return HttpResponse.text('data: {"operation_id":"exec_1","cursor":3,"kind":"completed"}\n\n');
      }),
      http.get("http://localhost:9009/v1/runtime/executions/exec_1", () =>
        HttpResponse.json({ id: "exec_1", kind: "execution", state: window >= 2 ? "succeeded" : "running" })
      )
    );
    const cursors: number[] = [];
    await new Client("http://localhost:9009", "owner-key").watchRuntimeEvents(
      "executions",
      "exec_1",
      0,
      (event) => cursors.push(event.cursor)
    );
    expect(cursors).toEqual([2, 3]);
  });

  it("retries transient event stream failures", async () => {
    let window = 0;
    server.use(
      http.get("http://localhost:9009/v1/runtime/executions/exec_1/events", () => {
        window += 1;
        if (window === 1) return HttpResponse.text("temporarily unavailable", { status: 503 });
        return HttpResponse.text('data: {"operation_id":"exec_1","cursor":2,"kind":"completed"}\n\n');
      }),
      http.get("http://localhost:9009/v1/runtime/executions/exec_1", () =>
        HttpResponse.json({ id: "exec_1", kind: "execution", state: "succeeded" })
      )
    );
    await new Client("http://localhost:9009", "owner-key").watchRuntimeEvents(
      "executions",
      "exec_1",
      0,
      () => undefined
    );
    expect(window).toBe(2);
  });

  it("rejects unsafe event cursors before opening a stream", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    await expect(
      new Client("http://localhost:9009", "owner-key").watchRuntimeEvents(
        "executions",
        "exec_1",
        Number.MAX_SAFE_INTEGER + 1,
        () => undefined
      )
    ).rejects.toThrow("non-negative safe integer");
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("reconnects after a mid-stream failure from the last delivered cursor", async () => {
    let window = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const request = new Request(input);
      if (request.url.endsWith("/v1/runtime/executions/exec_1")) {
        return Response.json({ id: "exec_1", kind: "execution", state: "succeeded" });
      }
      if (request.url.includes("/v1/runtime/executions/exec_1/events")) {
        window += 1;
        const after = new URL(request.url).searchParams.get("after");
        if (window === 1) {
          expect(after).toBe(null);
          let pull = 0;
          return new HttpResponse(
            new ReadableStream({
              pull(controller) {
                pull += 1;
                if (pull === 1) {
                  controller.enqueue(
                    new TextEncoder().encode(
                      'data: {"operation_id":"exec_1","cursor":2,"kind":"running"}\n\n'
                    )
                  );
                  return;
                }
                controller.error(new TypeError("connection reset"));
              },
            })
          );
        }
        expect(after).toBe("2");
        return new Response(
          'data: {"operation_id":"exec_1","cursor":3,"kind":"completed"}\n\n'
        );
      }
      throw new Error(`unexpected fetch ${request.url}`);
    });
    const cursors: number[] = [];
    await new Client("http://localhost:9009", "owner-key").watchRuntimeEvents(
      "executions",
      "exec_1",
      0,
      (event) => cursors.push(event.cursor)
    );
    expect(cursors).toEqual([2, 3]);
  });

  it("verifies artifact bytes against the server checksum", async () => {
    const content = new TextEncoder().encode("durable artifact");
    const checksum = createHash("sha256").update(content).digest("hex");
    server.use(
      http.get("http://localhost:9009/v1/runtime/artifacts/artifact_1", () =>
        new HttpResponse(content, {
          headers: { "Content-Length": String(content.byteLength), "X-Content-SHA256": checksum },
        })
      )
    );
    const result = await new Client("http://localhost:9009", "owner-key").downloadRuntimeArtifact("artifact_1");
    expect(new TextDecoder().decode(result)).toBe("durable artifact");
  });

  it("rejects unverified artifacts", async () => {
    server.use(
      http.get("http://localhost:9009/v1/runtime/artifacts/artifact_1", () => HttpResponse.text("unverified"))
    );
    await expect(
      new Client("http://localhost:9009", "owner-key").downloadRuntimeArtifact("artifact_1")
    ).rejects.toThrow("omitted X-Content-SHA256");
  });

  it("rejects artifacts without a declared length", async () => {
    const content = new TextEncoder().encode("durable artifact");
    const checksum = createHash("sha256").update(content).digest("hex");
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(content, { headers: { "X-Content-SHA256": checksum } })
    );
    await expect(
      new Client("http://localhost:9009", "owner-key").downloadRuntimeArtifact("artifact_1")
    ).rejects.toThrow("omitted Content-Length");
  });

  it("rejects artifact Content-Length mismatches", async () => {
    const content = new TextEncoder().encode("abc");
    const checksum = createHash("sha256").update(content).digest("hex");
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(content, {
        headers: { "Content-Length": "4", "X-Content-SHA256": checksum },
      })
    );
    await expect(
      new Client("http://localhost:9009", "owner-key").downloadRuntimeArtifact("artifact_1")
    ).rejects.toThrow("Content-Length mismatch");
  });

  it("rejects oversized artifacts before buffering the body", async () => {
    server.use(
      http.get("http://localhost:9009/v1/runtime/artifacts/artifact_1", () =>
        new HttpResponse("x", {
          headers: { "Content-Length": "1073741825", "X-Content-SHA256": "0".repeat(64) },
        })
      )
    );
    await expect(
      new Client("http://localhost:9009", "owner-key").downloadRuntimeArtifact("artifact_1")
    ).rejects.toThrow("exceeds 1073741824 bytes");
  });
});
