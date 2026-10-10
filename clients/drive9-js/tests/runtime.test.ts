import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import {
  Client,
  RuntimeCanceledError,
  RuntimeOutcomeUnknownError,
  RuntimeStartError,
  RuntimeTimeoutError,
  isRuntimeOutcomeUnknown,
} from "../src/index.js";

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterAll(() => server.close());

describe("runtime exec", () => {
  it("streams separate output and returns a nonzero exit", async () => {
    server.use(http.post("http://localhost:9009/v1/runtime/exec", async ({ request }) => {
      expect(request.headers.get("authorization")).toBe("Bearer test-key");
      const payload = await request.json() as Record<string, unknown>;
      expect(payload).toMatchObject({
        argv: ["sh", "-c", "exit 7"],
        workspace: { root: "/project", layer_id: "pi-fork-layer", persistence: "workspace" },
      });
      expect(payload).not.toHaveProperty("execution_class");
      expect(payload).not.toHaveProperty("resources");
      const body = [
        `{"type":"started","execution_id":"rex_12345678"}`,
        `{"type":"stdout","execution_id":"rex_12345678","data_base64":"aGVsbG8="}`,
        `{"type":"stderr","execution_id":"rex_12345678","data_base64":"d2Fybg=="}`,
        `{"type":"exit","execution_id":"rex_12345678","exit_code":7,"duration_ms":12}`,
        "",
      ].join("\n");
      return new HttpResponse(body, { headers: { "Content-Type": "application/x-ndjson" } });
    }));
    const stdout: string[] = [];
    const stderr: string[] = [];
    const result = await new Client("http://localhost:9009", "test-key").exec({
      argv: ["sh", "-c", "exit 7"], workspace: { root: "/project", layer_id: "pi-fork-layer", persistence: "workspace" },
    }, {
      onStdout: (chunk) => { stdout.push(new TextDecoder().decode(chunk)); },
      onStderr: (chunk) => { stderr.push(new TextDecoder().decode(chunk)); },
    });
    expect(result).toEqual({ executionId: "rex_12345678", exitCode: 7, durationMs: 12 });
    expect(stdout).toEqual(["hello"]);
    expect(stderr).toEqual(["warn"]);
  });

  it("does not retry when the stream ends before terminal", async () => {
    let calls = 0;
    server.use(http.post("http://localhost:9009/v1/runtime/exec", () => {
      calls++;
      return new HttpResponse(`{"type":"started","execution_id":"rex_12345678"}\n`, {
        headers: { "Content-Type": "application/x-ndjson" },
      });
    }));
    const promise = new Client("http://localhost:9009", "test-key").exec({ argv: ["true"], workspace: { root: "/" } });
    await expect(promise).rejects.toBeInstanceOf(RuntimeOutcomeUnknownError);
    expect(calls).toBe(1);
  });

  it("rejects malformed protocol as unknown", async () => {
    server.use(http.post("http://localhost:9009/v1/runtime/exec", () => new HttpResponse([
      `{"type":"started","execution_id":"rex_12345678"}`,
      `{"type":"stdout","execution_id":"rex_12345678","data_base64":"***"}`,
      "",
    ].join("\n"), { headers: { "Content-Type": "application/x-ndjson" } })));
    try {
      await new Client("http://localhost:9009", "test-key").exec({ argv: ["true"], workspace: { root: "/" } });
      throw new Error("expected runtime failure");
    } catch (error) {
      expect(isRuntimeOutcomeUnknown(error)).toBe(true);
    }
  });

  it.each([
    ["start_failed", RuntimeStartError],
    ["timed_out", RuntimeTimeoutError],
    ["canceled", RuntimeCanceledError],
    ["outcome_unknown", RuntimeOutcomeUnknownError],
  ])("maps %s to a typed error", async (code, expected) => {
    server.use(http.post("http://localhost:9009/v1/runtime/exec", () => new HttpResponse(
      `{"type":"error","error_code":"${code}","message":"failed"}\n`,
      { headers: { "Content-Type": "application/x-ndjson" } },
    )));
    await expect(new Client("http://localhost:9009", "test-key").exec({ argv: [], workspace: { root: "/" } }))
      .rejects.toBeInstanceOf(expected);
  });

  it("uses AbortSignal to request and confirm remote cancellation", async () => {
    let cancelCalls = 0;
    server.use(
      http.post("http://localhost:9009/v1/runtime/exec", () => new HttpResponse(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(`{"type":"started","execution_id":"rex_12345678"}\n`));
          },
        }),
        { headers: { "Content-Type": "application/x-ndjson" } },
      )),
      http.post("http://localhost:9009/v1/runtime/executions/rex_12345678/cancel", () => {
        cancelCalls++;
        return HttpResponse.json({ canceled: true });
      }),
    );
    const controller = new AbortController();
    const promise = new Client("http://localhost:9009", "test-key").exec(
      { argv: ["sleep", "30"], workspace: { root: "/" } },
      { signal: controller.signal, onStarted: () => { controller.abort(); } },
    );
    await expect(promise).rejects.toBeInstanceOf(RuntimeCanceledError);
    expect(cancelCalls).toBe(1);
  });

  it("requires confirmed cancellation", async () => {
    server.use(http.post("http://localhost:9009/v1/runtime/executions/rex_12345678/cancel", () => HttpResponse.json({ canceled: true })));
    await new Client("http://localhost:9009", "test-key").cancelRuntimeExecution("rex_12345678");

    server.use(http.post("http://localhost:9009/v1/runtime/executions/rex_12345678/cancel", () =>
      HttpResponse.json({ error: "unconfirmed", code: "outcome_unknown" }, { status: 503 })));
    const promise = new Client("http://localhost:9009", "test-key").cancelRuntimeExecution("rex_12345678");
    await expect(promise).rejects.toBeInstanceOf(RuntimeOutcomeUnknownError);
  });

  it("validates capabilities and rejects duplicate providers", async () => {
    server.use(http.get("http://localhost:9009/v1/runtime/capabilities", () => HttpResponse.json({
      version: 1,
      streaming: true,
      separate_stdout_stderr: true,
      cancel: true,
      cancel_scope: "active_execution",
      detached: false,
      replay: false,
      providers: [{ provider: "docker", profiles: ["default"], candidates: [{
        profile: "default",
        execution_class: "linux-full",
        persistence: "full_root",
        rootfs: {
          driver: "user-union",
          capability_version: "drive9_rootfs.user_union.extent.v5",
          config_hash: "a".repeat(64),
          lower_image_digest: `sha256:${"b".repeat(64)}`,
        },
        capabilities: {
          "workspace_persistence": true,
          "full_root_persistence": true,
          "drive9.extent_xattr.v1": true,
        },
        production_eligible: true,
        bounded_selection_eligible: true,
      }] }, { provider: "daytona", profiles: ["preview"], candidates: [{
        profile: "preview",
        execution_class: "linux-full",
        persistence: "workspace",
        rootfs: {
          driver: "workspace-mount",
          capability_version: "drive9_workspace.mount.v1",
          config_hash: "c".repeat(64),
          lower_image_digest: `sha256:${"d".repeat(64)}`,
        },
        capabilities: { "workspace_persistence": true },
        production_eligible: false,
        bounded_selection_eligible: false,
      }] }],
    })));
    await expect(new Client("http://localhost:9009", "test-key").runtimeCapabilities()).resolves.toMatchObject({ version: 1 });

    server.use(http.get("http://localhost:9009/v1/runtime/capabilities", () => HttpResponse.json({
      version: 1,
      streaming: true,
      separate_stdout_stderr: true,
      cancel: true,
      cancel_scope: "active_execution",
      detached: false,
      replay: false,
      providers: [{ provider: "docker", profiles: ["default"], candidates: [{
        profile: "default",
        execution_class: "linux-full",
        persistence: "full_root",
        rootfs: {
          driver: "user-union",
          capability_version: "drive9_rootfs.user_union.extent.v3",
          config_hash: "a".repeat(64),
          lower_image_digest: `sha256:${"b".repeat(64)}`,
        },
        capabilities: {
          "workspace_persistence": true,
          "full_root_persistence": true,
          "drive9.extent_xattr.v1": true,
        },
        production_eligible: true,
        bounded_selection_eligible: true,
      }] }],
    })));
    await expect(new Client("http://localhost:9009", "test-key").runtimeCapabilities()).rejects.toThrow("candidate");

    server.use(http.get("http://localhost:9009/v1/runtime/capabilities", () => HttpResponse.json({
      version: 1,
      streaming: true,
      separate_stdout_stderr: true,
      cancel: true,
      cancel_scope: "active_execution",
      detached: false,
      replay: false,
      providers: [{ provider: "docker", profiles: ["default"] }, { provider: "docker", profiles: ["large"] }],
    })));
    await expect(new Client("http://localhost:9009", "test-key").runtimeCapabilities()).rejects.toThrow("duplicate");
  });
});
