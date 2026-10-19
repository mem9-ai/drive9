import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import {
  Client,
  RuntimeBusyError,
  RuntimeCanceledError,
  RuntimeNotEnabledError,
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
        workspace: { root: "/project", layer_id: "pi-fork-layer" },
      });
      expect(payload.workspace).not.toHaveProperty("persistence");
      expect(payload).not.toHaveProperty("execution_class");
      expect(payload).not.toHaveProperty("resources");
      const body = [
        `{"type":"heartbeat"}`,
        `{"type":"started","execution_id":"rex_12345678","cwd":"/workspace","sandbox":"new"}`,
        `{"type":"heartbeat"}`,
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
      argv: ["sh", "-c", "exit 7"], workspace: { root: "/project", layer_id: "pi-fork-layer" },
    }, {
      onStdout: (chunk) => { stdout.push(new TextDecoder().decode(chunk)); },
      onStderr: (chunk) => { stderr.push(new TextDecoder().decode(chunk)); },
    });
    expect(result).toEqual({ executionId: "rex_12345678", exitCode: 7, durationMs: 12, sandbox: "new" });
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
    await expect(new Client("http://localhost:9009", "test-key").exec({ argv: ["true"], workspace: { root: "/" } }))
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

  it("runs a command line through the shell and reports the sandbox", async () => {
    server.use(http.post("http://localhost:9009/v1/runtime/exec", async ({ request }) => {
      const payload = await request.json() as Record<string, unknown>;
      expect(payload).toMatchObject({ argv: ["/bin/sh", "-c", "npm ci && npm test"], cwd: "web", workspace: {} });
      expect(payload).not.toHaveProperty("command");
      const body = [
        `{"type":"started","execution_id":"rex_12345678","cwd":"/workspace/web","sandbox":"warm"}`,
        `{"type":"exit","execution_id":"rex_12345678","exit_code":0,"duration_ms":3}`,
        "",
      ].join("\n");
      return new HttpResponse(body, { headers: { "Content-Type": "application/x-ndjson" } });
    }));
    let started: unknown;
    const result = await new Client("http://localhost:9009", "test-key").exec(
      { command: "npm ci && npm test", cwd: "web" },
      { onStarted: (event) => { started = event; } },
    );
    expect(started).toEqual({ executionId: "rex_12345678", cwd: "/workspace/web", sandbox: "warm" });
    expect(result.sandbox).toBe("warm");
    await expect(new Client("http://localhost:9009", "test-key").exec({ command: "x", argv: ["y"] }))
      .rejects.toBeInstanceOf(RuntimeStartError);
  });

  it("types busy and disabled servers", async () => {
    server.use(http.post("http://localhost:9009/v1/runtime/exec", () =>
      HttpResponse.json({ error: "too many commands are already running", code: "busy" }, { status: 429, headers: { "Retry-After": "7" } })));
    const busy = new Client("http://localhost:9009", "test-key").exec({ argv: ["true"] });
    await expect(busy).rejects.toBeInstanceOf(RuntimeBusyError);
    await expect(busy).rejects.toMatchObject({ retryAfterMs: 7000 });

    server.use(http.post("http://localhost:9009/v1/runtime/exec", () => new HttpResponse(null, { status: 404 })));
    await expect(new Client("http://localhost:9009", "test-key").exec({ argv: ["true"] })).rejects.toBeInstanceOf(RuntimeNotEnabledError);
  });

  it("reads capabilities leniently and rejects old servers", async () => {
    server.use(http.get("http://localhost:9009/v1/runtime/capabilities", () => HttpResponse.json({
      version: 2, provider: "daytona", persistence: "workspace", workspace_path: "/workspace", user: "agent",
      streaming: true, cancel: true, max_active_per_tenant: 4, future_field: true,
    })));
    await expect(new Client("http://localhost:9009", "test-key").runtimeCapabilities())
      .resolves.toMatchObject({ provider: "daytona", workspace_path: "/workspace", user: "agent" });

    server.use(http.get("http://localhost:9009/v1/runtime/capabilities", () => HttpResponse.json({ version: 1, streaming: true })));
    await expect(new Client("http://localhost:9009", "test-key").runtimeCapabilities()).rejects.toThrow("too old");

    server.use(http.get("http://localhost:9009/v1/runtime/capabilities", () => new HttpResponse(null, { status: 404 })));
    await expect(new Client("http://localhost:9009", "test-key").runtimeCapabilities()).rejects.toBeInstanceOf(RuntimeNotEnabledError);
  });
  it("treats a start-type error after started as an unknown outcome", async () => {
    server.use(http.post("http://localhost:9009/v1/runtime/exec", () => new HttpResponse(
      `{"type":"started","execution_id":"rex_12345678"}\n{"type":"error","execution_id":"rex_12345678","error_code":"start_failed","message":"failed"}\n`,
      { headers: { "Content-Type": "application/x-ndjson" } },
    )));
    await expect(new Client("http://localhost:9009", "test-key").exec({ argv: ["true"] }))
      .rejects.toBeInstanceOf(RuntimeOutcomeUnknownError);
  });
  it("treats redirects and gateway failures as unknown and 4xx as start failures", async () => {
    for (const [status, unknown] of [[300, true], [304, true], [307, true], [502, true], [504, true], [500, true], [408, true], [400, false], [403, false]] as const) {
      server.use(http.post("http://localhost:9009/v1/runtime/exec", () => new HttpResponse(null, { status })));
      const promise = new Client("http://localhost:9009", "test-key").exec({ argv: ["true"] });
      await expect(promise).rejects.toBeInstanceOf(unknown ? RuntimeOutcomeUnknownError : RuntimeStartError);
    }
  });
});
