import { describe, expect, it } from "vitest";
import { Client, StatusError } from "../src/index.js";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";

const server = setupServer();
server.listen({ onUnhandledRequest: "error" });

const BASE = "http://localhost:9009";

describe("removeAll", () => {
  it("retries on 503 honoring Retry-After and succeeds after the sweep resumes", async () => {
    // Server answers 503, 503, then 200: removeAll must issue exactly 3
    // requests and honor the Retry-After header (1s on the second response).
    let calls = 0;
    server.use(
      http.delete(`${BASE}/v1/fs/data`, ({ request }) => {
        const url = new URL(request.url);
        expect(url.searchParams.get("recursive")).toBe("1");
        calls++;
        if (calls === 1) {
          return new HttpResponse(JSON.stringify({ error: "recursive delete in progress, retry to resume" }), {
            status: 503,
            headers: { "Content-Type": "application/json", "Retry-After": "0" },
          });
        }
        if (calls === 2) {
          return new HttpResponse(JSON.stringify({ error: "recursive delete in progress, retry to resume" }), {
            status: 503,
            headers: { "Content-Type": "application/json", "Retry-After": "1" },
          });
        }
        return HttpResponse.json({ status: "ok" });
      })
    );

    const client = new Client(BASE, "test-key");
    const start = Date.now();
    await client.removeAll("/data/");
    const elapsed = Date.now() - start;

    expect(calls).toBe(3);
    // The second 503 carried Retry-After: 1, so at least ~1s must have passed.
    expect(elapsed).toBeGreaterThanOrEqual(900);
  });

  it("surfaces the last 503 after exhausting retries", async () => {
    // Server always answers 503: removeAll must fail after 1 + 4 requests.
    let calls = 0;
    server.use(
      http.delete(`${BASE}/v1/fs/data`, () => {
        calls++;
        return new HttpResponse(JSON.stringify({ error: "recursive delete in progress, retry to resume" }), {
          status: 503,
          headers: { "Content-Type": "application/json", "Retry-After": "0" },
        });
      })
    );

    const client = new Client(BASE, "test-key");
    await expect(client.removeAll("/data/")).rejects.toMatchObject({
      name: "StatusError",
      statusCode: 503,
    });
    expect(calls).toBe(5);
  });

  it("does not retry non-recursive deletes on 503", async () => {
    let calls = 0;
    server.use(
      http.delete(`${BASE}/v1/fs/data`, () => {
        calls++;
        return new HttpResponse(JSON.stringify({ error: "boom" }), {
          status: 503,
          headers: { "Content-Type": "application/json" },
        });
      })
    );

    const client = new Client(BASE, "test-key");
    await expect(client.delete("/data/")).rejects.toBeInstanceOf(StatusError);
    expect(calls).toBe(1);
  });
});
