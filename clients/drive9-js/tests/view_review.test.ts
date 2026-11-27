import routes from "../../../pkg/viewprotocol/testdata/client_routes.json";
import { describe, expect, it } from "vitest";
import {
  InvalidLayerViewRequestError,
  UnsupportedInLayerViewError,
  fetchViewObjectRedirect,
  normalizeLayerRef,
  rewriteRequestForView,
  setClientViewBinding,
  validateViewBinding,
} from "../src/view.js";

describe("Layer view WriteGuard and redirect validation parity", () => {
  it("matches the server's Unicode layer name validation", () => {
    expect(normalizeLayerRef("\u0085分支🌱\u0085")).toBe("分支🌱");
    for (const name of ["a\u0081b", "a\u061cb", "a\u2066b", "a\uD800b"]) {
      expect(() => normalizeLayerRef(name)).toThrow(InvalidLayerViewRequestError);
    }
  });

  const layerId = "layer_01j8z4r9w3m5n7p9q2s4t6v8xz";

  it("invokes WriteGuard on GET /v1/uploads/{id}/resume but not on GET /v1/uploads list", () => {
    let guardCalls = 0;
    const binding = validateViewBinding(
      {
        layerId,
        fence: 3,
        writeGuard: () => {
          guardCalls++;
          throw new Error("self-fenced");
        },
      },
      "https://api.drive9.example/prefix"
    );

    expect(() =>
      rewriteRequestForView(binding, "https://api.drive9.example/prefix/v1/uploads/up-1/resume", {
        method: "GET",
      })
    ).toThrow("self-fenced");
    expect(guardCalls).toBe(1);

    const listRewritten = rewriteRequestForView(
      binding,
      "https://api.drive9.example/prefix/v1/uploads?path=%2Fa.txt",
      { method: "GET" }
    );
    expect(guardCalls).toBe(1);
    expect(listRewritten.url).toBe(
      `https://api.drive9.example/prefix/v1/views/${layerId}/v1/uploads?path=%2Fa.txt`
    );
  });

  it("recognizes an API route under the decoded deployment prefix", async () => {
    const client = { baseUrl: "https://example.test/%64eploy" };
    setClientViewBinding(client, validateViewBinding({ layerId }, client.baseUrl));
    await expect(fetchViewObjectRedirect(client, client.baseUrl, "/deploy/v1/status"))
      .rejects.toBeInstanceOf(InvalidLayerViewRequestError);
  });

  it("blocks redirect rebounds across alternate ports, subdomains, duplicate slashes, dot segments, and encoded separators", async () => {
    const fakeClient = { baseUrl: "https://api.drive9.example/deploy" };
    const binding = validateViewBinding({ layerId, fence: 1 }, fakeClient.baseUrl);
    setClientViewBinding(fakeClient, binding);

    const sourceUrl = `https://api.drive9.example/deploy/v1/views/${layerId}/v1/fs/file.bin`;
    const blockedTargets = [
      "https://api.drive9.example:8443/v1/fs/secret.txt",
      "https://sub.api.drive9.example/v1/fs/secret.txt",
      "https://sub.api.drive9.example/v2/uploads/initiate",
      "https://api.drive9.example//v1/fs/secret.txt",
      "https://api.drive9.example/pub/../v1/fs/secret.txt",
      "https://api.drive9.example/%2fv1/fs/secret.txt",
      "https://api.drive9.example/pub%5cv1/fs/secret.txt",
      "https://api.drive9.example/deploy//v1/fs/secret.txt",
      "https://api.drive9.example/deploy/x/../v2/uploads/initiate",
      "http://sub.api.drive9.example/bucket/obj.bin",
    ];

    for (const target of blockedTargets) {
      await expect(fetchViewObjectRedirect(fakeClient, sourceUrl, target)).rejects.toThrow(
        InvalidLayerViewRequestError
      );
    }
  });
});

describe("Go and JavaScript Layer route contract", () => {
  const origin = "https://api.drive9.example/deploy";
  const layerId = "layer_01j8z4r9w3m5n7p9q2s4t6v8xz";
  it.each(routes)("$method $target => $action", ({ method, target, action, mutates }) => {
    let guardCalls = 0;
    const binding = validateViewBinding({ layerId, fence: 1, writeGuard: () => { guardCalls++; } }, origin);
    const request = () => rewriteRequestForView(binding, origin + target, { method, headers: { "X-FS-Layer-Fence": "99" } });
    switch (action) {
      case "pass":
        const passed = request();
        expect(new Headers(passed.init?.headers).get("X-FS-Layer-Fence")).toBeNull();
        expect(passed.url).toBe(origin + target);
        break;
      case "rewrite":
        const rewritten = request();
        expect(new Headers(rewritten.init?.headers).get("X-FS-Layer-Fence")).toBe("1");
        expect(rewritten.url).toBe(origin + "/v1/views/" + layerId + target);
        break;
      case "unsupported":
        expect(request).toThrow(UnsupportedInLayerViewError);
        break;
      case "invalid":
        expect(request).toThrow(InvalidLayerViewRequestError);
        break;
      default:
        throw new Error("Unknown route action: " + action);
    }
    expect(guardCalls).toBe(mutates ? 1 : 0);
  });
});
