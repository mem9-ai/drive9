import { describe, expect, it } from "vitest";
import {
  InvalidLayerViewRequestError,
  fetchViewObjectRedirect,
  rewriteRequestForView,
  setClientViewBinding,
  validateViewBinding,
} from "../src/view.js";

describe("Layer view WriteGuard and redirect validation parity", () => {
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
