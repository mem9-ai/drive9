import { randomUUID } from "node:crypto";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { Client } from "../src/index.js";
import { Drive9Fs } from "../src/just-bash.js";

const [major, minor] = process.versions.node.split(".").map(Number);
const supported = major > 20 || (major === 20 && minor >= 19);
const enabled = supported && !!process.env.DRIVE9_INTEGRATION && !!process.env.DRIVE9_API_KEY;
// Shell workflows make many sequential HTTP requests. Allow for hosted-region
// latency as well as upload time; the local server usually finishes in seconds.
const liveTimeout = 120_000;

// Included in scripts/sdk-integration-tests.sh alongside the existing SDK suite.
describe.skipIf(!enabled)("Just Bash against a live Drive9 server", () => {
  let runtime: typeof import("just-bash");
  let client: Client;
  let root: string;
  let fs: Drive9Fs;
  beforeAll(async () => { runtime = await import("just-bash"); });
  beforeEach(async () => {
    client = Client.defaultClient();
    root = `/it-just-bash-${randomUUID()}`;
    await client.mkdir(root);
    fs = new Drive9Fs(client, { root });
  }, 30_000);
  afterEach(async () => { await client.removeAll(root); }, 30_000);
  const shell = (tokenClient = client) => {
    const mounts = new runtime.MountableFs({ base: new runtime.InMemoryFs() });
    mounts.mount("/workspace", new Drive9Fs(tokenClient, { root }));
    return new runtime.Bash({ fs: mounts, cwd: "/workspace" });
  };

  it("persists files across runtimes and runs text, copy, move and symlink commands", async () => {
    const result = await shell().exec(`
      set -e
      mkdir -p notes/nested
      printf 'alpha\\nbeta\\n' > notes/a.txt
      printf 'gamma\\n' >> notes/a.txt
      cat notes/*.txt | grep beta > notes/nested/match
      cp -r notes duplicate
      mv duplicate moved
      ln -s nested/match moved/link
      cat moved/link
    `);
    expect(result).toMatchObject({ exitCode: 0, stderr: "", stdout: "beta\n" });
    expect(await shell().exec("cat moved/a.txt")).toMatchObject({ exitCode: 0, stdout: "alpha\nbeta\ngamma\n" });
    await fs.writeFile("/moved/a.txt", "independent");
    expect(await fs.readFile("/notes/a.txt")).toBe("alpha\nbeta\ngamma\n");
    await fs.rm("/moved", { recursive: true });
    expect(await fs.exists("/moved")).toBe(false);
  }, liveTimeout);

  it.each(["文件\n.txt", "spaced "])("preserves binary content and hardlinks for %j", async (name) => {
    const data = new Uint8Array([0, 255, 128, 10]);
    await fs.writeFile("/" + name, data);
    await client.copy(root + "/" + name, root + "/sdk-copy");
    expect(await fs.readFileBuffer("/sdk-copy")).toEqual(data);
    await fs.link("/" + name, "/hard");
    await fs.mv("/" + name, "/移動");
    expect(await fs.readFileBuffer("/移動")).toEqual(data);
    expect(await shell().exec("cat hard | cat > binary-copy")).toMatchObject({ exitCode: 0, stderr: "" });
    expect(await fs.readFileBuffer("/binary-copy")).toEqual(data);
    await fs.writeFile("/hard", "shared");
    expect(await fs.readFile("/移動")).toBe("shared");
  }, liveTimeout);

  it("uploads, reads and appends object-backed binary content", async () => {
    // Exceed the SDK's 8 MiB rewrite limit so a successful append exercises
    // the server's native object append path instead of the small-file fallback.
    const data = new Uint8Array(9 * 1024 * 1024).fill(0xa5);
    await fs.writeFile("/large.bin", data);
    await fs.appendFile("/large.bin", new Uint8Array([0, 255]));
    const result = await fs.readFileBuffer("/large.bin");
    expect(result.length).toBe(data.length + 2);
    expect(Buffer.from(result.subarray(0, data.length)).equals(Buffer.from(data))).toBe(true);
    expect(result.slice(-2)).toEqual(new Uint8Array([0, 255]));
  }, liveTimeout);

  it("uses scoped credentials and honors server denials", async () => {
    await fs.writeFile("/seed", "visible");
    const grant = await client.issueScopedToken({
      ttl_seconds: 300,
      scopes: [{ prefix: root + "/", ops: ["read", "list"] }],
    });
    try {
      const scoped = new Client(client.baseURL(), grant.token);
      expect(await shell(scoped).exec("cat seed")).toMatchObject({ exitCode: 0, stdout: "visible" });
      await expect(new Drive9Fs(scoped, { root }).writeFile("/forbidden", "x")).rejects.toMatchObject({ code: "EACCES" });
      expect(await fs.exists("/forbidden")).toBe(false);
    } finally {
      if (grant.token_id) await client.revokeScopedToken(grant.token_id);
      else await client.revokeScopedTokenByAPIKey(grant.token);
    }
  }, liveTimeout);
});
