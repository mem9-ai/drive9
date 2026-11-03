import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import test from "node:test";

const require = createRequire(import.meta.url);

// Exercise the published entry points together. Unit tests importing src cannot
// catch duplicated error classes when the SDK and adapter are bundled separately.
for (const format of ["import", "require"]) {
  test(`built ${format} SDK and adapter share error handling`, async (t) => {
    const sdk = format === "import" ? await import("drive9") : require("drive9");
    const { Drive9Fs } = format === "import" ? await import("drive9/just-bash") : require("drive9/just-bash");
    const server = createServer((req, res) => {
      const path = new URL(req.url, "http://localhost").pathname;
      if (path === "/v1/fs/missing") res.statusCode = 404;
      else if (path === "/v1/fs/denied") res.statusCode = 403;
      else if (req.method === "HEAD") {
        res.setHeader("X-Dat9-IsDir", String(path === "/v1/fs/"));
        res.setHeader("X-Dat9-Revision", "1");
        res.setHeader("Content-Length", "0");
      } else if (req.method === "DELETE") res.statusCode = 404;
      else {
        res.statusCode = 409;
        res.setHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ error: "revision mismatch", server_revision: 2 }));
        return;
      }
      res.end();
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    t.after(() => new Promise((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
      server.closeAllConnections();
    }));
    const client = new sdk.Client(`http://127.0.0.1:${server.address().port}`, "test-key");
    const fs = new Drive9Fs(client);
    assert.equal(await fs.exists("/missing"), false);
    await assert.rejects(fs.readFile("/denied"), { code: "EACCES" });
    await assert.rejects(fs.appendFile("/conflict", "new"), { code: "EAGAIN" });
    await fs.rm("/concurrently-removed", { force: true });
  });
}
