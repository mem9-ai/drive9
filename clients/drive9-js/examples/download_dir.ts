/**
 * Runnable demo of the TS SDK downloadDir capability.
 *
 * It connects to a drive9 server using DRIVE9_SERVER/DRIVE9_BASE +
 * DRIVE9_API_KEY env vars (or ~/.drive9/config), builds a small remote
 * directory tree, downloads it to a local temp dir via downloadDir,
 * verifies the round-trip, and cleans up both remote and local paths.
 *
 * Run it with:
 *
 *   npx tsx examples/download_dir.ts
 *
 * Set DRIVE9_SERVER and DRIVE9_API_KEY, or configure ~/.drive9/config.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Client, Drive9Error } from "../src/index.js";

const DEFAULT_SERVER = "https://api.drive9.ai";

function resolveClient(): Client {
  const client = Client.defaultClient();
  // If the constructor fell back to the default server and there is no
  // API key, the user hasn't configured credentials.
  if (client.baseURL() === DEFAULT_SERVER && !client.apiKey) {
    throw new Drive9Error(
      "No credentials found. Set DRIVE9_SERVER/DRIVE9_BASE and DRIVE9_API_KEY, or configure ~/.drive9/config."
    );
  }
  return client;
}

async function main(): Promise<void> {
  const client = resolveClient();
  const root = `/sdk-download-dir-demo-${Date.now()}`;

  try {
    await run(client, root);
  } catch (err) {
    // Best-effort cleanup on failure.
    try {
      await client.removeAll(root + "/");
    } catch {
      // Ignore cleanup errors.
    }
    throw err;
  }
}

async function run(client: Client, root: string): Promise<void> {
  // 1. Build a remote directory tree:
  //   <root>/
  //     top.txt       "top content"
  //     sub/
  //       nested.txt  "nested content"
  //       deep/
  //         leaf.txt  "leaf content"
  //     empty/
  console.log(`Building remote tree at ${root}/`);
  await client.mkdir(root);
  await client.mkdir(`${root}/sub`);
  await client.mkdir(`${root}/sub/deep`);
  await client.mkdir(`${root}/empty`);
  await client.write(`${root}/top.txt`, new TextEncoder().encode("top content"));
  await client.write(`${root}/sub/nested.txt`, new TextEncoder().encode("nested content"));
  await client.write(`${root}/sub/deep/leaf.txt`, new TextEncoder().encode("leaf content"));

  // 2. Download the entire remote tree to a local temp directory.
  const localDir = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-download-dir-demo-"));
  console.log(`Downloading ${root}/ → ${localDir}`);
  await client.downloadDir(root, localDir);

  // 3. Verify the round-trip.
  const wantFiles: Record<string, string> = {
    [path.join(localDir, "top.txt")]: "top content",
    [path.join(localDir, "sub", "nested.txt")]: "nested content",
    [path.join(localDir, "sub", "deep", "leaf.txt")]: "leaf content",
  };
  for (const [localPath, want] of Object.entries(wantFiles)) {
    const got = fs.readFileSync(localPath, "utf8");
    if (got !== want) {
      throw new Drive9Error(`${localPath} = ${got}, want ${want}`);
    }
    console.log(`  ✓ ${localPath} → ${got}`);
  }
  const wantDirs = [
    localDir,
    path.join(localDir, "sub"),
    path.join(localDir, "sub", "deep"),
    path.join(localDir, "empty"),
  ];
  for (const d of wantDirs) {
    if (!fs.statSync(d).isDirectory()) {
      throw new Drive9Error(`${d} is not a directory`);
    }
  }
  console.log(`  ✓ empty dir preserved: ${path.join(localDir, "empty")}`);

  // 4. Clean up remote and local.
  console.log(`Cleaning up remote tree ${root}/`);
  await client.removeAll(root + "/");
  console.log(`Cleaning up local dir ${localDir}`);
  fs.rmSync(localDir, { recursive: true });

  console.log("Done — downloadDir demo succeeded!");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});