// archive.ts — standalone runnable example for client.archive / archiveToFile.
//
// Run with: node --experimental-vm-mode dist/examples/archive.js  (after build)
// Or directly via tsx/ts-node against a live drive9 server:
//   DRIVE9_SERVER=http://127.0.0.1:9009 DRIVE9_API_KEY=local-dev-key npx tsx examples/archive.ts
//
// Demonstrates: tar.gz streaming to stdout, file output, zip format, profile
// and include/exclude filtering, flat mode.

import { Client } from "../src/index.js";

async function main(): Promise<void> {
  const server = process.env.DRIVE9_SERVER ?? "http://127.0.0.1:9009";
  const apiKey = process.env.DRIVE9_API_KEY ?? "";
  if (!apiKey) {
    console.error("set DRIVE9_API_KEY");
    process.exit(2);
  }
  const client = new Client(server, apiKey);

  const remoteDir = process.env.DRIVE9_ARCHIVE_DIR ?? "/sdk-ts-archive-example";

  // Seed a small tree so the example is self-contained.
  await client.mkdir(`${remoteDir}/src`);
  await client.mkdir(`${remoteDir}/node_modules`);
  await client.mkdir(`${remoteDir}/node_modules/react`);
  await client.write(`${remoteDir}/README.md`, new TextEncoder().encode("archive example\n"));
  await client.write(`${remoteDir}/src/app.go`, new TextEncoder().encode("package main\n"));
  await client.write(`${remoteDir}/node_modules/react/index.js`, new TextEncoder().encode("module.exports\n"));

  // 1) tar.gz to a file, with node_modules excluded.
  const outFile = `${remoteDir}.tar.gz`.replace(/^\//, "./");
  await client.archiveToFile(remoteDir, outFile, { exclude: ["**/node_modules/**"] });
  console.log(`wrote ${outFile} (tar.gz, node_modules excluded)`);

  // 2) Stream tar.gz to stdout — pipe-friendly. Here we just count entries.
  const stream = await client.archive(remoteDir, { exclude: ["**/node_modules/**"] });
  const reader = stream.getReader();
  let chunks = 0;
  while (true) {
    const { done } = await reader.read();
    if (done) break;
    chunks++;
  }
  console.log(`streamed tar.gz to stdout in ${chunks} chunk(s)`);

  // 3) zip format with profile filtering (coding-agent skips node_modules/.git/dist).
  const zipFile = `${remoteDir}.zip`.replace(/^\//, "./");
  await client.archiveToFile(remoteDir, zipFile, { format: "zip", profile: "coding-agent" });
  console.log(`wrote ${zipFile} (zip, coding-agent profile)`);

  // 4) include whitelist — only Go source under src/.
  const inclFile = `${remoteDir}-include.tar.gz`.replace(/^\//, "./");
  await client.archiveToFile(remoteDir, inclFile, { include: ["src/**"] });
  console.log(`wrote ${inclFile} (tar.gz, include src/**)`);

  // 5) flat mode — basenames only, no directory hierarchy.
  const flatFile = `${remoteDir}-flat.tar.gz`.replace(/^\//, "./");
  await client.archiveToFile(remoteDir, flatFile, { flat: true, exclude: ["**/node_modules/**"] });
  console.log(`wrote ${flatFile} (tar.gz, flat)`);

  // Cleanup the seeded remote tree.
  await client.removeAll(remoteDir);
  console.log("done");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});