import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { Bash, InMemoryFs, MountableFs } from "just-bash";
import { Client } from "drive9";
import { Drive9Fs } from "drive9/just-bash";

/** Run against a provisioned Drive9 tenant; only the unique example directory is changed. */
export async function runExample(client, { cleanup = false } = {}) {
  const root = `/just-bash-example-${randomUUID()}`;
  await client.mkdir(root);

  // Construct the adapter in host code, before running any guest commands.
  const newShell = () => {
    const fs = new MountableFs({ base: new InMemoryFs() });
    fs.mount("/workspace", new Drive9Fs(client, { root }));
    return new Bash({ fs, cwd: "/workspace" });
  };
  try {
    const bash = newShell();
    const result = await bash.exec(`
      set -e
      mkdir -p notes
      printf 'alpha\\nbeta\\n' > notes/input.txt
      printf 'gamma\\n' >> notes/input.txt
      cat notes/*.txt | grep beta > notes/matches.txt
      cp notes/input.txt notes/copy.txt
      mv notes/copy.txt notes/saved.txt
      ln -s saved.txt notes/latest
      cat notes/latest
      cp notes/matches.txt /tmp/scratch.txt
    `);
    if (result.exitCode !== 0) throw new Error(result.stderr);

    // A fresh runtime reconnects to the same persisted remote files.
    const fresh = newShell();
    const persisted = await fresh.exec("cat notes/matches.txt");
    if (persisted.exitCode !== 0 || persisted.stdout !== "beta\n") {
      throw new Error(`Persistence check failed: ${persisted.stderr}`);
    }
    const scratch = await fresh.exec("test -e /tmp/scratch.txt");
    if (scratch.exitCode !== 1) throw new Error("Scratch file unexpectedly survived a new runtime");

    return { root, stdout: result.stdout, persisted: persisted.stdout };
  } finally {
    if (cleanup) await client.removeAll(root);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const client = Client.defaultClient();
    if (!client.apiKey) throw new Error("Set DRIVE9_SERVER and DRIVE9_API_KEY, or configure ~/.drive9/config first.");
    const cleanup = process.argv.includes("--cleanup");
    const result = await runExample(client, { cleanup });
    process.stdout.write(result.stdout);
    process.stdout.write(`Fresh runtime read: ${result.persisted}`);
    process.stdout.write(`Remote directory ${cleanup ? "removed" : "retained"}: ${result.root}\n`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
