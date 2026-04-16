import { Client } from "../src/index.js";

async function main() {
  const client = Client.defaultClient();
  // Or: const client = new Client("http://127.0.0.1:9009", "your-api-key");

  const path = "/examples/hello.txt";

  // Write
  await client.write(path, new TextEncoder().encode("hello from drive9-js"));
  console.log("Wrote", path);

  // Read back
  const data = await client.read(path);
  console.log("Read back:", new TextDecoder().decode(data));

  // Stat
  const info = await client.stat(path);
  console.log(`Stat: size=${info.size} revision=${info.revision} isDir=${info.isDir}`);

  // List
  const entries = await client.list("/examples/");
  console.log("Entries in /examples/:");
  for (const e of entries) {
    console.log(`  ${e.name} (${e.size} bytes)`);
  }

  // Conditional write
  await client.write(path, new TextEncoder().encode("updated content"), info.revision);
  console.log("Conditional write succeeded");

  // Copy and rename
  await client.copy(path, "/examples/hello-copy.txt");
  await client.rename("/examples/hello-copy.txt", "/examples/hello-moved.txt");

  // Clean up
  await client.delete(path);
  await client.delete("/examples/hello-moved.txt");
  console.log("Cleaned up");
}

main().catch(console.error);
