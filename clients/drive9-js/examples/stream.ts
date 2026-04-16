import { Client } from "../src/index.js";

async function streamToUint8Array(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const total = chunks.reduce((a, b) => a + b.length, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    result.set(c, offset);
    offset += c.length;
  }
  return result;
}

async function main() {
  const client = Client.defaultClient();
  // Or: const client = new Client("http://127.0.0.1:9009", "your-api-key");

  const path = "/examples/stream.bin";
  const data = new Uint8Array(1_000_000);
  for (let i = 0; i < data.length; i++) {
    data[i] = i % 256;
  }
  const size = data.length;

  // Stream upload
  await client.writeStream(path, data, size);
  console.log(`Stream-uploaded ${size} bytes to ${path}`);

  // Read back as stream
  const stream = await client.readStream(path);
  const readBack = await streamToUint8Array(stream);
  console.log(`Read back ${readBack.length} bytes via stream`);

  // Range read
  const rangeStream = await client.readStreamRange(path, 0, 1024);
  const rangeData = await streamToUint8Array(rangeStream);
  console.log(`Range read ${rangeData.length} bytes`);

  // StreamWriter (manual part-by-part)
  const writer = client.newStreamWriter(path, size);
  const partSize = 256_000;
  let partNum = 1;
  for (let offset = 0; offset < size; offset += partSize) {
    const chunk = data.subarray(offset, offset + partSize);
    await writer.writePart(partNum, chunk);
    partNum++;
  }
  await writer.complete(partNum - 1, new Uint8Array(0));
  console.log("Completed via StreamWriter");

  // Clean up
  await client.delete(path);
  console.log("Cleaned up");
}

main().catch(console.error);
