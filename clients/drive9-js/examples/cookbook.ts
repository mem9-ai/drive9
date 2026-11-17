import { Client, StreamWriter } from "../src/index.js";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

export const coveredClientMethods = new Set([
  "appendJournalEntries",
  "append",
  "appendStream",
  "archive",
  "archiveToFile",
  "authHeaders",
  "baseURL",
  "batchReadSmall",
  "batchStat",
  "cachedSmallFileThreshold",
  "cancelRuntimeExecution",
  "checkpointFSLayer",
  "chmod",
  "commitExtentPatch",
  "commitFSLayer",
  "copy",
  "createExtentFile",
  "createFSLayer",
  "createFile",
  "createJournal",
  "createVaultSecret",
  "delete",
  "deleteDir",
  "deleteFile",
  "deleteFileConditional",
  "deleteFSLayer",
  "deleteGitWorkspace",
  "deleteVaultSecret",
  "diffFSLayer",
  "downloadDir",
  "downloadToFile",
  "exec",
  "fetchExtentBlockRange",
  "find",
  "forkFSLayer",
  "fsUrl",
  "getExtentReadPlan",
  "getFSLayer",
  "getFSLayerCheckpoint",
  "getFSLayerEntry",
  "getGitObjectPack",
  "getGitOverlayEntry",
  "getGitState",
  "getGitWorkspace",
  "getGitWorkspaceByRoot",
  "grep",
  "grepWithLayer",
  "hardlink",
  "initiateExtentPatch",
  "issueScopedToken",
  "issueVaultGrant",
  "issueVaultToken",
  "list",
  "listFSLayerEvents",
  "listFSLayers",
  "listGitObjectPacks",
  "listGitOverlayEntries",
  "listGitTree",
  "listGitWorkspaces",
  "listReadableVaultSecrets",
  "listVaultSecrets",
  "maxUploadBytes",
  "mkdir",
  "newStreamWriter",
  "patchFile",
  "putGitObjectPack",
  "putGitOverlayEntry",
  "queryVaultAudit",
  "rawDelete",
  "rawPost",
  "read",
  "readAt",
  "readFSLayerFile",
  "readFSLayerFileStream",
  "readJournalEntries",
  "readStream",
  "readStreamRange",
  "readVaultSecret",
  "readVaultSecretAsOwner",
  "readVaultSecretField",
  "readVaultSecretFieldAsOwner",
  "removeAll",
  "rename",
  "replaceGitTree",
  "replayFSLayer",
  "resumeUpload",
  "revokeScopedToken",
  "revokeScopedTokenByAPIKey",
  "revokeVaultGrant",
  "revokeVaultToken",
  "rollbackFSLayer",
  "runtimeCapabilities",
  "searchJournal",
  "setActor",
  "smallFileThresholdValue",
  "sql",
  "stat",
  "statMetadata",
  "statMetadataCompat",
  "status",
  "symlink",
  "truncateExtent",
  "upsertFSLayerEntry",
  "uploadFSLayerFile",
  "upsertGitState",
  "upsertGitWorkspace",
  "updateVaultSecret",
  "vaultUrl",
  "verifyJournal",
  "warm",
  "watchEvents",
  "watchEventsWithLifecycle",
  "withSmallFileThreshold",
  "write",
  "writeStream",
  "writeStreamWithSummary",
  "writeWithRevision",
]);

export const coveredStreamWriterMethods = new Set(["abort", "complete", "writePart"]);

export async function filesystemCookbook(client: Client): Promise<void> {
  await client.warm();
  await client.mkdir("/sdk-ts/");
  await client.write("/sdk-ts/hello.txt", new TextEncoder().encode("hello"), {
    expectedRevision: 0,
    tags: { example: "typescript-sdk" },
    description: "cookbook payload",
  });
  await client.read("/sdk-ts/hello.txt");
  await client.statMetadataCompat("/sdk-ts/hello.txt");
  await client.batchStat(["/sdk-ts/hello.txt"]);
  await client.batchReadSmall(["/sdk-ts/hello.txt"], 64 * 1024);
  await client.grep("hello", "/sdk-ts/", 10);
  await client.find("/sdk-ts/", { type: "file" });
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-cookbook-"));
  try {
    await client.downloadDir("/sdk-ts/", tmpDir);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
    await client.removeAll("/sdk-ts/");
  }
}

export async function streamingCookbook(client: Client, data: Uint8Array): Promise<void> {
  await client.writeStreamWithSummary("/sdk-ts/large.bin", data, data.length, {
    tags: { example: "typescript-sdk" },
    layout: "extent",
  });
  const plan = await client.getExtentReadPlan("/sdk-ts/large.bin", 0, Math.max(0, data.length - 1));
  if (plan.blocks[0]?.url) {
    const blkStream = await client.fetchExtentBlockRange(plan.blocks[0].url, plan.blocks[0].block_offset, 1);
    void blkStream.cancel().catch(() => {});
  }
  await client.commitExtentPatch(
    "/sdk-ts/large.bin",
    data.length,
    [{ chunk: 0, off: 0, data: data.subarray(0, Math.min(4, data.length)) }],
    plan.revision || 1
  );
  await client.truncateExtent("/sdk-ts/large.bin", 0);
  const created = await client.createExtentFile("/sdk-ts/empty-extent.bin");
  await client.deleteFileConditional("/sdk-ts/empty-extent.bin", created.revision, created.resource_id);
  const writer = client.newStreamWriter("/sdk-ts/manual.bin", data.length);
  await writer.writePart(1, data);
  await writer.complete(0, new Uint8Array());
  // Archive: download a remote tree as a streaming tar.gz with profile-based
  // filtering. The stream can be piped to stdout or a file.
  const archiveStream = await client.archive("/sdk-ts/", {
    exclude: ["**/node_modules/**"],
  });
  // Drain the archive stream (caller would pipe to a file in practice).
  const reader = archiveStream.getReader();
  while (true) {
    const { done } = await reader.read();
    if (done) break;
  }
  await client.archiveToFile("/sdk-ts/", "/tmp/sdk-ts-archive.tar.gz");
}

export function streamWriterCookbook(writer: StreamWriter): Promise<void> {
  return writer.abort();
}
