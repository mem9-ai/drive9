import { Client, StreamWriter } from "../src/index.js";

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
  "checkpointFSLayer",
  "chmod",
  "commitFSLayer",
  "copy",
  "createFSLayer",
  "createFile",
  "createJournal",
  "createVaultSecret",
  "delete",
  "deleteDir",
  "deleteFile",
  "deleteGitWorkspace",
  "deleteVaultSecret",
  "diffFSLayer",
  "downloadToFile",
  "find",
  "fsUrl",
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
  "searchJournal",
  "setActor",
  "smallFileThresholdValue",
  "sql",
  "stat",
  "statMetadata",
  "statMetadataCompat",
  "status",
  "symlink",
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
  await client.removeAll("/sdk-ts/");
}

export async function streamingCookbook(client: Client, data: Uint8Array): Promise<void> {
  await client.writeStreamWithSummary("/sdk-ts/large.bin", data, data.length, {
    tags: { example: "typescript-sdk" },
  });
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
