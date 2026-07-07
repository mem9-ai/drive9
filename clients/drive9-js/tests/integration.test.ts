// Integration tests for the Drive9 TypeScript SDK.
//
// Exercises every exported Client / StreamWriter method against a live
// drive9-server-local. Gated by the DRIVE9_INTEGRATION env var so the default
// `npm test` (which runs the MSW-mocked suites) is unaffected. The cross-SDK
// runner (scripts/sdk-integration-tests.sh) exports DRIVE9_INTEGRATION=1 plus
// DRIVE9_SERVER / DRIVE9_API_KEY before invoking:
//
//   npx vitest run tests/integration.test.ts
//
// The client is constructed via Client.defaultClient() so the real
// config/env-resolution path (~/.drive9/config + DRIVE9_SERVER/DRIVE9_API_KEY)
// is exercised end to end.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as zlib from "node:zlib";
import { execSync } from "node:child_process";
import { Client, StreamWriter, ConflictError } from "../src/index.js";

const ENABLED = !!process.env.DRIVE9_INTEGRATION;
const BASE = (process.env.DRIVE9_SERVER ?? "http://127.0.0.1:9009").replace(/\/$/, "");
const API_KEY = process.env.DRIVE9_API_KEY ?? "local-dev-key";

// Minimal ustar reader: returns entry names (dirs get trailing slash).
function listTarGzNames(buf: Buffer): string[] {
  const decompressed = zlib.gunzipSync(buf);
  const names: string[] = [];
  let offset = 0;
  while (offset + 512 <= decompressed.length) {
    const header = decompressed.subarray(offset, offset + 512);
    let allZero = true;
    for (let i = 0; i < 512; i++) {
      if (header[i] !== 0) { allZero = false; break; }
    }
    if (allZero) break;
    const name = header.subarray(0, 100).toString("utf8").replace(/\0+$/, "");
    const typeflag = String.fromCharCode(header[156]);
    const sizeStr = header.subarray(124, 136).toString("utf8").replace(/\0+$/, "").trim();
    const size = parseInt(sizeStr, 8) || 0;
    names.push(typeflag === "5" ? name.replace(/\/?$/, "/") : name);
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  return names.sort();
}

// List zip entries via python3 (no native zip dep in the test env).
// python's zipfile needs a seekable file, so write the buffer to a temp file.
function listZipNames(buf: Buffer): string[] {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-ts-zip-"));
  const zipPath = path.join(tmp, "out.zip");
  try {
    fs.writeFileSync(zipPath, buf);
    const out = execSync(
      `python3 -c 'import zipfile,sys; print("\\n".join(sorted(zipfile.ZipFile(sys.argv[1]).namelist())))' ${zipPath}`,
      { encoding: "utf8" }
    );
    return out.trim().split("\n").filter(Boolean);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

function makeClient(): Client {
  // defaultClient() reads DRIVE9_SERVER / DRIVE9_API_KEY / ~/.drive9/config.
  return Client.defaultClient();
}

// Unique per-run prefix directory; removed in afterEach.
let prefix = "";
const prefixes: string[] = [];

async function newPrefix(): Promise<string> {
  const ts = Date.now();
  const rnd = Math.floor(Math.random() * 1e6);
  const p = `/it-ts-${ts}-${rnd}/`;
  const c = makeClient();
  await c.mkdir(p.replace(/\/$/, ""));
  prefixes.push(p);
  return p;
}

async function cleanupPrefixes(): Promise<void> {
  const c = makeClient();
  for (const p of prefixes) {
    try {
      await c.removeAll(p);
    } catch {
      // best-effort
    }
  }
  prefixes.length = 0;
}

async function serverReachable(): Promise<boolean> {
  try {
    const c = new Client(BASE, API_KEY);
    await c.warm();
    return true;
  } catch {
    return false;
  }
}

const describeIntegration = ENABLED ? describe : describe.skip;

describeIntegration("TypeScript SDK integration", () => {
  beforeEach(async () => {
    prefix = await newPrefix();
  });

  afterEach(async () => {
    await cleanupPrefixes();
  });

  // Skip the whole suite early if the server is not reachable.
  it("server is reachable", async () => {
    expect(await serverReachable()).toBe(true);
  });

  // ---------------------------------------------------------------------------
  // Lifecycle & config
  // ---------------------------------------------------------------------------

  it("lifecycle & config: baseURL, authHeaders, status, warm, thresholds, actor", async () => {
    const c = makeClient();
    expect(c.baseURL()).toBe(BASE);
    expect(c.authHeaders().Authorization).toBe(`Bearer ${API_KEY}`);
    c.setActor("it-ts-actor");
    expect(c.authHeaders().Authorization).toBe(`Bearer ${API_KEY}`);

    await c.warm();
    const status = await c.status();
    expect(status).toBeTruthy();
    expect(await c.maxUploadBytes()).toBeGreaterThanOrEqual(0);
    expect(await c.smallFileThresholdValue()).toBeGreaterThan(0);
    expect(c.cachedSmallFileThreshold()).toBeGreaterThanOrEqual(0);
    // fsUrl / vaultUrl
    expect(c.fsUrl("/x.txt")).toBe(`${BASE}/v1/fs/x.txt`);
    expect(c.vaultUrl("/read")).toBe(`${BASE}/v1/vault/read`);
    // withSmallFileThreshold builder
    const c2 = c.withSmallFileThreshold(123);
    expect(c2.smallFileThreshold).toBe(123);
  });

  // ---------------------------------------------------------------------------
  // FS core
  // ---------------------------------------------------------------------------

  it("FS core: write/read/createFile/symlink/hardlink/readAt/list/stat/statMetadata", async () => {
    const c = makeClient();
    const enc = new TextEncoder();
    const file = prefix + "hello.txt";
    const data = enc.encode("hello integration ts");
    await c.write(file, data);
    const got = await c.read(file);
    expect(new TextDecoder().decode(got)).toBe("hello integration ts");

    // writeWithRevision + ConflictError on CAS
    const rev = await c.writeWithRevision(file, enc.encode("v2"), -1);
    expect(rev).toBeGreaterThan(0);
    await expect(c.write(file, enc.encode("x"), 0)).rejects.toThrow(ConflictError);

    // createFile
    const empty = prefix + "empty.txt";
    expect(await c.createFile(empty)).toBeGreaterThan(0);

    // readAt
    const rng = prefix + "range.txt";
    await c.write(rng, enc.encode("0123456789"));
    const sub = await c.readAt(rng, 3, 4);
    expect(new TextDecoder().decode(sub)).toBe("3456");

    // list
    const entries = await c.list(prefix);
    const names = entries.map((e) => e.name);
    expect(names).toContain("hello.txt");

    // stat / statMetadata / statMetadataCompat — file now contains "v2" (overwritten above).
    const st = await c.stat(file);
    expect(st.size).toBe(2);
    expect(st.isDir).toBe(false);
    const sm = await c.statMetadata(file);
    expect(sm).toBeTruthy();
    const smc = await c.statMetadataCompat(file);
    expect(smc).toBeTruthy();

    // symlink / hardlink
    const link = prefix + "link.txt";
    await c.symlink(file, link);
    const hl = prefix + "hardlink.txt";
    await c.hardlink(file, hl);
  });

  it("FS core: delete/deleteFile/deleteDir/removeAll/copy/rename/mkdir/chmod", async () => {
    const c = makeClient();
    const enc = new TextEncoder();

    // mkdir + chmod (chmod may fail against a freshly-initialized local schema)
    const dir = prefix + "subdir";
    await c.mkdir(dir);
    try {
      await c.chmod(dir, 0o755);
    } catch {
      // best-effort
    }

    // copy / rename
    const src = prefix + "cp-src.txt";
    const dst = prefix + "cp-dst.txt";
    await c.write(src, enc.encode("copy-me"));
    await c.copy(src, dst);
    const got = await c.read(dst);
    expect(new TextDecoder().decode(got)).toBe("copy-me");

    const oldp = prefix + "rn-old.txt";
    const newp = prefix + "rn-new.txt";
    await c.write(oldp, enc.encode("rename-me"));
    await c.rename(oldp, newp);
    await expect(c.read(oldp)).rejects.toThrow();

    // delete / deleteFile / deleteDir
    const f1 = prefix + "d1.txt";
    await c.write(f1, enc.encode("x"));
    await c.delete(f1);
    const f2 = prefix + "d2.txt";
    await c.write(f2, enc.encode("x"));
    await c.deleteFile(f2);
    await c.deleteDir(dir);

    // removeAll
    const rmDir = prefix + "rmdir";
    await c.mkdir(rmDir + "/deep");
    await c.write(rmDir + "/deep/a.txt", enc.encode("a"));
    await c.removeAll(rmDir);
    await expect(c.stat(rmDir)).rejects.toThrow();
  });

  // ---------------------------------------------------------------------------
  // Batch / search / SQL
  // ---------------------------------------------------------------------------

  it("batch/search/sql", async () => {
    const c = makeClient();
    const enc = new TextEncoder();
    await c.write(prefix + "a.txt", enc.encode("aaa"));
    await c.write(prefix + "b.txt", enc.encode("bbb"));

    const stats = await c.batchStat([prefix + "a.txt", prefix + "b.txt", prefix + "missing"]);
    expect(stats.length).toBe(3);
    expect(stats.filter((s) => s.status === 200).length).toBe(2);

    const reads = await c.batchReadSmall([prefix + "a.txt", prefix + "b.txt"], 64);
    expect(reads.length).toBe(2);
    expect(reads.every((r) => r.status === 200)).toBe(true);

    // sql — must not error
    const rows = (await c.sql("SELECT path FROM file_nodes LIMIT 5")) as unknown[];
    expect(Array.isArray(rows)).toBe(true);

    // grep / grepWithLayer / find
    const results = await c.grep("aaa", prefix, 10);
    expect(Array.isArray(results)).toBe(true);
    const results2 = await c.grepWithLayer("aaa", prefix, 10, "");
    expect(Array.isArray(results2)).toBe(true);
    const found = await c.find(prefix, { name: "a.txt" });
    expect(Array.isArray(found)).toBe(true);
  });

  // ---------------------------------------------------------------------------
  // Transfer / streaming
  // ---------------------------------------------------------------------------

  it("streaming: writeStream small + large, readStream, readStreamRange, downloadToFile", async () => {
    const c = makeClient();
    const enc = new TextEncoder();

    // small
    const small = prefix + "small.bin";
    const sdata = enc.encode("small stream payload");
    await c.writeStream(small, sdata, sdata.length);
    const got = await c.read(small);
    expect(new TextDecoder().decode(got)).toBe("small stream payload");

    // large (> threshold → multipart)
    const large = prefix + "large.bin";
    const size = 2 * 1024 * 1024;
    const ldata = new Uint8Array(size).fill(76); // 'L'
    await c.writeStream(large, ldata, size);
    const st = await c.stat(large);
    expect(st.size).toBe(size);

    // writeStreamWithSummary
    const sum = await c.writeStreamWithSummary(prefix + "wsum.bin", sdata, sdata.length);
    expect(sum).toBeTruthy();

    // readStream
    const rc = await c.readStream(small);
    const buf = await new Response(rc).arrayBuffer();
    expect(new TextDecoder().decode(new Uint8Array(buf))).toBe("small stream payload");

    // readStreamRange
    const rc2 = await c.readStreamRange(large, 0, 10);
    const head = new Uint8Array(await new Response(rc2).arrayBuffer());
    expect(head.length).toBe(10);

    // downloadToFile
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-ts-"));
    const local = path.join(tmp, "large.copy");
    await c.downloadToFile(large, local);
    expect(fs.statSync(local).size).toBe(size);
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it("append + appendStream", async () => {
    const c = makeClient();
    const enc = new TextEncoder();
    const app = prefix + "append.bin";
    await c.write(app, enc.encode("head"));
    await c.append(app, enc.encode("tail"));
    const got = await c.read(app);
    expect(new TextDecoder().decode(got)).toBe("headtail");

    // appendStream from a stream
    const app2 = prefix + "append2.bin";
    await c.write(app2, enc.encode("abc"));
    const stream = new Response(enc.encode("def")).body!;
    await c.appendStream(app2, stream, 3);
    const got2 = await c.read(app2);
    expect(new TextDecoder().decode(got2)).toBe("abcdef");
  });

  it("resumeUpload (best-effort)", async () => {
    const c = makeClient();
    const large = prefix + "large.bin";
    const size = 2 * 1024 * 1024;
    const ldata = new Uint8Array(size).fill(76);
    // Try to resume; tolerate no in-progress upload.
    try {
      await c.resumeUpload(large, ldata);
    } catch (e) {
      // non-fatal
    }
  });

  it("patchFile (best-effort)", async () => {
    const c = makeClient();
    const large = prefix + "patch.bin";
    const size = 2 * 1024 * 1024;
    const orig = new Uint8Array(size).fill(79); // 'O'
    await c.writeStream(large, orig, size);
    const newData = new Uint8Array(size).fill(78); // 'N'
    try {
      await c.patchFile(large, size, [1], () => newData, undefined, 8 * 1024 * 1024);
    } catch {
      // some local servers may not support PATCH — non-fatal
    }
  });

  it("StreamWriter: writePart/complete/abort", async () => {
    const c = makeClient();
    const path_ = prefix + "sw.bin";
    const total = 2 * 1024 * 1024;
    const sw = c.newStreamWriter(path_, total);
    const part = new Uint8Array(8 * 1024 * 1024).fill(83); // 'S'
    await sw.writePart(1, part.subarray(0, total));
    await sw.complete(1, new Uint8Array(0));
    const got = await c.read(path_);
    expect(got.length).toBe(total);

    // abort path
    const sw2 = c.newStreamWriter(prefix + "sw-abort.bin", 64);
    await sw2.abort();
  });

  // ---------------------------------------------------------------------------
  // FS Layers
  // ---------------------------------------------------------------------------

  it("FS Layers", async () => {
    const c = makeClient();
    const layer = await c.createFSLayer({ base_root_path: prefix, name: "it-ts-layer" });
    expect(layer.layer_id).toBeTruthy();

    expect(await c.listFSLayers()).toBeTruthy();
    expect(await c.getFSLayer(layer.layer_id)).toBeTruthy();

    const entryPath = prefix + "layer-file.txt";
    await c.upsertFSLayerEntry(layer.layer_id, {
      path: entryPath,
      op: "upsert",
      kind: "file",
      content_text: "layer content",
      size_bytes: 13,
      mode: 0o644,
    });
    await c.getFSLayerEntry(layer.layer_id, entryPath);

    // uploadFSLayerFile + readFSLayerFile + readFSLayerFileStream
    const objPath = prefix + "layer-obj.bin";
    await c.uploadFSLayerFile(layer.layer_id, objPath, new TextEncoder().encode("obj"), { mode: 0o644 });
    const data = await c.readFSLayerFile(layer.layer_id, objPath);
    expect(new TextDecoder().decode(data)).toBe("obj");
    const rc = await c.readFSLayerFileStream(layer.layer_id, objPath);
    await new Response(rc).arrayBuffer();

    // diff / replay
    expect(await c.diffFSLayer(layer.layer_id)).toBeTruthy();
    expect(await c.diffFSLayer(layer.layer_id, 1 << 30)).toBeTruthy();
    expect(await c.replayFSLayer(layer.layer_id)).toBeTruthy();
    expect(await c.replayFSLayer(layer.layer_id, 1 << 30)).toBeTruthy();

    // events
    expect(await c.listFSLayerEvents(layer.layer_id, 0)).toBeTruthy();

    // checkpoint + get
    const ch = await c.checkpointFSLayer(layer.layer_id, { label: "it-ts-cp" });
    expect(ch.checkpoint_id).toBeTruthy();
    await c.getFSLayerCheckpoint(ch.checkpoint_id);

    // rollback + commit (best-effort)
    try {
      await c.rollbackFSLayer(layer.layer_id);
    } catch {
      // non-fatal
    }
    try {
      await c.commitFSLayer(layer.layer_id);
    } catch {
      // non-fatal
    }
  });

  // ---------------------------------------------------------------------------
  // Journals
  // ---------------------------------------------------------------------------

  it("Journals", async () => {
    const c = makeClient();
    const jid = "it-ts-journal-" + Date.now();
    const j = await c.createJournal({
      journal_id: jid,
      kind: "agent",
      title: "it-ts journal",
      actor: { type: "agent", id: "it-ts" },
      source: "self_reported",
    });
    expect(j.journal_id).toBe(jid);

    const appendID = "append-1";
    const resp = await c.appendJournalEntries(jid, appendID, [
      { type: "step", status: "ok", source: "self_reported", subjects: ["task:it-ts"] },
    ]);
    expect(resp.count).toBe(1);

    // idempotent re-append
    await c.appendJournalEntries(jid, appendID, [
      { type: "step", status: "ok", source: "self_reported", subjects: ["task:it-ts"] },
    ]);

    const read = await c.readJournalEntries(jid, 0, 10);
    expect(read.length).toBe(1);

    // entry search requires at least one of type/status/actor/subject/metadata filter
    const matches = await c.searchJournal({ kind: "agent", entries: true, subjects: ["task:it-ts"], limit: 10 });
    expect(Array.isArray(matches)).toBe(true);

    const v = await c.verifyJournal(jid);
    expect(v.ok).toBe(true);
  });

  // ---------------------------------------------------------------------------
  // Events / SSE
  // ---------------------------------------------------------------------------

  it("Events / SSE", async () => {
    const c = makeClient();
    const enc = new TextEncoder();
    const ctrl = new AbortController();
    let seen = false;
    // Pass the abort signal into watchEvents so ctrl.abort() actually stops
    // the SSE loop (otherwise the watcher keeps reconnecting after the test).
    const promise = c.watchEvents("it-ts-actor", (change, reset) => {
      if (change && change.path.startsWith(prefix)) {
        seen = true;
        ctrl.abort();
      }
    }, { signal: ctrl.signal });
    // give the SSE connection a moment to establish before writing
    await new Promise((r) => setTimeout(r, 500));
    // generate an event
    await c.write(prefix + "ev.txt", enc.encode("event"));
    // wait up to 12s for the event / abort
    const timeout = new Promise((resolve) => setTimeout(resolve, 12000));
    await Promise.race([promise.catch(() => {}), timeout]);
    ctrl.abort();
    expect(seen).toBe(true);
  }, 20000);

  // ---------------------------------------------------------------------------
  // Tokens
  // ---------------------------------------------------------------------------

  it("Tokens (best-effort — local server may not enable token mgmt)", async () => {
    const c = makeClient();
    let resp;
    try {
      resp = await c.issueScopedToken({
        subject: "it-ts-subject",
        ttl_seconds: 3600,
        scopes: [{ prefix: "/", ops: ["read"] }],
      });
    } catch (e) {
      // token management may be disabled on drive9-server-local
      return;
    }
    expect(resp.token).toBeTruthy();
    try {
      await c.revokeScopedToken(resp.token_id!);
    } catch {
      // non-fatal
    }

    try {
      const resp2 = await c.issueScopedToken({
        subject: "it-ts-subject-2",
        ttl_seconds: 3600,
        scopes: [{ prefix: "/", ops: ["read"] }],
      });
      await c.revokeScopedTokenByAPIKey(resp2.token);
    } catch {
      // non-fatal
    }
  });

  // ---------------------------------------------------------------------------
  // Vault
  // ---------------------------------------------------------------------------

  it("Vault (best-effort — local server may not enable the vault backend)", async () => {
    const c = makeClient();
    const secName = "it-ts-secret-" + Date.now();

    let sec;
    try {
      sec = await c.createVaultSecret(secName, { token: "hunter2" });
    } catch {
      // vault backend not configured on drive9-server-local
      return;
    }
    expect(sec.name).toBe(secName);

    await c.updateVaultSecret(secName, { token: "hunter3" });

    const list = await c.listVaultSecrets();
    expect(list.some((s) => s.name === secName)).toBe(true);

    const vals = await c.readVaultSecretAsOwner(secName);
    expect(vals.token).toBe("hunter3");
    const fv = await c.readVaultSecretFieldAsOwner(secName, "token");
    expect(fv).toBe("hunter3");

    const vt = await c.issueVaultToken("it-ts-agent", "it-ts-task", [`secret:${secName}`], 60);
    expect(vt.token).toBeTruthy();
    try {
      await c.revokeVaultToken(vt.token_id);
    } catch {
      // non-fatal
    }

    const gr = await c.issueVaultGrant({
      agent: "it-ts-agent",
      scope: [`secret:${secName}`],
      perm: "read",
      ttl_seconds: 60,
      label_hint: "it-ts-grant",
    });
    expect(gr.grant_id).toBeTruthy();
    try {
      await c.revokeVaultGrant(gr.grant_id);
    } catch {
      // non-fatal
    }

    expect(await c.queryVaultAudit(secName, 10)).toBeTruthy();

    // capability-token read path (best-effort in local mode)
    try {
      await c.listReadableVaultSecrets();
    } catch {
      // non-fatal
    }
    try {
      await c.readVaultSecret(secName);
    } catch {
      // non-fatal
    }
    try {
      await c.readVaultSecretField(secName, "token");
    } catch {
      // non-fatal
    }

    await c.deleteVaultSecret(secName);
  });

  // ---------------------------------------------------------------------------
  // Git workspaces
  // ---------------------------------------------------------------------------

  it("Git workspaces", async () => {
    const c = makeClient();
    const root = `/it-ts-git-${Date.now()}/`;
    const ws = await c.upsertGitWorkspace({
      root_path: root,
      repo_url: "https://example.com/repo.git",
      branch_name: "main",
      base_commit: "0000000000000000000000000000000000000001",
      head_commit: "0000000000000000000000000000000000000002",
    });
    expect(ws.workspace_id).toBeTruthy();
    try {
      await c.getGitWorkspaceByRoot(root);
      await c.getGitWorkspace(ws.workspace_id);
      await c.listGitWorkspaces();

      await c.replaceGitTree(ws.workspace_id, {
        commit_sha: ws.head_commit,
        nodes: [
          {
            // node paths are relative to the workspace root; object_sha must
            // be a 40- or 64-character git object id.
            path: "a.txt",
            parent_path: "",
            name: "a.txt",
            kind: "file",
            mode: "100644",
            object_sha: "00000000000000000000000000000000000000a1",
            size_bytes: 1,
          },
        ],
      });
      const tree = await c.listGitTree(ws.workspace_id, ws.head_commit);
      expect(tree.length).toBe(1);

      await c.upsertGitState(ws.workspace_id, {
        checkpoint_commit: ws.head_commit,
        content: new TextEncoder().encode("git-state-content"),
      });
      await c.getGitState(ws.workspace_id);

      const pack = await c.putGitObjectPack(ws.workspace_id, {
        content: new TextEncoder().encode("pack-content"),
      });
      await c.listGitObjectPacks(ws.workspace_id);
      if (pack.pack_id) {
        await c.getGitObjectPack(ws.workspace_id, pack.pack_id);
      }

      const ovPath = "overlay.txt";
      await c.putGitOverlayEntry(ws.workspace_id, {
        path: ovPath,
        op: "upsert",
        kind: "file",
        mode: "100644",
        content: new TextEncoder().encode("overlay-content"),
      });
      await c.getGitOverlayEntry(ws.workspace_id, ovPath);
      await c.listGitOverlayEntries(ws.workspace_id);
    } finally {
      try {
        await c.deleteGitWorkspace(ws.workspace_id);
      } catch {
        // best-effort
      }
    }
  });

  // ---------------------------------------------------------------------------
  // Raw HTTP
  // ---------------------------------------------------------------------------

  it("raw HTTP", async () => {
    const c = makeClient();
    const resp = await c.rawPost("/v1/sql", { query: "SELECT 1" });
    expect(resp.status).toBeLessThan(300);
    await resp.text();

    const file = prefix + "raw-del.txt";
    await c.write(file, new TextEncoder().encode("x"));
    const resp2 = await c.rawDelete(`/v1/fs${file}`);
    expect(resp2.status).toBeLessThan(300);
    await resp2.text();
  });

  // ---------------------------------------------------------------------------
  // downloadDir
  // ---------------------------------------------------------------------------

  it("downloadDir downloads a remote tree to a local dir", async () => {
    const c = makeClient();
    const enc = new TextEncoder();
    const root = prefix + "dl/";
    await c.mkdir(root.replace(/\/$/, ""));
    await c.write(root + "a.txt", enc.encode("aaa"));
    await c.mkdir(root + "sub");
    await c.write(root + "sub/b.txt", enc.encode("bbb"));
    await c.mkdir(root + "sub/nested");
    await c.write(root + "sub/nested/c.txt", enc.encode("ccc"));

    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-ts-dd-"));
    const dest = path.join(tmp, "dl");
    await c.downloadDir(root, dest);
    expect(fs.readFileSync(path.join(dest, "a.txt"), "utf8")).toBe("aaa");
    expect(fs.readFileSync(path.join(dest, "sub", "b.txt"), "utf8")).toBe("bbb");
    expect(fs.readFileSync(path.join(dest, "sub", "nested", "c.txt"), "utf8")).toBe("ccc");

    // downloadDir refuses to overwrite an existing destination.
    await expect(c.downloadDir(root, dest)).rejects.toThrow();

    // downloadDir rejects a file source (not a directory).
    const fileSrc = prefix + "dl-file.txt";
    await c.write(fileSrc, enc.encode("x"));
    await expect(c.downloadDir(fileSrc, path.join(tmp, "notdir"))).rejects.toThrow();

    fs.rmSync(tmp, { recursive: true, force: true });
  });

  // ---------------------------------------------------------------------------
  // archive / archiveToFile
  // ---------------------------------------------------------------------------

  it("archive + archiveToFile (tar.gz + zip + exclude)", async () => {
    const c = makeClient();
    const enc = new TextEncoder();
    const root = prefix + "ar/";
    await c.mkdir(root.replace(/\/$/, ""));
    await c.write(root + "a.txt", enc.encode("aaa"));
    await c.mkdir(root + "sub");
    await c.write(root + "sub/b.txt", enc.encode("bbb"));
    await c.write(root + "sub/excluded.txt", enc.encode("drop-me"));
    await c.mkdir(root + "node_modules");
    await c.mkdir(root + "node_modules/pkg");
    await c.write(root + "node_modules/pkg/index.js", enc.encode("js"));

    // archive() returns a streaming tar.gz; collect it and inspect entries.
    const stream = await c.archive(root);
    const chunks: Buffer[] = [];
    for await (const chunk of stream as any) {
      chunks.push(Buffer.from(chunk));
    }
    const tarGz = Buffer.concat(chunks);
    const tarNames = listTarGzNames(tarGz);
    expect(tarNames.some((n) => n.endsWith("a.txt"))).toBe(true);
    expect(tarNames.some((n) => n.endsWith("sub/b.txt"))).toBe(true);

    // archiveToFile zip with exclude dropping "**/excluded.txt" and node_modules.
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "drive9-ts-ar-"));
    const zipPath = path.join(tmp, "out.zip");
    await c.archiveToFile(root, zipPath, {
      format: "zip",
      exclude: ["**/excluded.txt", "**/node_modules/**"],
    });
    const zipBuf = fs.readFileSync(zipPath);
    const zipNames = listZipNames(zipBuf);
    expect(zipNames.some((n) => n.endsWith("excluded.txt"))).toBe(false);
    expect(zipNames.some((n) => n.includes("node_modules"))).toBe(false);
    expect(zipNames.some((n) => n.endsWith("a.txt"))).toBe(true);
    expect(zipNames.some((n) => n.endsWith("sub/b.txt"))).toBe(true);

    // archiveToFile default tar.gz with include-only "sub/**" (prefix filter:
    // keep only the sub subtree).
    const tarGzPath = path.join(tmp, "out.tar.gz");
    await c.archiveToFile(root, tarGzPath, { include: ["sub/**"] });
    const incNames = listTarGzNames(fs.readFileSync(tarGzPath));
    expect(incNames.some((n) => n.endsWith("node_modules/pkg/index.js"))).toBe(false);
    expect(incNames.some((n) => n.endsWith("a.txt"))).toBe(false);
    expect(incNames.some((n) => n.endsWith("sub/b.txt"))).toBe(true);

    fs.rmSync(tmp, { recursive: true, force: true });
  });
});