import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

import { Client } from "../src/index.js";
import { Drive9Fs } from "../src/just-bash.js";
import { drive9HTTP } from "./helpers/drive9-http.js";

const server = setupServer();
let fixture: ReturnType<typeof drive9HTTP>;
let fs: Drive9Fs;
let client: Client;

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());
beforeEach(() => {
  fixture = drive9HTTP();
  fixture.put("/tenant/work/seed.txt", "hello");
  server.use(...fixture.handlers);
  client = new Client(fixture.base, "test-key").withSmallFileThreshold(1024 * 1024);
  fs = new Drive9Fs(client, { root: "/tenant/work" });
});

describe("Drive9Fs over HTTP", () => {
  it("persists UTF-8 and binary writes across adapter instances", async () => {
    await fs.writeFile("/résumé.txt", "你好 🌍");
    await fs.writeFile("/binary", new Uint8Array([0, 255, 128, 10]));
    const fresh = new Drive9Fs(client, { root: "/tenant/work/" });
    expect(await fresh.readFile("/re\u0301sume\u0301.txt")).toBe("你好 🌍");
    expect(await fresh.readFileBuffer("/binary")).toEqual(new Uint8Array([0, 255, 128, 10]));
    expect(await fresh.readFile("/binary", "hex")).toBe("00ff800a");
    await fs.writeFile("/encoded", "AP+A", { encoding: "base64" });
    expect(await fresh.readFile("/encoded", "hex")).toBe("00ff80");
  });

  it("appends to existing and missing files with SDK revision checks", async () => {
    await fs.appendFile("/seed.txt", " world");
    await fs.appendFile("/new", "ff", "hex");
    expect(await fs.readFile("/seed.txt")).toBe("hello world");
    expect(await fs.readFile("/new", "hex")).toBe("ff");
  });

  it("lists names and types and translates stat metadata", async () => {
    await fs.mkdir("/sub");
    await fs.symlink("seed.txt", "/shortcut");
    expect(await fs.readdir("/")).toEqual(["seed.txt", "shortcut", "sub"]);
    expect(await fs.readdirWithFileTypes("/")).toEqual([
      { name: "seed.txt", isFile: true, isDirectory: false, isSymbolicLink: false },
      { name: "shortcut", isFile: false, isDirectory: false, isSymbolicLink: true },
      { name: "sub", isFile: false, isDirectory: true, isSymbolicLink: false },
    ]);
    expect(await fs.stat("/seed.txt")).toMatchObject({
      isFile: true, isDirectory: false, size: 5, mode: 0o644,
      mtime: new Date(1700000000000),
    });
    expect(await fs.exists("/missing")).toBe(false);
    expect(fs.getAllPaths()).toEqual([]);
  });

  it("enforces parent/type checks despite the server creating parents automatically", async () => {
    await expect(fs.writeFile("/missing/file", "x")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.mkdir("/missing/sub")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.writeFile("/seed.txt/child", "x")).rejects.toMatchObject({ code: "ENOTDIR" });
    await expect(fs.readFile("/")).rejects.toMatchObject({ code: "EISDIR" });
    await expect(fs.readdir("/seed.txt")).rejects.toMatchObject({ code: "ENOTDIR" });
    await expect(fs.stat("/seed.txt/")).rejects.toMatchObject({ code: "ENOTDIR" });
    expect(fixture.nodes.has("/tenant/work/missing")).toBe(false);
  });

  it("returns false for existence checks below files, including through symlinks", async () => {
    await fs.symlink("seed.txt", "/link");
    for (const path of ["/seed.txt/child", "/seed.txt/", "/link/child", "/link/"]) {
      expect(await fs.exists(path)).toBe(false);
    }
  });

  it("implements recursive/idempotent mkdir and empty-directory removal", async () => {
    await fs.mkdir("/a/b", { recursive: true });
    await fs.mkdir("/a/b", { recursive: true });
    await expect(fs.mkdir("/a")).rejects.toMatchObject({ code: "EEXIST" });
    await expect(fs.rm("/a")).rejects.toMatchObject({ code: "ENOTEMPTY" });
    await fs.rm("/a/b");
    await fs.rm("/a");
    await fs.rm("/absent", { force: true });
    await expect(fs.rm("/absent")).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("copies files independently and copies trees recursively", async () => {
    await fs.cp("/seed.txt", "/copy.txt");
    await fs.writeFile("/copy.txt", "changed");
    expect(await fs.readFile("/seed.txt")).toBe("hello");
    expect(fixture.calls.some((call) => call.query.includes("copy"))).toBe(false);
    await fs.mkdir("/tree/nested", { recursive: true });
    await fs.writeFile("/tree/nested/file", "payload");
    await fs.symlink("nested/file", "/tree/link");
    await expect(fs.cp("/tree", "/out")).rejects.toMatchObject({ code: "EISDIR" });
    await fs.cp("/tree", "/out", { recursive: true });
    expect(await fs.readFile("/out/link")).toBe("payload");
    await fs.rm("/tree", { recursive: true });
    expect(await fs.exists("/tree")).toBe(false);
    expect(await fs.readFile("/out/nested/file")).toBe("payload");
  });

  it("copies symlinks over files and links without modifying their referents", async () => {
    await fs.symlink("seed.txt", "/source");
    await fs.writeFile("/old", "keep");
    await fs.writeFile("/file", "replace");
    await fs.symlink("old", "/link");
    await fs.symlink("missing", "/dangling");
    await fs.mkdir("/dir");
    await fs.symlink("dir", "/dir-link");
    for (const path of ["/file", "/link", "/dangling", "/dir-link"]) {
      await fs.cp("/source", path);
      expect((await fs.lstat(path)).isSymbolicLink).toBe(true);
      expect(await fs.readlink(path)).toBe("seed.txt");
    }
    expect(await fs.readFile("/old")).toBe("keep");
    expect(await fs.readFile("/seed.txt")).toBe("hello");
    expect(await fs.exists("/missing")).toBe(false);
    expect((await fs.stat("/dir")).isDirectory).toBe(true);
  });

  it("preserves copy destinations when a symlink cannot be copied", async () => {
    await fs.symlink("seed.txt", "/source");
    await fs.mkdir("/dir");
    await fs.writeFile("/dir/keep", "keep");
    await expect(fs.cp("/source", "/dir")).rejects.toMatchObject({ code: "EISDIR" });
    await expect(fs.cp("/source", "/source")).rejects.toMatchObject({ code: "EINVAL" });
    await fs.symlink("../seed.txt", "/dir/relative");
    await fs.writeFile("/target", "keep");
    await expect(fs.cp("/dir/relative", "/target")).rejects.toMatchObject({ code: "EACCES" });
    server.use(http.get(`${fixture.base}/v1/fs/tenant/work/source`, () => new HttpResponse(null, { status: 403 })));
    await expect(fs.cp("/source", "/target")).rejects.toMatchObject({ code: "EACCES" });
    expect(await fs.readFile("/target")).toBe("keep");
    expect(await fs.readFile("/dir/keep")).toBe("keep");
    expect(await fs.readlink("/dir/relative")).toBe("../seed.txt");
  });

  it("rejects copying/moving a tree into itself, including through a symlink", async () => {
    await fs.mkdir("/tree");
    await fs.symlink("tree", "/alias");
    await expect(fs.cp("/tree", "/alias/nested", { recursive: true })).rejects.toMatchObject({ code: "EINVAL" });
    await expect(fs.mv("/tree", "/tree/nested")).rejects.toMatchObject({ code: "EINVAL" });
    await expect(fs.cp("/seed.txt", "/seed.txt")).rejects.toMatchObject({ code: "EINVAL" });
    await fs.symlink("seed.txt", "/seed-alias");
    await expect(fs.cp("/seed.txt", "/seed-alias")).rejects.toMatchObject({ code: "EINVAL" });
  });

  it("preserves permission bits on newly copied files and directories", async () => {
    await fs.mkdir("/tools");
    await fs.writeFile("/tools/run", "script");
    await fs.chmod("/tools/run", 0o755);
    await fs.chmod("/tools", 0o700);
    await fs.cp("/tools", "/backup", { recursive: true });
    expect((await fs.stat("/backup")).mode).toBe(0o700);
    expect((await fs.stat("/backup/run")).mode).toBe(0o755);
  });

  it("renames files, replaces files, and renames directories", async () => {
    await fs.writeFile("/target", "old");
    await fs.mv("/seed.txt", "/target");
    expect(await fs.readFile("/target")).toBe("hello");
    expect(await fs.exists("/seed.txt")).toBe(false);
    await fs.mkdir("/dir");
    await fs.writeFile("/dir/file", "x");
    await fs.mv("/dir", "/renamed");
    expect(await fs.readFile("/renamed/file")).toBe("x");
  });

  it("handles Unicode and control characters in rename/hardlink source headers", async () => {
    await fs.writeFile("/文件\n.txt", "content");
    await fs.link("/文件\n.txt", "/hardlink");
    await fs.mv("/文件\n.txt", "/移動.txt");
    expect(await fs.readFile("/移動.txt")).toBe("content");
    expect(await fs.readFile("/hardlink")).toBe("content");
    await client.copy("/tenant/work/移動.txt", "/tenant/work/copied-by-sdk");
    expect(await fs.readFile("/copied-by-sdk")).toBe("content");
  });

  it("preserves trailing spaces in copy, rename and hardlink source headers", async () => {
    await fs.writeFile("/spaced ", "content");
    await client.copy("/tenant/work/spaced ", "/tenant/work/copied");
    await fs.link("/spaced ", "/hard");
    await fs.mv("/spaced ", "/moved");
    for (const path of ["/copied", "/hard", "/moved"]) {
      expect(await fs.readFile(path)).toBe("content");
    }
    expect(await fs.exists("/spaced ")).toBe(false);
  });

  it("resolves relative symlinks in files and intermediate directories", async () => {
    await fs.mkdir("/dir");
    await fs.symlink("../seed.txt", "/dir/link");
    await fs.symlink("dir", "/dir-alias");
    expect(await fs.readFile("/dir-alias/link")).toBe("hello");
    expect(await fs.realpath("/dir-alias/link")).toBe("/seed.txt");
    expect((await fs.lstat("/dir/link")).isSymbolicLink).toBe(true);
    expect((await fs.stat("/dir/link")).isFile).toBe(true);
    expect(await fs.readlink("/dir/link")).toBe("../seed.txt");
    await fs.writeFile("/dir-alias/link", "updated");
    expect(await fs.readFile("/seed.txt")).toBe("updated");
    await fs.rm("/dir/link");
    expect(await fs.exists("/seed.txt")).toBe(true);
  });

  it("supports dangling links without following them for removal", async () => {
    await fs.symlink("later", "/dangling");
    expect(await fs.exists("/dangling")).toBe(false);
    expect((await fs.lstat("/dangling")).isSymbolicLink).toBe(true);
    await fs.writeFile("/dangling", "created");
    expect(await fs.readFile("/later")).toBe("created");
    await fs.rm("/later");
    await fs.rm("/dangling");
    expect(await fs.exists("/dangling")).toBe(false);
  });

  it("does not turn dangling links or trailing-slash paths into new entries", async () => {
    await fs.symlink("missing-dir", "/dangling-dir");
    await expect(fs.mkdir("/dangling-dir", { recursive: true })).rejects.toMatchObject({ code: "EEXIST" });
    expect(await fs.exists("/missing-dir")).toBe(false);
    await fs.mkdir("/dir");
    await fs.symlink("dir", "/dir-link");
    await fs.mkdir("/dir-link", { recursive: true });
    await fs.mkdir("/dir-link/child", { recursive: true });
    expect((await fs.stat("/dir/child")).isDirectory).toBe(true);
    await expect(fs.writeFile("/absent/", "x")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.appendFile("/absent/", "x")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.cp("/seed.txt", "/absent/")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.mv("/seed.txt", "/absent/")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.symlink("seed.txt", "/absent/")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.link("/seed.txt", "/absent/")).rejects.toMatchObject({ code: "ENOENT" });
    expect(await fs.exists("/absent")).toBe(false);
  });

  it("retains trailing-slash directory checks through symbolic links", async () => {
    await fs.symlink("seed.txt", "/link");
    await fs.symlink("seed.txt/", "/slash-target");
    await fs.symlink("missing/", "/dangling-slash");
    for (const path of ["/link/", "/slash-target"]) {
      await expect(fs.stat(path)).rejects.toMatchObject({ code: "ENOTDIR" });
      await expect(fs.readFile(path)).rejects.toMatchObject({ code: "ENOTDIR" });
      await expect(fs.writeFile(path, "changed")).rejects.toMatchObject({ code: "ENOTDIR" });
    }
    await expect(fs.writeFile("/dangling-slash", "x")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.appendFile("/dangling-slash", "x")).rejects.toMatchObject({ code: "ENOENT" });
    expect(await fs.exists("/missing")).toBe(false);
    expect(await fs.readFile("/seed.txt")).toBe("hello");
    await fs.mkdir("/dir");
    await fs.symlink("dir/", "/dir-link");
    expect((await fs.stat("/dir-link/")).isDirectory).toBe(true);
  });

  it("detects link loops and blocks links that escape the remote root", async () => {
    await fs.symlink("b", "/a");
    await fs.symlink("a", "/b");
    await expect(fs.readFile("/a")).rejects.toMatchObject({ code: "ELOOP" });
    await expect(fs.symlink("../secret", "/escape")).rejects.toMatchObject({ code: "EACCES" });
    await expect(fs.symlink("/tenant/secret", "/absolute")).rejects.toMatchObject({ code: "ENOTSUP" });
    fixture.put("/tenant/work/existing-escape", "../secret", 0o120777);
    fixture.put("/tenant/secret", "private");
    fixture.calls.length = 0;
    await expect(fs.readFile("/existing-escape")).rejects.toMatchObject({ code: "EACCES" });
    expect(fixture.calls.every((call) => call.path.startsWith("/tenant/work"))).toBe(true);
  });

  it("implements hardlinks and chmod while rejecting unsupported timestamps", async () => {
    await fs.link("/seed.txt", "/hard");
    await fs.writeFile("/hard", "shared");
    expect(await fs.readFile("/seed.txt")).toBe("shared");
    expect((await fs.stat("/hard")).identity).toBe((await fs.stat("/seed.txt")).identity);
    await expect(fs.cp("/seed.txt", "/hard")).rejects.toMatchObject({ code: "EINVAL" });
    await fs.chmod("/hard", 0o600);
    expect((await fs.stat("/seed.txt")).mode).toBe(0o600);
    await expect(fs.utimes("/hard", new Date(), new Date())).rejects.toMatchObject({ code: "ENOTSUP" });
  });

  it("rejects hardlinking symlinks while still resolving parent symlinks", async () => {
    await fs.symlink("seed.txt", "/link");
    await fs.symlink("missing", "/dangling");
    for (const path of ["/link", "/dangling"]) {
      await expect(fs.link(path, "/hard")).rejects.toMatchObject({ code: "EPERM" });
      expect(await fs.exists("/hard")).toBe(false);
    }
    expect(fixture.calls.some((call) => call.query.includes("hardlink"))).toBe(false);
    expect(await fs.readFile("/seed.txt")).toBe("hello");
    await fs.mkdir("/dir");
    await fs.writeFile("/dir/file", "content");
    await fs.symlink("dir", "/dir-link");
    await fs.link("/dir-link/file", "/hard");
    expect((await fs.stat("/hard")).identity).toBe((await fs.stat("/dir/file")).identity);
  });

  it("protects the root and refuses a file as the configured root", async () => {
    await expect(fs.rm("/", { recursive: true })).rejects.toMatchObject({ code: "EBUSY" });
    await expect(fs.mv("/", "/gone")).rejects.toMatchObject({ code: "EBUSY" });
    await expect(fs.mv("/seed.txt", "/")).rejects.toMatchObject({ code: "EBUSY" });
    const invalid = new Drive9Fs(client, { root: "/tenant/work/seed.txt" });
    await expect(invalid.readdir("/")).rejects.toMatchObject({ code: "ENOTDIR" });
  });

  it("enforces read-only mode for every mutation before making requests", async () => {
    const readonly = new Drive9Fs(client, { root: "/tenant/work", readOnly: true });
    expect(await readonly.readFile("/seed.txt")).toBe("hello");
    fixture.calls.length = 0;
    for (const operation of [
      () => readonly.writeFile("/x", "x"), () => readonly.appendFile("/x", "x"),
      () => readonly.mkdir("/x"), () => readonly.rm("/seed.txt"),
      () => readonly.cp("/seed.txt", "/x"), () => readonly.mv("/seed.txt", "/x"),
      () => readonly.chmod("/seed.txt", 0o600), () => readonly.symlink("seed.txt", "/x"),
      () => readonly.link("/seed.txt", "/x"), () => readonly.utimes("/seed.txt", new Date(), new Date()),
    ]) await expect(operation()).rejects.toMatchObject({ code: "EROFS" });
    expect(fixture.calls).toEqual([]);
  });

  it.each([[403, "EACCES"], [429, "EAGAIN"], [500, "EIO"]])(
    "does not turn HTTP %i failures into missing files", async (status, code) => {
      server.use(http.head(`${fixture.base}/v1/fs/tenant/work/seed.txt`, () => new HttpResponse(null, { status })));
      await expect(fs.exists("/seed.txt")).rejects.toMatchObject({ code });
      await expect(fs.rm("/seed.txt", { force: true })).rejects.toMatchObject({ code });
    },
  );

  it("surfaces concurrent append conflicts without overwriting a newer revision", async () => {
    server.use(http.put(`${fixture.base}/v1/fs/tenant/work/seed.txt`, () =>
      HttpResponse.json({ error: "revision mismatch", server_revision: 9 }, { status: 409 })));
    await expect(fs.appendFile("/seed.txt", "more")).rejects.toMatchObject({ code: "EAGAIN" });
    expect(await fs.readFile("/seed.txt")).toBe("hello");
  });

  it("rejects invalid paths and malformed directory listings", async () => {
    for (const path of ["/bad\\path", "/bad\0path", "/bad\uD800"]) {
      await expect(fs.writeFile(path, "x")).rejects.toMatchObject({ code: "EINVAL" });
    }
    server.use(http.get(`${fixture.base}/v1/fs/tenant/work/`, () =>
      HttpResponse.json({ entries: [{ name: "../outside", isDir: false }] })));
    await expect(fs.readdir("/")).rejects.toMatchObject({ code: "EIO" });
  });
});

const [major, minor] = process.versions.node.split(".").map(Number);
const supportsJustBash = major > 20 || (major === 20 && minor >= 19);

describe.skipIf(!supportsJustBash)("Just Bash commands over Drive9 HTTP (Node >=20.19)", () => {
  let runtime: typeof import("just-bash");
  beforeAll(async () => { runtime = await import("just-bash"); });
  const shell = () => {
    const mounts = new runtime.MountableFs({ base: new runtime.InMemoryFs() });
    mounts.mount("/workspace", new Drive9Fs(client, { root: "/tenant/work" }));
    return new runtime.Bash({ fs: mounts, cwd: "/workspace" });
  };

  it("runs redirects, pipes, grep, globs, find, copy, move and removal", async () => {
    const bash = shell();
    const result = await bash.exec(`
      mkdir -p notes/sub
      printf 'alpha\\nbeta\\n' > notes/a.txt
      printf 'gamma\\n' >> notes/a.txt
      cat notes/*.txt | grep beta > notes/sub/match.txt
      cp notes/a.txt notes/copy.txt
      mv notes/copy.txt notes/moved.txt
      find notes -type f | sort
      cat notes/sub/match.txt
      rm notes/moved.txt
    `);
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("notes/sub/match.txt");
    expect(result.stdout).toContain("beta\n");
    expect(await shell().exec("cat notes/a.txt")).toMatchObject({ stdout: "alpha\nbeta\ngamma\n", exitCode: 0 });
    expect(await fs.exists("/notes/moved.txt")).toBe(false);
  });

  it("keeps scratch files in memory and copies across mounts", async () => {
    const bash = shell();
    expect(await bash.exec("cp seed.txt /tmp/scratch; cat /tmp/scratch")).toMatchObject({ stdout: "hello", exitCode: 0 });
    expect(await fs.exists("/tmp/scratch")).toBe(false);
    expect(await shell().exec("cat /tmp/scratch")).toMatchObject({ exitCode: 1 });
  });

  it("supports shell symlinks, recursive copies and binary pipelines", async () => {
    const bash = shell();
    const result = await bash.exec(`
      mkdir originals
      printf 'value' > originals/file
      ln -s file originals/link
      cp -r originals duplicate
      rm -r originals
      cat duplicate/link
    `);
    expect(result).toMatchObject({ stdout: "value", stderr: "", exitCode: 0 });
    await fs.writeFile("/binary", new Uint8Array([0, 255, 128, 10]));
    expect(await bash.exec("cat binary | cat > roundtrip")).toMatchObject({ stderr: "", exitCode: 0 });
    expect(await fs.readFileBuffer("/roundtrip")).toEqual(new Uint8Array([0, 255, 128, 10]));
  });

  it("rejects shell hardlinks to symlinks without creating an alias of the referent", async () => {
    const result = await shell().exec("ln -s seed.txt sym; ln sym hard");
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("ln:");
    expect(await fs.exists("/hard")).toBe(false);
    expect(await fs.readlink("/sym")).toBe("seed.txt");
  });

  it("treats an existence check below a file as a false condition", async () => {
    expect(await shell().exec("test -e seed.txt/child; echo status=$?")).toMatchObject({
      stdout: "status=1\n", stderr: "", exitCode: 0,
    });
  });

  it("repeats recursive copies containing symlinks", async () => {
    const bash = shell();
    expect(await bash.exec("mkdir src; ln -s ../seed.txt src/link; cp -r src dst")).toMatchObject({
      stderr: "", exitCode: 0,
    });
    expect(await bash.exec("cp -r src/. dst")).toMatchObject({ stderr: "", exitCode: 0 });
    expect(await fs.readlink("/dst/link")).toBe("../seed.txt");
    expect(await fs.readFile("/dst/link")).toBe("hello");
    expect(await fs.readlink("/src/link")).toBe("../seed.txt");
  });

  it("surfaces command errors and rejected redirects", async () => {
    expect(await shell().exec("cat missing")).toMatchObject({ exitCode: 1 });
    // just-bash 3.6 propagates a redirect's missing-parent failure to its caller.
    await expect(shell().exec("echo x > missing/child")).rejects.toMatchObject({ code: "ENOENT" });
  });
});
