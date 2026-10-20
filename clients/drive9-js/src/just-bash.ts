import { AsyncResource } from "node:async_hooks";
import { posix } from "node:path";
import type { BufferEncoding, CpOptions, FileContent, FsStat, IFileSystem, MkdirOptions, RmOptions } from "just-bash";

import type { Client } from "./client.js";
import { ConflictError, Drive9Error, StatusError } from "./error.js";
import type { StatResult } from "./models.js";

// These option types are not exported from just-bash's public entry point.
type ReadOptions = Parameters<IFileSystem["readFile"]>[1];
type WriteOptions = Parameters<IFileSystem["writeFile"]>[2];
type Dirent = Awaited<ReturnType<NonNullable<IFileSystem["readdirWithFileTypes"]>>>[number];

export interface Drive9FsOptions {
  /** Existing remote directory exposed as "/". Defaults to the tenant root. */
  root?: string;
  /** Reject mutations through this adapter. Server-side scoped tokens are still recommended. */
  readOnly?: boolean;
}

/** Filesystem errors carry both a Node-style code and the prefix used by just-bash. */
export class Drive9FsError extends Error {
  constructor(public readonly code: string, message: string, options?: ErrorOptions) {
    super(`${code}: ${message}`, options);
    this.name = "Drive9FsError";
  }
}

/**
 * A write-through just-bash filesystem backed by the Drive9 HTTP SDK.
 *
 * No metadata/content cache or host filesystem access is used. Relative symlinks
 * are resolved inside the remote root. Absolute symlinks, timestamp updates and
 * atomic private-file creation are unsupported; see the integration guide.
 */
export class Drive9Fs implements IFileSystem {
  private readonly root: string;
  private readonly readOnly: boolean;
  // Construct in the host before bash.exec(). Network clients and their timers
  // need the host async context, not just-bash's instrumented guest context.
  private readonly ioContext = new AsyncResource("Drive9Fs");

  constructor(private readonly client: Client, options: Drive9FsOptions = {}) {
    if (options.root !== undefined && !options.root.startsWith("/")) {
      throw new Drive9FsError("EINVAL", "root must be an absolute Drive9 directory");
    }
    this.root = this.normalize(options.root ?? "/");
    this.readOnly = options.readOnly ?? false;
  }

  resolvePath(base: string, path: string): string {
    this.validate(base);
    this.validate(path);
    return posix.resolve("/", base, path).normalize("NFC");
  }

  // Remote enumeration is asynchronous. just-bash expands globs via readdir.
  getAllPaths(): string[] {
    return [];
  }

  async readFile(path: string, options?: ReadOptions): Promise<string> {
    return Buffer.from(await this.readFileBuffer(path)).toString(encoding(options));
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    const resolved = await this.resolve(path);
    this.requireFile(resolved.info!, path);
    return this.io(path, async () => {
      const stream = await this.client.readStream(this.remote(resolved.path));
      return new Uint8Array(await new Response(stream).arrayBuffer());
    });
  }

  async writeFile(path: string, content: FileContent, options?: WriteOptions): Promise<void> {
    this.writable(path);
    const resolved = await this.resolve(path, true, true);
    if (resolved.info) this.requireFile(resolved.info, path);
    const data = bytes(content, options);
    await this.io(path, () => this.client.writeStream(this.remote(resolved.path), data, data.length));
  }

  async appendFile(path: string, content: FileContent, options?: WriteOptions): Promise<void> {
    this.writable(path);
    const resolved = await this.resolve(path, true, true);
    if (resolved.info) this.requireFile(resolved.info, path);
    // The SDK uses revision-checked native append or a bounded small-file rewrite.
    await this.io(path, () => this.client.append(this.remote(resolved.path), bytes(content, options)));
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.stat(path);
      return true;
    } catch (error) {
      if (isCode(error, "ENOENT")) return false;
      throw error;
    }
  }

  async stat(path: string): Promise<FsStat> {
    const resolved = await this.resolve(path);
    return this.toStat(resolved.info!);
  }

  async lstat(path: string): Promise<FsStat> {
    const resolved = await this.resolve(path, false);
    return this.toStat(resolved.info!);
  }

  async realpath(path: string): Promise<string> {
    return (await this.resolve(path)).path;
  }

  async mkdir(path: string, options?: MkdirOptions): Promise<void> {
    this.writable(path);
    const normalized = this.normalize(path);
    if (options?.recursive && normalized !== "/") {
      await this.mkdir(posix.dirname(normalized), { recursive: true });
    }
    const resolved = await this.resolve(normalized, false, true);
    if (resolved.info) {
      if (options?.recursive && resolved.info.isDir) return;
      if (options?.recursive && isSymlink(resolved.info)) {
        try {
          if ((await this.stat(normalized)).isDirectory) return;
        } catch (error) {
          if (!isCode(error, "ENOENT")) throw error;
        }
      }
      throw new Drive9FsError("EEXIST", `path already exists, mkdir '${path}'`);
    }
    try {
      await this.io(path, () => this.client.mkdir(this.remote(resolved.path, true)));
    } catch (error) {
      // mkdir -p remains idempotent if another client creates the directory.
      if (options?.recursive && isCode(error, "EEXIST") && (await this.stat(path)).isDirectory) return;
      throw error;
    }
  }

  async readdir(path: string): Promise<string[]> {
    return (await this.readdirWithFileTypes(path)).map((entry) => entry.name);
  }

  async readdirWithFileTypes(path: string): Promise<Dirent[]> {
    const resolved = await this.resolve(path);
    if (!resolved.info!.isDir) throw new Drive9FsError("ENOTDIR", `not a directory '${path}'`);
    const entries = await this.io(path, () => this.client.list(this.remote(resolved.path, true)));
    return entries.map((entry) => {
      // Never let a malformed listing turn a recursive copy into a traversal.
      if (!entry.name || entry.name === "." || entry.name === ".." || entry.name.includes("/")) {
        throw new Drive9FsError("EIO", `invalid directory entry in '${path}'`);
      }
      this.validate(entry.name);
      const isSymbolicLink = isSymlink(entry);
      return {
        name: entry.name,
        isFile: !entry.isDir && !isSymbolicLink,
        isDirectory: entry.isDir,
        isSymbolicLink,
      };
    }).sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  }

  async rm(path: string, options?: RmOptions): Promise<void> {
    this.writable(path);
    this.notRoot(path);
    let resolved: Resolved;
    try {
      resolved = await this.resolve(path, false);
    } catch (error) {
      if (options?.force && isCode(error, "ENOENT")) return;
      throw error;
    }
    const remote = this.remote(resolved.path, resolved.info!.isDir);
    await this.io(path, async () => {
      try {
        if (resolved.info!.isDir) {
          if (options?.recursive) await this.client.removeAll(remote);
          else {
            if ((await this.readdir(path)).length) {
              throw new Drive9FsError("ENOTEMPTY", `directory not empty '${path}'`);
            }
            try {
              await this.client.deleteDir(remote);
            } catch (error) {
              if (error instanceof StatusError && error.statusCode === 409) {
                throw new Drive9FsError("ENOTEMPTY", `directory not empty '${path}'`, { cause: error });
              }
              throw error;
            }
          }
        } else {
          await this.client.deleteFile(remote);
        }
      } catch (error) {
        if (options?.force && error instanceof StatusError && error.statusCode === 404) return;
        throw error;
      }
    });
  }

  async cp(src: string, dest: string, options?: CpOptions): Promise<void> {
    this.writable(dest);
    const source = await this.resolve(src, false);
    const target = await this.resolve(dest, false, true);
    if (source.path === target.path || (source.info!.isDir && within(source.path, target.path))) {
      throw new Drive9FsError("EINVAL", `cannot copy '${src}' into itself '${dest}'`);
    }
    if (source.info!.resource_id && source.info!.resource_id === target.info?.resource_id) {
      throw new Drive9FsError("EINVAL", `source and destination are the same file '${dest}'`);
    }
    if (source.info!.isDir) {
      if (!options?.recursive) throw new Drive9FsError("EISDIR", `recursive copy required '${src}'`);
      if (target.info && !target.info.isDir) throw new Drive9FsError("ENOTDIR", `not a directory '${dest}'`);
      await this.mkdir(target.path, { recursive: true });
      for (const name of await this.readdir(source.path)) {
        await this.cp(posix.join(source.path, name), posix.join(target.path, name), options);
      }
      if (!target.info && source.info!.mode !== undefined && (source.info!.mode & 0o7777) !== 0o755) {
        await this.chmod(target.path, source.info!.mode);
      }
    } else if (isSymlink(source.info!)) {
      if (target.info) throw new Drive9FsError("EEXIST", `copy destination exists '${dest}'`);
      await this.symlink(await this.readlink(source.path), target.path);
    } else {
      const fileTarget = await this.resolve(dest, true, true);
      if (source.info!.resource_id && source.info!.resource_id === fileTarget.info?.resource_id) {
        throw new Drive9FsError("EINVAL", `source and destination are the same file '${dest}'`);
      }
      // Drive9's ?copy creates an alias sharing the source entity. A shell copy
      // must be independent, so transfer the bytes instead.
      await this.writeFile(fileTarget.path, await this.readFileBuffer(source.path));
      if (!fileTarget.info && source.info!.mode !== undefined && (source.info!.mode & 0o7777) !== 0o644) {
        await this.chmod(fileTarget.path, source.info!.mode);
      }
    }
  }

  async mv(src: string, dest: string): Promise<void> {
    this.writable(src);
    this.notRoot(src);
    this.notRoot(dest);
    const source = await this.resolve(src, false);
    const target = await this.resolve(dest, false, true);
    if (source.path === target.path) return;
    if (source.info!.isDir && within(source.path, target.path)) {
      throw new Drive9FsError("EINVAL", `cannot move '${src}' into itself '${dest}'`);
    }
    if (target.info && source.info!.isDir !== target.info.isDir) {
      throw new Drive9FsError(target.info.isDir ? "EISDIR" : "ENOTDIR", `incompatible destination '${dest}'`);
    }
    if (target.info?.isDir) {
      // The server's directory rename does not atomically replace a directory.
      throw new Drive9FsError("EEXIST", `directory destination exists '${dest}'`);
    }
    await this.io(dest, () => this.client.rename(
      this.remote(source.path, source.info!.isDir),
      this.remote(target.path, source.info!.isDir),
    ));
  }

  async chmod(path: string, mode: number): Promise<void> {
    this.writable(path);
    if (!Number.isInteger(mode) || mode < 0) throw new Drive9FsError("EINVAL", "invalid mode");
    const resolved = await this.resolve(path);
    await this.io(path, () => this.client.chmod(this.remote(resolved.path, resolved.info!.isDir), mode & 0o7777));
  }

  async symlink(target: string, linkPath: string): Promise<void> {
    this.writable(linkPath);
    const resolved = await this.resolve(linkPath, false, true);
    if (resolved.info) throw new Drive9FsError("EEXIST", `path already exists '${linkPath}'`);
    this.linkTarget(resolved.path, target);
    await this.io(linkPath, () => this.client.symlink(target, this.remote(resolved.path)));
  }

  async link(existingPath: string, newPath: string): Promise<void> {
    this.writable(newPath);
    const source = await this.resolve(existingPath);
    this.requireFile(source.info!, existingPath);
    const target = await this.resolve(newPath, false, true);
    if (target.info) throw new Drive9FsError("EEXIST", `path already exists '${newPath}'`);
    await this.io(newPath, () => this.client.hardlink(this.remote(source.path), this.remote(target.path)));
  }

  async readlink(path: string): Promise<string> {
    const resolved = await this.resolve(path, false);
    if (!isSymlink(resolved.info!)) throw new Drive9FsError("EINVAL", `not a symbolic link '${path}'`);
    return this.rawLink(resolved.path);
  }

  async utimes(path: string, _atime: Date, _mtime: Date): Promise<void> {
    this.writable(path);
    await this.stat(path);
    throw new Drive9FsError("ENOTSUP", "Drive9 has no timestamp-update API");
  }

  private validate(path: string): void {
    if (path.includes("\0") || path.includes("\\") || /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(path)) {
      throw new Drive9FsError("EINVAL", "invalid Drive9 path");
    }
  }

  private normalize(path: string): string {
    if (!path) throw new Drive9FsError("ENOENT", "empty path");
    return this.resolvePath("/", path);
  }

  private remote(path: string, directory = false): string {
    const remote = this.root === "/" ? path : this.root + (path === "/" ? "" : path);
    return directory && remote !== "/" ? `${remote}/` : remote;
  }

  private writable(path: string): void {
    if (this.readOnly) throw new Drive9FsError("EROFS", `read-only filesystem '${path}'`);
  }

  private notRoot(path: string): void {
    if (this.normalize(path) === "/") throw new Drive9FsError("EBUSY", "cannot remove or replace the filesystem root");
  }

  private requireFile(info: StatResult, path: string): void {
    if (info.isDir) throw new Drive9FsError("EISDIR", `is a directory '${path}'`);
  }

  private toStat(info: StatResult): FsStat {
    const symlink = isSymlink(info);
    return {
      isFile: !info.isDir && !symlink,
      isDirectory: info.isDir,
      isSymbolicLink: symlink,
      mode: info.mode ?? (info.isDir ? 0o755 : 0o644),
      size: info.size,
      mtime: info.mtime ?? new Date(0),
      identity: info.resource_id,
    };
  }

  private async rawStat(path: string): Promise<StatResult> {
    return this.io(path, () => this.client.stat(this.remote(path)));
  }

  private async rawLink(path: string): Promise<string> {
    // Drive9 stores symlink targets as the file payload; HEAD is lstat-like.
    return this.io(path, async () => Buffer.from(await this.client.read(this.remote(path))).toString("utf8"));
  }

  private linkTarget(link: string, target: string): string {
    this.validate(target);
    if (!target) throw new Drive9FsError("EINVAL", "empty symbolic link target");
    // MountableFs forwards absolute targets unchanged, so they cannot safely be
    // interpreted as either a mount path or a remote tenant path here.
    if (posix.isAbsolute(target)) throw new Drive9FsError("ENOTSUP", "use relative symbolic links with Drive9Fs");
    const components = posix.dirname(link).split("/").filter(Boolean);
    for (const part of target.split("/")) {
      if (part === "..") {
        if (!components.length) throw new Drive9FsError("EACCES", "symbolic link escapes the filesystem root");
        components.pop();
      } else if (part && part !== ".") {
        components.push(part);
      }
    }
    return "/" + components.join("/").normalize("NFC");
  }

  /** Resolve parents and optionally the last link, never leaving the virtual root. */
  private async resolve(path: string, follow = true, allowMissing = false, depth = 0): Promise<Resolved> {
    if (depth > 40) throw new Drive9FsError("ELOOP", `too many symbolic links '${path}'`);
    const normalized = this.normalize(path);
    const rootInfo = await this.rawStat("/");
    if (!rootInfo.isDir) throw new Drive9FsError("ENOTDIR", "Drive9Fs root must be a directory");
    if (normalized === "/") return { path: "/", info: rootInfo };
    const parts = normalized.slice(1).split("/");
    let current = "";
    for (let i = 0; i < parts.length; i++) {
      current += "/" + parts[i];
      const last = i === parts.length - 1;
      let info: StatResult;
      try {
        info = await this.rawStat(current);
      } catch (error) {
        if (last && allowMissing && !path.endsWith("/") && isCode(error, "ENOENT")) return { path: current };
        throw error;
      }
      if (isSymlink(info) && (!last || follow)) {
        const link = await this.rawLink(current);
        const target = this.linkTarget(current, link);
        const next = posix.join(target, ...parts.slice(i + 1));
        // Normalization strips the slash, but it still requires a directory,
        // including when the link payload itself ends in a slash.
        const directory = path.endsWith("/") || (last && link.endsWith("/"));
        return this.resolve(directory ? next + "/" : next, follow, allowMissing, depth + 1);
      }
      if (!last && !info.isDir) throw new Drive9FsError("ENOTDIR", `not a directory '${current}'`);
      if (last) {
        if (path.endsWith("/") && !info.isDir) throw new Drive9FsError("ENOTDIR", `not a directory '${path}'`);
        return { path: current, info };
      }
    }
    throw new Drive9FsError("ENOENT", `no such file or directory '${path}'`);
  }

  private async io<T>(path: string, operation: () => Promise<T>): Promise<T> {
    try {
      return await this.ioContext.runInAsyncScope(operation);
    } catch (error) {
      if (error instanceof Drive9FsError) throw error;
      let code = "EIO";
      if (error instanceof StatusError) {
        code = ({
          400: "EINVAL", 401: "EACCES", 403: "EACCES", 404: "ENOENT",
          409: "EEXIST", 413: "EFBIG", 429: "EAGAIN", 507: "ENOSPC",
        } as Record<number, string>)[error.statusCode] ?? "EIO";
        if (error instanceof ConflictError && error.serverRevision !== undefined) code = "EAGAIN";
      } else if (error instanceof Drive9Error && error.message.startsWith("not found: ")) {
        // Client.stat's historical 404 error does not carry a status code.
        code = "ENOENT";
      }
      // Avoid exposing host URLs, credentials, or server internals to guest scripts.
      throw new Drive9FsError(code, `Drive9 operation failed '${path}'`, { cause: error });
    }
  }
}

interface Resolved {
  path: string;
  info?: StatResult;
}

function isSymlink(info: { mode?: number }): boolean {
  return ((info.mode ?? 0) & 0o170000) === 0o120000;
}

function isCode(error: unknown, code: string): boolean {
  return error instanceof Drive9FsError && error.code === code;
}

function within(parent: string, path: string): boolean {
  return parent === "/" || path.startsWith(parent + "/");
}

function encoding(options: ReadOptions | WriteOptions): BufferEncoding {
  return (typeof options === "string" ? options : options?.encoding) ?? "utf8";
}

function bytes(content: FileContent, options: WriteOptions): Uint8Array {
  return typeof content === "string" ? Buffer.from(content, encoding(options)) : content;
}
