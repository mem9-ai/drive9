import { Vault, TFile, Modal, App, Setting, Notice } from "obsidian";
import { Drive9Client, Drive9Error } from "./client";
import type { SyncState } from "./types";
import { IgnoreMatcher } from "./ignore";

export type FirstRunResult =
  | { action: "push_all" }
  | { action: "pull_all" }
  | { action: "reconciled"; states: Record<string, SyncState> }
  | { action: "cancelled" };

/**
 * Detect remote state and reconcile with local vault on first run.
 * Safety invariant: never silent overwrite.
 */
export async function runFirstRunReconciliation(
  app: App,
  vault: Vault,
  client: Drive9Client,
  ignorePaths: string[],
): Promise<FirstRunResult> {
  const ignore = new IgnoreMatcher(ignorePaths);

  // Gather local files.
  const localFiles = new Map<string, TFile>();
  for (const file of vault.getFiles()) {
    if (!ignore.isIgnored(file.path)) {
      localFiles.set(file.path, file);
    }
  }

  // Gather remote files.
  let remoteFiles: Map<string, { size: number; mtime: number }>;
  try {
    const entries = await client.listRecursive("/");
    remoteFiles = new Map(
      entries
        .filter((e) => !ignore.isIgnored(e.name))
        .map((e) => [e.name, { size: e.size, mtime: e.mtime }]),
    );
  } catch (e) {
    if (e instanceof Drive9Error && e.status === 404) {
      remoteFiles = new Map();
    } else {
      throw e;
    }
  }

  const localEmpty = localFiles.size === 0;
  const remoteEmpty = remoteFiles.size === 0;

  // Scenario 1: Remote empty, local has content → push all.
  if (remoteEmpty && !localEmpty) {
    return { action: "push_all" };
  }

  // Scenario 2: Remote has content, local empty → ask to pull.
  if (!remoteEmpty && localEmpty) {
    const confirmed = await askUser(
      app,
      "Download from drive9?",
      `drive9 has ${remoteFiles.size} files. Download them to your vault?`,
    );
    return confirmed ? { action: "pull_all" } : { action: "cancelled" };
  }

  // Scenario 3: Both empty → nothing to do.
  if (remoteEmpty && localEmpty) {
    return { action: "reconciled", states: {} };
  }

  // Scenario 4: Both have content → reconcile.
  const onlyLocal: string[] = [];
  const onlyRemote: string[] = [];
  const bothDifferent: string[] = [];
  const states: Record<string, SyncState> = {};

  for (const [path, file] of localFiles) {
    const remote = remoteFiles.get(path);
    if (!remote) {
      onlyLocal.push(path);
    } else if (file.stat.size !== remote.size) {
      bothDifferent.push(path);
    } else {
      // Same size — treat as synced (best effort without checksum).
      states[path] = {
        path,
        localMtime: file.stat.mtime,
        localSize: file.stat.size,
        remoteRevision: 0, // unknown until stat()
        syncedAt: Date.now(),
        status: "synced",
      };
    }
  }

  for (const path of remoteFiles.keys()) {
    if (!localFiles.has(path)) {
      onlyRemote.push(path);
    }
  }

  // If there are conflicts (both have different content), stop and ask.
  if (bothDifferent.length > 0) {
    const msg = [
      `Found ${bothDifferent.length} file(s) with different content on both sides.`,
      "",
      bothDifferent.slice(0, 10).join("\n"),
      bothDifferent.length > 10 ? `...and ${bothDifferent.length - 10} more` : "",
      "",
      "These files need manual resolution. Plugin will not auto-sync until resolved.",
      "For now, only files unique to each side will be synced.",
    ].join("\n");

    new Notice(msg, 15000);
  }

  // Mark only-local files for push, only-remote for pull,
  // both-different as conflict.
  for (const path of onlyLocal) {
    const file = localFiles.get(path)!;
    states[path] = {
      path,
      localMtime: file.stat.mtime,
      localSize: file.stat.size,
      remoteRevision: 0,
      syncedAt: 0,
      status: "local_dirty",
    };
  }

  for (const path of onlyRemote) {
    const remote = remoteFiles.get(path)!;
    states[path] = {
      path,
      localMtime: 0,
      localSize: 0,
      remoteRevision: 0,
      syncedAt: 0,
      status: "remote_dirty",
    };
  }

  for (const path of bothDifferent) {
    const file = localFiles.get(path)!;
    states[path] = {
      path,
      localMtime: file.stat.mtime,
      localSize: file.stat.size,
      remoteRevision: 0,
      syncedAt: 0,
      status: "conflict",
    };
  }

  return { action: "reconciled", states };
}

/**
 * Pull all remote files into the local vault.
 */
export async function pullAllRemote(
  vault: Vault,
  client: Drive9Client,
  syncStates: Record<string, SyncState>,
  ignorePaths: string[],
): Promise<void> {
  const ignore = new IgnoreMatcher(ignorePaths);
  const entries = await client.listRecursive("/");
  let count = 0;

  for (const entry of entries) {
    if (ignore.isIgnored(entry.name)) continue;

    // Ensure parent directories exist.
    const dir = entry.name.contains("/")
      ? entry.name.substring(0, entry.name.lastIndexOf("/"))
      : "";
    if (dir && !vault.getAbstractFileByPath(dir)) {
      await vault.createFolder(dir);
    }

    const data = await client.read(entry.name);
    const existing = vault.getAbstractFileByPath(entry.name);
    if (existing instanceof TFile) {
      await vault.modifyBinary(existing, data);
    } else {
      await vault.createBinary(entry.name, data);
    }

    const file = vault.getAbstractFileByPath(entry.name);
    if (file instanceof TFile) {
      syncStates[entry.name] = {
        path: entry.name,
        localMtime: file.stat.mtime,
        localSize: file.stat.size,
        remoteRevision: 0,
        syncedAt: Date.now(),
        status: "synced",
      };
    }
    count++;
  }

  new Notice(`drive9: downloaded ${count} files`);
}

// ---------------------------------------------------------------------------
// Simple confirmation dialog
// ---------------------------------------------------------------------------

function askUser(app: App, title: string, message: string): Promise<boolean> {
  return new Promise((resolve) => {
    const modal = new ConfirmModal(app, title, message, resolve);
    modal.open();
  });
}

class ConfirmModal extends Modal {
  constructor(
    app: App,
    private title: string,
    private message: string,
    private resolve: (confirmed: boolean) => void,
  ) {
    super(app);
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.createEl("h3", { text: this.title });
    contentEl.createEl("p", { text: this.message });

    new Setting(contentEl)
      .addButton((btn) =>
        btn.setButtonText("Yes").setCta().onClick(() => {
          this.resolve(true);
          this.close();
        }),
      )
      .addButton((btn) =>
        btn.setButtonText("Cancel").onClick(() => {
          this.resolve(false);
          this.close();
        }),
      );
  }

  onClose(): void {
    this.contentEl.empty();
  }
}
