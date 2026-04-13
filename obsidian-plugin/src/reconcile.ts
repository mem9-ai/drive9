import { App, Modal, Setting, TFile, Notice } from "obsidian";
import { Drive9Client } from "./client";
import type { FileInfo, ReconcileAction, SyncStateEntry } from "./types";
import { shouldIgnore } from "./ignore";

/**
 * First-run reconciliation: compare local vault with remote drive9,
 * determine safe actions, and ask the user before doing anything destructive.
 */
export async function reconcile(
  app: App,
  client: Drive9Client,
  ignorePaths: string[],
  maxFileSize: number,
): Promise<Map<string, SyncStateEntry> | null> {
  const notice = new Notice("drive9: Checking remote status...", 0);
  try {
    const remoteFiles = await client.listRecursive("/");
    const localFiles = app.vault.getFiles();

    const actions: ReconcileAction[] = [];
    const localPaths = new Set<string>();

    // Compare local files against remote
    for (const file of localFiles) {
      if (shouldIgnore(file.path, ignorePaths)) continue;
      if (file.stat.size > maxFileSize) continue;

      const remotePath = "/" + file.path;
      localPaths.add(remotePath);
      const remote = remoteFiles.get(remotePath);

      if (!remote) {
        // Only local → push
        actions.push({
          path: file.path,
          action: "push",
          localSize: file.stat.size,
        });
      } else if (remote.size === file.stat.size) {
        // Same size → assume synced (no checksum available)
        actions.push({ path: file.path, action: "skip" });
      } else {
        // Both exist but different → conflict
        actions.push({
          path: file.path,
          action: "conflict",
          localSize: file.stat.size,
          remoteSize: remote.size,
        });
      }
    }

    // Check for remote-only files
    for (const [remotePath, info] of remoteFiles) {
      if (localPaths.has(remotePath)) continue;
      const vaultPath = remotePath.startsWith("/")
        ? remotePath.slice(1)
        : remotePath;
      if (shouldIgnore(vaultPath, ignorePaths)) continue;
      actions.push({
        path: vaultPath,
        action: "pull",
        remoteSize: info.size,
      });
    }

    notice.hide();

    // Scenario 1: remote is empty → proceed with push
    if (remoteFiles.size === 0) {
      new Notice("drive9: Remote is empty, starting sync.");
      return buildInitialSyncState(localFiles, ignorePaths, maxFileSize);
    }

    // Scenario 2: conflicts exist → stop and ask
    const conflicts = actions.filter((a) => a.action === "conflict");
    const pulls = actions.filter((a) => a.action === "pull");
    const pushes = actions.filter((a) => a.action === "push");

    if (conflicts.length > 0) {
      return new Promise((resolve) => {
        new ReconcileModal(
          app,
          client,
          actions,
          ignorePaths,
          maxFileSize,
          resolve,
        ).open();
      });
    }

    // No conflicts, but have pulls or pushes → show summary
    if (pulls.length > 0 || pushes.length > 0) {
      return new Promise((resolve) => {
        new ReconcileModal(
          app,
          client,
          actions,
          ignorePaths,
          maxFileSize,
          resolve,
        ).open();
      });
    }

    // Everything matches
    new Notice("drive9: All files in sync.");
    return buildInitialSyncState(localFiles, ignorePaths, maxFileSize);
  } catch (err) {
    notice.hide();
    new Notice(
      `drive9: Failed to check remote — ${err instanceof Error ? err.message : String(err)}`,
    );
    return null;
  }
}

function buildInitialSyncState(
  files: TFile[],
  ignorePaths: string[],
  maxFileSize: number,
): Map<string, SyncStateEntry> {
  const state = new Map<string, SyncStateEntry>();
  const now = Date.now();
  for (const file of files) {
    if (shouldIgnore(file.path, ignorePaths)) continue;
    if (file.stat.size > maxFileSize) continue;
    state.set(file.path, {
      path: file.path,
      localMtime: file.stat.mtime,
      localSize: file.stat.size,
      remoteRevision: 0,
      syncedAt: now,
      status: "local_dirty", // will be pushed on first sync cycle
    });
  }
  return state;
}

class ReconcileModal extends Modal {
  private actions: ReconcileAction[];
  private client: Drive9Client;
  private ignorePaths: string[];
  private maxFileSize: number;
  private resolve: (
    state: Map<string, SyncStateEntry> | null,
  ) => void;

  constructor(
    app: App,
    client: Drive9Client,
    actions: ReconcileAction[],
    ignorePaths: string[],
    maxFileSize: number,
    resolve: (state: Map<string, SyncStateEntry> | null) => void,
  ) {
    super(app);
    this.actions = actions;
    this.client = client;
    this.ignorePaths = ignorePaths;
    this.maxFileSize = maxFileSize;
    this.resolve = resolve;
  }

  onOpen(): void {
    const { contentEl } = this;
    contentEl.empty();
    contentEl.createEl("h2", { text: "drive9: First-run Sync" });

    const conflicts = this.actions.filter((a) => a.action === "conflict");
    const pulls = this.actions.filter((a) => a.action === "pull");
    const pushes = this.actions.filter((a) => a.action === "push");

    if (conflicts.length > 0) {
      contentEl.createEl("p", {
        text: `${conflicts.length} file(s) differ between local and remote. ` +
          `These will be skipped until you resolve them manually.`,
      });
      const list = contentEl.createEl("ul");
      for (const c of conflicts.slice(0, 20)) {
        list.createEl("li", {
          text: `${c.path} (local: ${fmtSize(c.localSize)}, remote: ${fmtSize(c.remoteSize)})`,
        });
      }
      if (conflicts.length > 20) {
        list.createEl("li", {
          text: `... and ${conflicts.length - 20} more`,
        });
      }
    }

    const summary: string[] = [];
    if (pushes.length > 0) summary.push(`${pushes.length} to upload`);
    if (pulls.length > 0) summary.push(`${pulls.length} to download`);
    if (summary.length > 0) {
      contentEl.createEl("p", { text: summary.join(", ") + "." });
    }

    new Setting(contentEl)
      .addButton((btn) =>
        btn
          .setButtonText("Proceed")
          .setCta()
          .onClick(async () => {
            this.close();
            await this.executeReconcile(pulls, pushes, conflicts);
          }),
      )
      .addButton((btn) =>
        btn.setButtonText("Cancel").onClick(() => {
          this.close();
          this.resolve(null);
        }),
      );
  }

  private async executeReconcile(
    pulls: ReconcileAction[],
    pushes: ReconcileAction[],
    conflicts: ReconcileAction[],
  ): Promise<void> {
    const state = new Map<string, SyncStateEntry>();
    const now = Date.now();

    // Pull remote-only files
    let pullCount = 0;
    for (const action of pulls) {
      try {
        const data = await this.client.read("/" + action.path);
        const dir = action.path.split("/").slice(0, -1).join("/");
        if (dir && !this.app.vault.getAbstractFileByPath(dir)) {
          await this.app.vault.createFolder(dir);
        }
        await this.app.vault.createBinary(action.path, data);
        const stat = await this.client.stat("/" + action.path);
        state.set(action.path, {
          path: action.path,
          localMtime: now,
          localSize: data.byteLength,
          remoteRevision: stat?.revision ?? 0,
          syncedAt: now,
          status: "synced",
        });
        pullCount++;
      } catch (err) {
        console.error(`drive9: Failed to pull ${action.path}:`, err);
      }
    }

    // Mark local-only files as dirty (will push on next cycle)
    for (const action of pushes) {
      const file = this.app.vault.getAbstractFileByPath(action.path);
      if (file instanceof TFile) {
        state.set(action.path, {
          path: action.path,
          localMtime: file.stat.mtime,
          localSize: file.stat.size,
          remoteRevision: 0,
          syncedAt: now,
          status: "local_dirty",
        });
      }
    }

    // Mark conflicts
    for (const action of conflicts) {
      const file = this.app.vault.getAbstractFileByPath(action.path);
      if (file instanceof TFile) {
        state.set(action.path, {
          path: action.path,
          localMtime: file.stat.mtime,
          localSize: file.stat.size,
          remoteRevision: 0,
          syncedAt: 0,
          status: "conflict",
        });
      }
    }

    // Mark existing synced files (skip actions)
    for (const action of this.actions.filter((a) => a.action === "skip")) {
      const file = this.app.vault.getAbstractFileByPath(action.path);
      if (file instanceof TFile) {
        const stat = await this.client.stat("/" + action.path);
        state.set(action.path, {
          path: action.path,
          localMtime: file.stat.mtime,
          localSize: file.stat.size,
          remoteRevision: stat?.revision ?? 0,
          syncedAt: now,
          status: "synced",
        });
      }
    }

    if (pullCount > 0) {
      new Notice(`drive9: Downloaded ${pullCount} file(s) from remote.`);
    }
    if (conflicts.length > 0) {
      new Notice(
        `drive9: ${conflicts.length} conflicting file(s) skipped. Resolve manually.`,
      );
    }

    this.resolve(state);
  }

  onClose(): void {
    this.contentEl.empty();
  }
}

function fmtSize(bytes: number | undefined): string {
  if (bytes === undefined) return "?";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
