import { Plugin, Notice, TFile } from "obsidian";
import { Drive9Client, sanitizeError } from "./client";
import { RemoteWatcher } from "./remote-watcher";
import { SyncEngine } from "./sync-engine";
import { ShadowStore } from "./shadow-store";
import { ConflictResolver } from "./conflict-resolver";
import { Drive9SettingTab } from "./settings";
import { Drive9SearchModal } from "./search-modal";
import { runFirstRunReconciliation, pullAllRemote } from "./first-run";
import type { PluginData, Drive9Settings, SyncState } from "./types";
import { DEFAULT_PLUGIN_DATA, DEFAULT_SETTINGS } from "./types";

export default class Drive9Plugin extends Plugin {
  settings: Drive9Settings = DEFAULT_SETTINGS;
  private client!: Drive9Client;
  private remoteWatcher: RemoteWatcher | null = null;
  private syncEngine!: SyncEngine;
  private conflictResolver!: ConflictResolver;
  private shadowStore!: ShadowStore;
  private resolutionTimer: ReturnType<typeof setInterval> | null = null;
  private shadowGCTimer: ReturnType<typeof setInterval> | null = null;
  private syncStates: Record<string, SyncState> = {};
  private firstRunComplete = false;
  private statusBarEl: HTMLElement | null = null;
  private actorId = "";

  async onload(): Promise<void> {
    await this.loadPluginData();

    const needsActorId = !this.actorId;
    if (needsActorId) {
      this.actorId = generateActorId();
    }

    this.client = new Drive9Client(
      this.settings.serverUrl,
      this.settings.apiKey,
    );
    this.client.setActorId(this.actorId);

    this.syncEngine = new SyncEngine(
      this.app.vault,
      this.client,
      this.syncStates,
      this.settings,
      () => this.savePluginData(),
    );

    this.shadowStore = new ShadowStore(this.app.vault.adapter);
    this.syncEngine.setShadowStore(this.shadowStore);

    this.conflictResolver = new ConflictResolver(
      this.app,
      this.app.vault,
      this.client,
      this.syncStates,
      () => this.savePluginData(),
    );
    this.conflictResolver.setSuppressLocalEvent(
      (path, fn) => this.syncEngine.withSuppressedLocalEvents(path, fn),
    );

    this.remoteWatcher = new RemoteWatcher(this.client, {
      onChange: (event) => this.syncEngine.onRemoteChange(event.path, event.op),
      onReset: () => this.syncEngine.fullSync(),
      onPoll: () => this.syncEngine.fullSync(),
    });

    if (needsActorId) {
      await this.savePluginData();
    }

    this.statusBarEl = this.addStatusBarItem();
    this.updateStatusBar();
    this.syncEngine.onStatusChange(() => this.updateStatusBar());

    this.addSettingTab(new Drive9SettingTab(this.app, this));

    this.addCommand({
      id: "drive9-search",
      name: "Search (drive9)",
      callback: () => new Drive9SearchModal(this.app, this.client).open(),
    });

    this.app.workspace.onLayoutReady(() => {
      void this.onLayoutReady();
    });
  }

  private async onLayoutReady(): Promise<void> {
    if (!this.settings.serverUrl) {
      this.setStatusBar("⚙ drive9: configure in settings");
      return;
    }

    if (!this.firstRunComplete) {
      try {
        await this.doFirstRun();
      } catch (e) {
        console.error("[drive9] first-run failed", e instanceof Error ? e.message : sanitizeError(String(e)));
        new Notice(`drive9: first-run failed — ${e instanceof Error ? e.message : sanitizeError(String(e))}`);
        this.setStatusBar("✗ drive9: first-run failed");
        return;
      }
    }

    this.registerEvent(
      this.app.vault.on("create", (file) => this.syncEngine.onLocalCreate(file)),
    );
    this.registerEvent(
      this.app.vault.on("modify", (file) => this.syncEngine.onLocalModify(file)),
    );
    this.registerEvent(
      this.app.vault.on("delete", (file) => this.syncEngine.onLocalDelete(file)),
    );
    this.registerEvent(
      this.app.vault.on("rename", (file, oldPath) =>
        this.syncEngine.onLocalRename(file, oldPath),
      ),
    );

    this.remoteWatcher?.start();

    // Resolution loop: scan for conflicts and remote_deleted every 10s
    this.resolutionTimer = setInterval(() => {
      void this.conflictResolver.resolveAll();
    }, 10_000);

    // Shadow GC: clean up unreferenced shadow files every 5 minutes
    this.shadowGCTimer = setInterval(() => {
      void this.conflictResolver.gcShadows();
    }, 5 * 60_000);

    this.setStatusBar("✓ drive9: synced");
  }

  private async doFirstRun(): Promise<void> {
    this.setStatusBar("↕ drive9: reconciling...");

    const result = await runFirstRunReconciliation(
      this.app,
      this.app.vault,
      this.client,
      this.settings.ignorePaths,
    );

    switch (result.action) {
      case "push_all":
        new Notice("drive9: uploading vault to drive9...");
        for (const file of this.app.vault.getFiles()) {
          this.syncEngine.onLocalCreate(file);
        }
        break;

      case "pull_all":
        new Notice("drive9: downloading from drive9...");
        await pullAllRemote(
          this.app.vault,
          this.client,
          this.syncStates,
          this.settings.ignorePaths,
          this.shadowStore,
        );
        break;

      case "reconciled":
        Object.assign(this.syncStates, result.states);
        for (const [path, state] of Object.entries(result.states)) {
          if (state.status === "local_dirty") {
            this.syncEngine.onLocalCreate(
              this.app.vault.getAbstractFileByPath(path)!,
            );
          }
        }
        for (const [path, state] of Object.entries(result.states)) {
          if (state.status === "remote_dirty") {
            try {
              const data = await this.client.read(path);
              const dir = path.contains("/")
                ? path.substring(0, path.lastIndexOf("/"))
                : "";
              if (dir && !this.app.vault.getAbstractFileByPath(dir)) {
                await this.app.vault.createFolder(dir);
              }
              await this.app.vault.createBinary(path, data);
              if (state.remoteRevision === null) {
                try {
                  const st = await this.client.stat(path);
                  state.remoteRevision = st.revision;
                } catch {
                  // Leave revision unknown; push path will refresh before write.
                }
              }
              try {
                state.lastSyncedContentHash = await this.shadowStore.save(data);
              } catch { /* shadow save is best-effort */ }
              const pulled = this.app.vault.getAbstractFileByPath(path);
              if (pulled instanceof TFile) {
                state.localMtime = pulled.stat.mtime;
                state.localSize = pulled.stat.size;
              }
              state.status = state.remoteRevision !== null ? "synced" : "needs_refresh";
              state.syncedAt = Date.now();
            } catch (e) {
              console.error(`[drive9] pull failed: ${path}`, e instanceof Error ? e.message : sanitizeError(String(e)));
            }
          }
        }
        break;

      case "cancelled":
        new Notice("drive9: first-run cancelled. Sync is disabled.");
        return;
    }

    this.firstRunComplete = true;
    await this.savePluginData();
  }

  async loadPluginData(): Promise<void> {
    const raw = await this.loadData();
    const data: PluginData = Object.assign({}, DEFAULT_PLUGIN_DATA, raw ?? {});
    this.settings = Object.assign({}, DEFAULT_SETTINGS, data.settings);
    this.syncStates = data.syncStates ?? {};
    this.firstRunComplete = data.firstRunComplete ?? false;
    this.actorId = data.actorId ?? "";
  }

  async savePluginData(): Promise<void> {
    const data: PluginData = {
      settings: this.settings,
      syncStates: this.syncStates,
      firstRunComplete: this.firstRunComplete,
      actorId: this.actorId,
    };
    await this.saveData(data);

    if (!this.client) {
      return;
    }

    const urlChanged =
      this.client.getServerUrl() !== this.settings.serverUrl ||
      this.client.getAPIKey() !== this.settings.apiKey;

    this.client.updateConfig(this.settings.serverUrl, this.settings.apiKey);
    this.client.setActorId(this.actorId);
    this.syncEngine?.updateSettings(this.settings);

    if (this.remoteWatcher && urlChanged) {
      if (this.firstRunComplete && this.settings.serverUrl) {
        this.remoteWatcher.restart();
      } else {
        this.remoteWatcher.stop();
      }
    }
  }

  private updateStatusBar(): void {
    const engine = this.syncEngine;
    if (!engine) return;

    const skipped = engine.skippedLargeFiles.length;
    switch (engine.status) {
      case "syncing": {
        const progress = engine.uploadProgressText;
        if (progress) {
          this.setStatusBar(`↕ drive9: ${progress}`);
        } else {
          this.setStatusBar(`↕ drive9: syncing ${engine.pendingCount} files`);
        }
        break;
      }
      case "error":
        this.setStatusBar("✗ drive9: error");
        break;
      case "idle":
        if (engine.pendingCount > 0) {
          this.setStatusBar(`↕ drive9: queued ${engine.pendingCount} files`);
        } else if (skipped > 0) {
          this.setStatusBar(`✓ drive9: synced (${skipped} skipped — too large)`);
        } else {
          this.setStatusBar("✓ drive9: synced");
        }
        break;
    }
  }

  private setStatusBar(text: string): void {
    if (this.statusBarEl) {
      this.statusBarEl.setText(text);
    }
  }

  onunload(): void {
    this.remoteWatcher?.stop();
    if (this.resolutionTimer) {
      clearInterval(this.resolutionTimer);
      this.resolutionTimer = null;
    }
    if (this.shadowGCTimer) {
      clearInterval(this.shadowGCTimer);
      this.shadowGCTimer = null;
    }
  }
}

function generateActorId(): string {
  if (globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return `obsidian-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
