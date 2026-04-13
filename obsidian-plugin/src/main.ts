import { Plugin, Notice, TFile } from "obsidian";
import { Drive9Client } from "./client";
import { SyncEngine } from "./sync-engine";
import { Drive9SettingTab } from "./settings";
import { runFirstRunReconciliation, pullAllRemote } from "./first-run";
import type { PluginData, Drive9Settings, SyncState } from "./types";
import { DEFAULT_PLUGIN_DATA, DEFAULT_SETTINGS } from "./types";

export default class Drive9Plugin extends Plugin {
  settings: Drive9Settings = DEFAULT_SETTINGS;
  private client!: Drive9Client;
  private syncEngine!: SyncEngine;
  private syncStates: Record<string, SyncState> = {};
  private firstRunComplete = false;
  private statusBarEl: HTMLElement | null = null;

  async onload(): Promise<void> {
    await this.loadPluginData();

    this.client = new Drive9Client(
      this.settings.serverUrl,
      this.settings.apiKey,
    );

    this.syncEngine = new SyncEngine(
      this.app.vault,
      this.client,
      this.syncStates,
      this.settings,
      () => this.savePluginData(),
    );

    // Status bar.
    this.statusBarEl = this.addStatusBarItem();
    this.updateStatusBar();
    this.syncEngine.onStatusChange(() => this.updateStatusBar());

    // Settings tab.
    this.addSettingTab(new Drive9SettingTab(this.app, this));

    // Wait for layout ready before registering vault events.
    this.app.workspace.onLayoutReady(() => {
      this.onLayoutReady();
    });
  }

  private async onLayoutReady(): Promise<void> {
    if (!this.settings.serverUrl) {
      this.setStatusBar("⚙ drive9: configure in settings");
      return;
    }

    // First-run reconciliation.
    if (!this.firstRunComplete) {
      try {
        await this.doFirstRun();
      } catch (e) {
        console.error("[drive9] first-run failed", e);
        new Notice(`drive9: first-run failed — ${e instanceof Error ? e.message : String(e)}`);
        this.setStatusBar("✗ drive9: first-run failed");
        return;
      }
    }

    // Register vault events for ongoing sync.
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
        // Mark all local files as dirty so the sync engine pushes them.
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
        );
        break;

      case "reconciled":
        Object.assign(this.syncStates, result.states);
        // Push files that are only local.
        for (const [path, state] of Object.entries(result.states)) {
          if (state.status === "local_dirty") {
            this.syncEngine.onLocalCreate(
              this.app.vault.getAbstractFileByPath(path)!,
            );
          }
        }
        // Pull files that are only remote.
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
              // Refresh revision if it was unknown from first-run.
              if (state.remoteRevision === null) {
                try {
                  const st = await this.client.stat(path);
                  state.remoteRevision = st.revision;
                } catch { /* stays null — will refresh before next push */ }
              }
              const pulled = this.app.vault.getAbstractFileByPath(path);
              if (pulled instanceof TFile) {
                state.localMtime = pulled.stat.mtime;
                state.localSize = pulled.stat.size;
              }
              state.status = state.remoteRevision !== null ? "synced" : "needs_refresh";
              state.syncedAt = Date.now();
            } catch (e) {
              console.error(`[drive9] pull failed: ${path}`, e);
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

  // ---------------------------------------------------------------------------
  // Data persistence
  // ---------------------------------------------------------------------------

  async loadPluginData(): Promise<void> {
    const raw = await this.loadData();
    const data: PluginData = Object.assign({}, DEFAULT_PLUGIN_DATA, raw ?? {});
    this.settings = Object.assign({}, DEFAULT_SETTINGS, data.settings);
    this.syncStates = data.syncStates ?? {};
    this.firstRunComplete = data.firstRunComplete ?? false;
  }

  async savePluginData(): Promise<void> {
    const data: PluginData = {
      settings: this.settings,
      syncStates: this.syncStates,
      firstRunComplete: this.firstRunComplete,
    };
    await this.saveData(data);

    // Keep client and sync engine in sync with settings.
    this.client.updateConfig(this.settings.serverUrl, this.settings.apiKey);
    this.syncEngine?.updateSettings(this.settings);
  }

  // ---------------------------------------------------------------------------
  // Status bar
  // ---------------------------------------------------------------------------

  private updateStatusBar(): void {
    const engine = this.syncEngine;
    if (!engine) return;

    switch (engine.status) {
      case "syncing":
        this.setStatusBar(`↑ drive9: uploading ${engine.pendingCount} files`);
        break;
      case "error":
        this.setStatusBar("✗ drive9: error");
        break;
      case "idle":
        this.setStatusBar("✓ drive9: synced");
        break;
    }
  }

  private setStatusBar(text: string): void {
    if (this.statusBarEl) {
      this.statusBarEl.setText(text);
    }
  }

  onunload(): void {
    // Nothing to clean up — Obsidian handles event deregistration.
  }
}
