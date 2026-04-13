import { Plugin, Notice } from "obsidian";
import { Drive9Client } from "./client";
import { Drive9SettingTab } from "./settings";
import { SyncEngine, SyncStatus } from "./sync-engine";
import { reconcile } from "./reconcile";
import { DEFAULT_SETTINGS, Drive9PluginSettings, SyncStateEntry } from "./types";

interface PluginData {
  settings: Drive9PluginSettings;
  syncState: Record<string, SyncStateEntry>;
  reconciled: boolean;
}

export default class Drive9Plugin extends Plugin {
  settings: Drive9PluginSettings = DEFAULT_SETTINGS;
  private syncEngine: SyncEngine | null = null;
  private statusBarEl: HTMLElement | null = null;
  private syncState = new Map<string, SyncStateEntry>();
  private reconciled = false;

  async onload(): Promise<void> {
    await this.loadSettings();

    this.statusBarEl = this.addStatusBarItem();
    this.updateStatusBar("idle");

    this.addSettingTab(new Drive9SettingTab(this.app, this));

    this.addCommand({
      id: "drive9-force-sync",
      name: "Force sync all files",
      callback: () => this.forceSync(),
    });

    this.addCommand({
      id: "drive9-reconnect",
      name: "Reconnect to drive9",
      callback: () => this.startSync(),
    });

    // Wait for layout ready to avoid vault initialization events
    this.app.workspace.onLayoutReady(() => {
      this.startSync();
    });
  }

  async onunload(): Promise<void> {
    this.syncEngine?.stop();
    await this.savePluginData();
  }

  async loadSettings(): Promise<void> {
    const data = (await this.loadData()) as PluginData | null;
    if (data) {
      this.settings = Object.assign({}, DEFAULT_SETTINGS, data.settings);
      if (data.syncState) {
        this.syncState = new Map(Object.entries(data.syncState));
      }
      this.reconciled = data.reconciled ?? false;
    }
  }

  async saveSettings(): Promise<void> {
    await this.savePluginData();
    // Restart sync if settings changed
    if (this.syncEngine) {
      this.syncEngine.stop();
      this.syncEngine = null;
      this.startSync();
    }
  }

  private async savePluginData(): Promise<void> {
    const data: PluginData = {
      settings: this.settings,
      syncState: Object.fromEntries(this.syncState),
      reconciled: this.reconciled,
    };
    await this.saveData(data);
  }

  private async startSync(): Promise<void> {
    if (!this.settings.serverUrl || !this.settings.apiKey) {
      this.updateStatusBar("idle", "not configured");
      return;
    }

    const client = new Drive9Client(
      this.settings.serverUrl,
      this.settings.apiKey,
    );

    // Check connectivity
    const reachable = await client.ping();
    if (!reachable) {
      this.updateStatusBar("error", "unreachable");
      new Notice("drive9: Cannot reach server. Will retry later.");
      // Retry in 30s
      setTimeout(() => this.startSync(), 30_000);
      return;
    }

    // First-run reconciliation
    if (!this.reconciled) {
      const state = await reconcile(
        this.app,
        client,
        this.settings.ignorePaths,
        this.settings.maxFileSize,
      );
      if (!state) {
        // User cancelled
        this.updateStatusBar("idle", "cancelled");
        return;
      }
      this.syncState = state;
      this.reconciled = true;
      await this.savePluginData();
    }

    // Start sync engine
    this.syncEngine = new SyncEngine(client, this.app.vault, this.syncState, {
      debounceMs: this.settings.pushDebounce,
      ignorePaths: this.settings.ignorePaths,
      maxFileSize: this.settings.maxFileSize,
    });

    this.syncEngine.onStatus((status, detail) => {
      this.updateStatusBar(status, detail);
    });

    // Register vault events
    this.registerEvent(
      this.app.vault.on("create", (file) => this.syncEngine?.onFileCreate(file)),
    );
    this.registerEvent(
      this.app.vault.on("modify", (file) => this.syncEngine?.onFileModify(file)),
    );
    this.registerEvent(
      this.app.vault.on("delete", (file) => this.syncEngine?.onFileDelete(file)),
    );
    this.registerEvent(
      this.app.vault.on("rename", (file, oldPath) =>
        this.syncEngine?.onFileRename(file, oldPath),
      ),
    );

    this.updateStatusBar("idle");
    new Notice("drive9: Connected and syncing.");
  }

  private async forceSync(): Promise<void> {
    if (!this.syncEngine) {
      new Notice("drive9: Not connected. Configure server URL and API key first.");
      return;
    }

    const files = this.app.vault.getFiles();
    let count = 0;
    for (const file of files) {
      this.syncEngine.onFileModify(file);
      count++;
    }
    new Notice(`drive9: Queued ${count} file(s) for sync.`);
  }

  private updateStatusBar(
    status: SyncStatus | "idle",
    detail?: string,
  ): void {
    if (!this.statusBarEl) return;
    switch (status) {
      case "idle":
        this.statusBarEl.setText(
          detail ? `drive9: ${detail}` : "drive9: synced",
        );
        break;
      case "syncing":
        this.statusBarEl.setText(
          detail ? `drive9: syncing ${detail}` : "drive9: syncing...",
        );
        break;
      case "error":
        this.statusBarEl.setText(
          detail ? `drive9: error (${detail})` : "drive9: error",
        );
        break;
    }
  }
}
