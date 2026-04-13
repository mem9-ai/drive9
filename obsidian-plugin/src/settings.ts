import { App, PluginSettingTab, Setting, Notice } from "obsidian";
import type Drive9Plugin from "./main";
import { Drive9Client } from "./client";

export class Drive9SettingTab extends PluginSettingTab {
  private validateTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    app: App,
    private plugin: Drive9Plugin,
  ) {
    super(app, plugin);
  }

  display(): void {
    const { containerEl } = this;
    containerEl.empty();

    containerEl.createEl("h2", { text: "drive9 Settings" });

    new Setting(containerEl)
      .setName("Server URL")
      .setDesc("drive9 server address (e.g. https://api.drive9.ai)")
      .addText((text) =>
        text
          .setPlaceholder("https://api.drive9.ai")
          .setValue(this.plugin.settings.serverUrl)
          .onChange(async (value) => {
            this.plugin.settings.serverUrl = value.trim();
            await this.plugin.savePluginData();
            this.scheduleValidation();
          }),
      );

    new Setting(containerEl)
      .setName("API Key")
      .setDesc("drive9 API key for authentication")
      .addText((text) => {
        text.inputEl.type = "password";
        text.inputEl.autocomplete = "off";
        text
          .setPlaceholder("your-api-key")
          .setValue(this.plugin.settings.apiKey)
          .onChange(async (value) => {
            this.plugin.settings.apiKey = value.trim();
            await this.plugin.savePluginData();
            this.scheduleValidation();
          });
      });

    new Setting(containerEl)
      .setName("Test Connection")
      .setDesc("Verify server URL and API key")
      .addButton((btn) =>
        btn.setButtonText("Test").onClick(async () => {
          await this.testConnection();
        }),
      );

    new Setting(containerEl)
      .setName("Push Debounce (ms)")
      .setDesc("Delay before syncing after a file change (default: 2000)")
      .addText((text) =>
        text
          .setPlaceholder("2000")
          .setValue(String(this.plugin.settings.pushDebounce))
          .onChange(async (value) => {
            const n = parseInt(value, 10);
            if (!isNaN(n) && n >= 500) {
              this.plugin.settings.pushDebounce = n;
              await this.plugin.savePluginData();
            }
          }),
      );

    new Setting(containerEl)
      .setName("Ignore Paths")
      .setDesc("Glob patterns to exclude from sync (one per line)")
      .addTextArea((text) =>
        text
          .setPlaceholder(".obsidian/**\n.trash/**")
          .setValue(this.plugin.settings.ignorePaths.join("\n"))
          .onChange(async (value) => {
            this.plugin.settings.ignorePaths = value
              .split("\n")
              .map((s) => s.trim())
              .filter((s) => s.length > 0);
            await this.plugin.savePluginData();
          }),
      );

    new Setting(containerEl)
      .setName("Max File Size (MB)")
      .setDesc("Skip files larger than this (default: 100)")
      .addText((text) =>
        text
          .setPlaceholder("100")
          .setValue(String(Math.round(this.plugin.settings.maxFileSize / (1024 * 1024))))
          .onChange(async (value) => {
            const n = parseInt(value, 10);
            if (!isNaN(n) && n >= 1) {
              this.plugin.settings.maxFileSize = n * 1024 * 1024;
              await this.plugin.savePluginData();
            }
          }),
      );

    // .gitignore warning
    void this.checkGitignore(containerEl);
  }

  private scheduleValidation(): void {
    if (this.validateTimer) clearTimeout(this.validateTimer);
    this.validateTimer = setTimeout(() => {
      this.validateTimer = null;
      void this.testConnection();
    }, 1500);
  }

  private async testConnection(): Promise<void> {
    if (!this.plugin.settings.serverUrl) {
      new Notice("Please enter a server URL first");
      return;
    }
    if (!this.plugin.settings.apiKey) {
      new Notice("Please enter an API key first");
      return;
    }
    try {
      const testClient = new Drive9Client(
        this.plugin.settings.serverUrl,
        this.plugin.settings.apiKey,
      );
      await testClient.ping();
      new Notice("drive9: connection successful!");
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      new Notice(`drive9: connection failed — ${sanitizeError(msg)}`);
    }
  }

  private async checkGitignore(containerEl: HTMLElement): Promise<void> {
    const adapter = this.app.vault.adapter;
    const vaultRoot = (adapter as { getBasePath?: () => string }).getBasePath?.();
    if (!vaultRoot) return;

    try {
      const gitignorePath = `${vaultRoot}/.gitignore`;
      const fs = (globalThis as { require?: (name: string) => { existsSync: (p: string) => boolean; readFileSync: (p: string, e: string) => string } }).require?.("fs");
      if (!fs) return;

      if (!fs.existsSync(`${vaultRoot}/.git`)) return;

      if (!fs.existsSync(gitignorePath)) {
        this.addGitignoreWarning(containerEl, "No .gitignore found. Your API key in .obsidian/ could be committed to git.");
        return;
      }

      const content = fs.readFileSync(gitignorePath, "utf-8");
      const lines = content.split("\n").map((l: string) => l.trim());
      const coversObsidian = lines.some((l: string) =>
        l === ".obsidian" || l === ".obsidian/" || l === ".obsidian/**" || l === ".obsidian/*",
      );

      if (!coversObsidian) {
        this.addGitignoreWarning(containerEl, ".gitignore does not cover .obsidian/ — your API key could be committed to git.");
      }
    } catch {
      // Not on desktop or fs access failed — skip warning
    }
  }

  private addGitignoreWarning(containerEl: HTMLElement, message: string): void {
    const warning = containerEl.createEl("div", { cls: "drive9-gitignore-warning" });
    warning.style.padding = "8px 12px";
    warning.style.marginTop = "12px";
    warning.style.borderRadius = "4px";
    warning.style.backgroundColor = "var(--background-modifier-error)";
    warning.style.color = "var(--text-on-accent)";
    warning.createEl("strong", { text: "⚠ Security Warning: " });
    warning.createSpan({ text: message });
  }
}

/** Strip any potential API key / auth token from error messages. */
function sanitizeError(msg: string): string {
  // Remove Bearer tokens
  return msg.replace(/Bearer\s+\S+/gi, "Bearer ***");
}
