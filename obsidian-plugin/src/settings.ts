import { App, PluginSettingTab, Setting } from "obsidian";
import type Drive9Plugin from "./main";

export class Drive9SettingTab extends PluginSettingTab {
  plugin: Drive9Plugin;

  constructor(app: App, plugin: Drive9Plugin) {
    super(app, plugin);
    this.plugin = plugin;
  }

  display(): void {
    const { containerEl } = this;
    containerEl.empty();

    new Setting(containerEl)
      .setName("Server URL")
      .setDesc("drive9 server URL (e.g. https://api.drive9.ai)")
      .addText((text) =>
        text
          .setPlaceholder("https://api.drive9.ai")
          .setValue(this.plugin.settings.serverUrl)
          .onChange(async (value) => {
            this.plugin.settings.serverUrl = value.trim();
            await this.plugin.saveSettings();
          }),
      );

    new Setting(containerEl)
      .setName("API Key")
      .setDesc("drive9 API key for authentication")
      .addText((text) => {
        text.inputEl.type = "password";
        text
          .setPlaceholder("your-api-key")
          .setValue(this.plugin.settings.apiKey)
          .onChange(async (value) => {
            this.plugin.settings.apiKey = value.trim();
            await this.plugin.saveSettings();
          });
      });

    new Setting(containerEl)
      .setName("Push debounce (ms)")
      .setDesc("Delay before syncing after an edit (default 2000ms)")
      .addText((text) =>
        text
          .setPlaceholder("2000")
          .setValue(String(this.plugin.settings.pushDebounce))
          .onChange(async (value) => {
            const n = parseInt(value, 10);
            if (!isNaN(n) && n >= 500) {
              this.plugin.settings.pushDebounce = n;
              await this.plugin.saveSettings();
            }
          }),
      );

    new Setting(containerEl)
      .setName("Ignore paths")
      .setDesc(
        "Glob patterns to exclude from sync (one per line). " +
          "Default: .obsidian/**, .trash/**, *.tmp, .DS_Store",
      )
      .addTextArea((text) =>
        text
          .setPlaceholder(".obsidian/**\n.trash/**\n*.tmp\n.DS_Store")
          .setValue(this.plugin.settings.ignorePaths.join("\n"))
          .onChange(async (value) => {
            this.plugin.settings.ignorePaths = value
              .split("\n")
              .map((s) => s.trim())
              .filter((s) => s.length > 0);
            await this.plugin.saveSettings();
          }),
      );

    new Setting(containerEl)
      .setName("Max file size (MB)")
      .setDesc("Skip files larger than this (default 100 MB)")
      .addText((text) =>
        text
          .setPlaceholder("100")
          .setValue(String(this.plugin.settings.maxFileSize / (1024 * 1024)))
          .onChange(async (value) => {
            const n = parseInt(value, 10);
            if (!isNaN(n) && n > 0) {
              this.plugin.settings.maxFileSize = n * 1024 * 1024;
              await this.plugin.saveSettings();
            }
          }),
      );
  }
}
