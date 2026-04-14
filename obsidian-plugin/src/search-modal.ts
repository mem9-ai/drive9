import { App, SuggestModal, Notice } from "obsidian";
import { Drive9Client, Drive9Error, sanitizeError } from "./client";
import type { SearchResult } from "./types";

const DEBOUNCE_MS = 300;
const MIN_QUERY_LENGTH = 3;
const SEARCH_LIMIT = 20;

export class Drive9SearchModal extends SuggestModal<SearchResult> {
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private lastQuery = "";
  private cachedResults: SearchResult[] = [];

  constructor(
    app: App,
    private client: Drive9Client,
  ) {
    super(app);
    this.setPlaceholder("Search files in drive9...");
    this.setInstructions([
      { command: "↑↓", purpose: "navigate" },
      { command: "↵", purpose: "open file" },
      { command: "esc", purpose: "dismiss" },
    ]);
  }

  getSuggestions(query: string): SearchResult[] | Promise<SearchResult[]> {
    if (query.length < MIN_QUERY_LENGTH) {
      this.cachedResults = [];
      return [];
    }

    if (query === this.lastQuery) {
      return this.cachedResults;
    }

    return new Promise<SearchResult[]>((resolve) => {
      if (this.debounceTimer) clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(async () => {
        try {
          const results = await this.client.grep(query, SEARCH_LIMIT);
          this.lastQuery = query;
          this.cachedResults = results;
          resolve(results);
        } catch (e) {
          if (e instanceof Drive9Error) {
            new Notice(`drive9 search: ${e.message}`);
          } else {
            new Notice(`drive9 search: ${sanitizeError(e instanceof Error ? e.message : String(e))}`);
          }
          resolve(this.cachedResults);
        }
      }, DEBOUNCE_MS);
    });
  }

  renderSuggestion(result: SearchResult, el: HTMLElement): void {
    const container = el.createDiv({ cls: "drive9-search-result" });

    container.createDiv({
      cls: "drive9-search-path",
      text: result.path,
    });

    const meta = container.createDiv({ cls: "drive9-search-meta" });
    meta.style.fontSize = "0.85em";
    meta.style.color = "var(--text-muted)";

    const parts: string[] = [];
    if (result.size_bytes > 0) {
      parts.push(formatSize(result.size_bytes));
    }
    if (result.score != null) {
      parts.push(`score: ${result.score.toFixed(2)}`);
    }
    if (parts.length > 0) {
      meta.setText(parts.join(" · "));
    }
  }

  onChooseSuggestion(result: SearchResult): void {
    const file = this.app.vault.getAbstractFileByPath(result.path);
    if (file) {
      void this.app.workspace.openLinkText(result.path, "", false);
    } else {
      new Notice(`drive9: file not found locally — ${result.path}`);
    }
  }

  onClose(): void {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
  }
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
