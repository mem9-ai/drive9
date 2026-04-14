import { App, SuggestModal, Notice } from "obsidian";
import type { Drive9Client } from "./client";
import { Drive9Error, sanitizeError } from "./client";
import type { SearchResult } from "./types";

const MIN_QUERY_LENGTH = 3;
const DEBOUNCE_MS = 300;

export class Drive9SearchModal extends SuggestModal<SearchResult> {
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private lastQuery = "";
  private cachedResults: SearchResult[] = [];

  constructor(
    app: App,
    private client: Drive9Client,
  ) {
    super(app);
    this.setPlaceholder("Search your drive9 files…");
    this.emptyStateText = "Type at least 3 characters to search";
    this.limit = 20;
  }

  getSuggestions(query: string): SearchResult[] | Promise<SearchResult[]> {
    if (query.length < MIN_QUERY_LENGTH) {
      this.cachedResults = [];
      return [];
    }

    if (query === this.lastQuery && this.cachedResults.length > 0) {
      return this.cachedResults;
    }

    return new Promise((resolve) => {
      if (this.debounceTimer) clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(async () => {
        try {
          const results = await this.client.grep(query, "/", 20);
          this.lastQuery = query;
          this.cachedResults = results;
          resolve(results);
        } catch (e) {
          if (e instanceof Drive9Error) {
            new Notice(`drive9 search failed: ${e.message}`);
          } else {
            new Notice(`drive9 search failed: ${sanitizeError(String(e))}`);
          }
          resolve([]);
        }
      }, DEBOUNCE_MS);
    });
  }

  renderSuggestion(result: SearchResult, el: HTMLElement): void {
    el.createEl("div", { text: result.path, cls: "drive9-search-path" });
    const meta: string[] = [];
    if (result.score != null) {
      meta.push(`score: ${result.score.toFixed(2)}`);
    }
    if (result.size_bytes > 0) {
      meta.push(formatSize(result.size_bytes));
    }
    if (meta.length > 0) {
      el.createEl("small", { text: meta.join(" · "), cls: "drive9-search-meta" });
    }
  }

  onChooseSuggestion(result: SearchResult): void {
    this.app.workspace.openLinkText(result.path, "");
  }
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
