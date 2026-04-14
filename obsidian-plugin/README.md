# drive9 for Obsidian

Sync your Obsidian vault to [drive9](https://github.com/mem9-ai/drive9) for semantic search and AI-agent accessibility.

## Features

- **Bidirectional sync** — local changes push to drive9; remote changes pull automatically via SSE (desktop) or polling (mobile)
- **Conflict resolution** — 3-way merge with shadow store; binary files get `.conflict` copies
- **Semantic search** — `Cmd+Shift+S` opens a search modal powered by drive9's hybrid search (FTS + vector + keyword fallback)
- **Large file support** — files over 50 KB use multipart upload with progress indication
- **Mobile compatible** — works on iOS and Android with battery-friendly polling intervals

## Setup

1. Install the plugin from Obsidian Community Plugins (search "drive9") or manually copy `main.js` and `manifest.json` to your vault's `.obsidian/plugins/drive9/` directory.
2. Open **Settings > drive9**.
3. Enter your **Server URL** (e.g. `https://api.drive9.ai`).
4. Enter your **API Key**. The plugin will automatically test the connection.
5. Sync starts automatically after the first-run reconciliation.

## First-run behavior

On first launch, the plugin detects whether files exist locally, remotely, or both:

- **Local-only vault** — uploads all files to drive9
- **Remote-only files** — downloads from drive9
- **Both exist** — reconciles by comparing files and syncing differences

## Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Server URL | — | drive9 server address |
| API Key | — | Authentication key (stored locally in `.obsidian/`) |
| Push Debounce | 2000 ms | Delay before syncing after a file change |
| Ignore Paths | `.obsidian/**`, `.trash/**`, `*.tmp`, `.DS_Store` | Glob patterns excluded from sync |
| Max File Size | 100 MB | Skip files larger than this |
| Mobile Max File Size | 20 MB | Lower limit on mobile to prevent out-of-memory |

## Security

- API key is stored in `.obsidian/plugins/drive9/data.json` — ensure `.obsidian/` is in your `.gitignore` if your vault is a git repo.
- The plugin warns you in settings if `.gitignore` doesn't cover `.obsidian/`.
- Error messages are sanitized to strip Bearer tokens before display.

## Commands

| Command | Hotkey | Description |
|---------|--------|-------------|
| Search (drive9) | `Cmd+Shift+S` | Open hybrid search modal |

## Requirements

- Obsidian 1.5.0 or later
- A running drive9 server with a valid API key

## License

MIT
