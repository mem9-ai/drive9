# drive9 JavaScript SDK

Simple JavaScript/TypeScript client for drive9 API.

## Installation

```bash
npm install drive9
# or
yarn add drive9
```

Or copy `drive9.js` directly to your project.

## Quick Start

```javascript
import { Drive9Client } from './drive9.js';
// or: const { Drive9Client } = require('./drive9.js');

// Initialize client (get API key from 'drive9 create' or console)
const client = new Drive9Client(
    "https://api.drive9.ai",
    "your-api-key"
);

// Write file
await client.write("/data/hello.txt", new TextEncoder().encode("Hello!"));

// Read file
const text = await client.readText("/data/hello.txt");
console.log(text);

// List directory
const entries = await client.list("/data/");
for (const e of entries) {
    console.log(`  - ${e.name} (${e.size} bytes)`);
}

// Copy (zero-copy)
await client.copy("/data/hello.txt", "/data/hello-copy.txt");

// Rename
await client.rename("/data/hello-copy.txt", "/data/hello-renamed.txt");

// Delete
await client.delete("/data/hello-renamed.txt");
```

## Features

- Works in Node.js and browsers
- Write/Read files (auto-handles small/large files)
- List directories
- Stat metadata
- Copy/Rename (zero-copy, metadata only)
- Delete
- Mkdir
- Semantic search (grep)

## Browser Usage

```html
<script src="drive9.js"></script>
<script>
    const client = new Drive9Client("https://api.drive9.ai", "your-key");
    // ... use client
</script>
```
