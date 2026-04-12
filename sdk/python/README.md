# drive9 Python SDK

Simple Python client for drive9 API.

## Installation

Copy `drive9.py` to your project or install via pip (when published):

```bash
pip install drive9
```

## Quick Start

```python
from drive9 import Drive9Client

# Initialize client (get API key from 'drive9 create' or console)
client = Drive9Client(
    base_url="https://api.drive9.ai",
    api_key="your-api-key"
)

# Write file
client.write("/data/hello.txt", b"Hello, drive9!")

# Read file
data = client.read_text("/data/hello.txt")
print(data)

# List directory
entries = client.list("/data/")
for e in entries:
    print(f"  - {e['name']} ({e['size']} bytes)")

# Copy (zero-copy)
client.copy("/data/hello.txt", "/data/hello-copy.txt")

# Rename
client.rename("/data/hello-copy.txt", "/data/hello-renamed.txt")

# Delete
client.delete("/data/hello-renamed.txt")
```

## Features

- Write/Read files (auto-handles small/large files)
- List directories
- Stat metadata
- Copy/Rename (zero-copy, metadata only)
- Delete
- Mkdir
- Semantic search (grep)

## Requirements

- Python 3.7+
- requests
