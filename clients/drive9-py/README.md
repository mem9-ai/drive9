# Drive9 Python SDK

Python SDK for [drive9](https://github.com/mem9-ai/drive9).

## Install

```bash
pip install -e ".[dev]"
```

## Usage

```python
import os
from drive9 import Client

client = Client("http://localhost:8080", api_key="your-api-key")

# Write / read
client.write("/hello.txt", b"hello world")
data = client.read("/hello.txt")

# List directory
entries = client.list("/data/")
for entry in entries:
    print(entry.name, entry.size, entry.is_dir)

# Metadata
info = client.stat("/hello.txt")
print(info.size, info.revision, info.is_dir)

# Directories, copies, moves
client.mkdir("/mydir")
client.copy("/src.txt", "/dst.txt")
client.rename("/old.txt", "/new.txt")
client.delete("/tmp.txt")

# SQL query
rows = client.sql('SELECT * FROM files WHERE path = "/hello.txt"')

# Search
results = client.grep("hello", "/data/", limit=10)
results = client.find("/data/", {"name": "*.txt"})

# Stream upload / download
with open("large.bin", "rb") as f:
    client.write_stream("/large.bin", f, size=os.path.getsize("large.bin"))

body = client.read_stream("/large.bin")
with open("out.bin", "wb") as f:
    f.write(body.read())
body.close()
```

## Run tests

```bash
pytest tests/
```
