# drive9 SDKs

Official and community SDKs for drive9.

## Official SDKs

| Language | Status | Location |
|----------|--------|----------|
| Go | ✅ Official | `pkg/client` (in-repo) |
| Python | 📦 Community | `sdk/python/` |
| JavaScript | 📦 Community | `sdk/javascript/` |
| Rust | 📦 Community | `sdk/rust/` |

## Quick Comparison

### Go
```go
import "github.com/mem9-ai/drive9/pkg/client"

c := client.New("https://api.drive9.ai", "your-api-key")
c.Write("/data/file.txt", []byte("hello"))
```

### Python
```python
from drive9 import Drive9Client

client = Drive9Client("https://api.drive9.ai", "your-api-key")
client.write("/data/file.txt", b"hello")
```

### JavaScript
```javascript
import { Drive9Client } from 'drive9';

const client = new Drive9Client("https://api.drive9.ai", "your-api-key");
await client.write("/data/file.txt", new TextEncoder().encode("hello"));
```

### Rust
```rust
use drive9::Drive9Client;

let client = Drive9Client::new("https://api.drive9.ai", "your-api-key");
client.write("/data/file.txt", b"hello".to_vec()).await?;
```

## Getting API Key

```bash
# Install CLI
curl -fsSL https://drive9.ai/install.sh | sh

# Create workspace and get API key
drive9 create
```

## API Endpoint

- **Production**: `https://api.drive9.ai`

## Contributing

Want to add a new language SDK? See the HTTP API spec in the main README and follow the patterns in existing SDKs.
