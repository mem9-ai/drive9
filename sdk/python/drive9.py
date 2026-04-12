"""
drive9 Python SDK

A simple Python client for drive9 API.
"""

import requests
from typing import List, Optional, Dict, Any


class Drive9Client:
    """drive9 HTTP API client."""
    
    def __init__(self, base_url: str, api_key: str):
        """
        Initialize drive9 client.
        
        Args:
            base_url: API endpoint (e.g., "https://api.drive9.ai")
            api_key: Your API key from 'drive9 create' or console
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}"
        })
    
    def _url(self, path: str) -> str:
        """Build full URL for a path."""
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}/v1/fs{path}"
    
    def write(self, path: str, data: bytes) -> None:
        """
        Upload data to a path.
        
        Args:
            path: Remote file path (e.g., "/data/file.txt")
            data: File content as bytes
        """
        resp = self.session.put(
            self._url(path),
            data=data,
            headers={"Content-Type": "application/octet-stream"}
        )
        resp.raise_for_status()
    
    def write_file(self, path: str, local_path: str) -> None:
        """
        Upload a local file.
        
        Args:
            path: Remote file path
            local_path: Local file path to upload
        """
        with open(local_path, "rb") as f:
            self.write(path, f.read())
    
    def read(self, path: str) -> bytes:
        """
        Download a file's content.
        
        Args:
            path: Remote file path
            
        Returns:
            File content as bytes
        """
        resp = self.session.get(self._url(path))
        resp.raise_for_status()
        return resp.content
    
    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """
        Download a file as text.
        
        Args:
            path: Remote file path
            encoding: Text encoding (default: utf-8)
            
        Returns:
            File content as string
        """
        return self.read(path).decode(encoding)
    
    def list(self, path: str = "/") -> List[Dict[str, Any]]:
        """
        List directory entries.
        
        Args:
            path: Directory path (default: root)
            
        Returns:
            List of entry dicts with 'name', 'size', 'isDir' keys
        """
        resp = self.session.get(self._url(path), params={"list": ""})
        resp.raise_for_status()
        return resp.json().get("entries", [])
    
    def stat(self, path: str) -> Dict[str, Any]:
        """
        Get file/directory metadata.
        
        Args:
            path: File or directory path
            
        Returns:
            Dict with 'size', 'isDir', 'revision' keys
        """
        resp = self.session.head(self._url(path))
        resp.raise_for_status()
        return {
            "size": int(resp.headers.get("X-Drive9-Size", 0)),
            "isDir": resp.headers.get("X-Drive9-IsDir") == "true",
            "revision": int(resp.headers.get("X-Drive9-Revision", 0))
        }
    
    def copy(self, src: str, dst: str) -> None:
        """
        Copy a file (zero-copy, metadata only within drive9).
        
        Args:
            src: Source path
            dst: Destination path
        """
        resp = self.session.post(
            self._url(dst),
            params={"copy": ""},
            headers={"X-Drive9-Copy-Source": src}
        )
        resp.raise_for_status()
    
    def rename(self, src: str, dst: str) -> None:
        """
        Rename/move a file (metadata only).
        
        Args:
            src: Source path
            dst: Destination path
        """
        resp = self.session.post(
            self._url(dst),
            params={"rename": ""},
            headers={"X-Drive9-Rename-Source": src}
        )
        resp.raise_for_status()
    
    def delete(self, path: str) -> None:
        """
        Delete a file or directory.
        
        Args:
            path: Path to delete
        """
        resp = self.session.delete(self._url(path))
        resp.raise_for_status()
    
    def mkdir(self, path: str) -> None:
        """
        Create a directory.
        
        Args:
            path: Directory path
        """
        resp = self.session.post(self._url(path), params={"mkdir": ""})
        resp.raise_for_status()
    
    def grep(self, query: str, path: str = "/") -> List[Dict[str, Any]]:
        """
        Semantic/keyword search.
        
        Args:
            query: Search query
            path: Search root path
            
        Returns:
            List of matching entries
        """
        resp = self.session.get(
            self._url(path),
            params={"grep": query}
        )
        resp.raise_for_status()
        return resp.json()


# Example usage
if __name__ == "__main__":
    # Initialize client
    client = Drive9Client(
        base_url="https://api.drive9.ai",
        api_key="your-api-key"  # Get from 'drive9 create' or console
    )
    
    # Write file
    client.write("/data/hello.txt", b"Hello, drive9!")
    print("✓ Written")
    
    # Read file
    data = client.read_text("/data/hello.txt")
    print(f"✓ Read: {data}")
    
    # List directory
    entries = client.list("/data/")
    print(f"✓ Found {len(entries)} entries")
    for e in entries:
        print(f"  - {e['name']} ({e['size']} bytes)")
    
    # Copy (zero-copy)
    client.copy("/data/hello.txt", "/data/hello-copy.txt")
    print("✓ Copied")
    
    # Rename
    client.rename("/data/hello-copy.txt", "/data/hello-renamed.txt")
    print("✓ Renamed")
    
    # Delete
    client.delete("/data/hello-renamed.txt")
    print("✓ Deleted")
    
    # Cleanup
    client.delete("/data/hello.txt")
    print("✓ Cleanup done")
