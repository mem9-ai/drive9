/**
 * drive9 JavaScript SDK
 * 
 * A simple JavaScript/TypeScript client for drive9 API.
 */

class Drive9Client {
    /**
     * Initialize drive9 client.
     * 
     * @param {string} baseUrl - API endpoint (e.g., "https://api.drive9.ai")
     * @param {string} apiKey - Your API key from 'drive9 create' or console
     */
    constructor(baseUrl, apiKey) {
        this.baseUrl = baseUrl.replace(/\/$/, "");
        this.apiKey = apiKey;
    }

    /**
     * Build full URL for a path.
     * @private
     */
    _url(path) {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return `${this.baseUrl}/v1/fs${path}`;
    }

    /**
     * Make authenticated request.
     * @private
     */
    async _request(method, path, options = {}) {
        const url = this._url(path);
        const headers = {
            "Authorization": `Bearer ${this.apiKey}`,
            ...options.headers
        };

        const resp = await fetch(url, {
            method,
            headers,
            ...options
        });

        if (!resp.ok) {
            const error = await resp.text();
            throw new Error(`HTTP ${resp.status}: ${error}`);
        }

        return resp;
    }

    /**
     * Upload data to a path.
     * 
     * @param {string} path - Remote file path (e.g., "/data/file.txt")
     * @param {Buffer|Blob|Uint8Array} data - File content
     */
    async write(path, data) {
        await this._request("PUT", path, {
            body: data,
            headers: { "Content-Type": "application/octet-stream" }
        });
    }

    /**
     * Upload a local file (Node.js) or File object (browser).
     * 
     * @param {string} path - Remote file path
     * @param {File|Buffer} file - File to upload
     */
    async writeFile(path, file) {
        await this.write(path, file);
    }

    /**
     * Download a file's content.
     * 
     * @param {string} path - Remote file path
     * @returns {Promise<ArrayBuffer>}
     */
    async read(path) {
        const resp = await this._request("GET", path);
        return resp.arrayBuffer();
    }

    /**
     * Download a file as text.
     * 
     * @param {string} path - Remote file path
     * @returns {Promise<string>}
     */
    async readText(path) {
        const resp = await this._request("GET", path);
        return resp.text();
    }

    /**
     * List directory entries.
     * 
     * @param {string} path - Directory path (default: root)
     * @returns {Promise<Array<{name: string, size: number, isDir: boolean}>>}
     */
    async list(path = "/") {
        const resp = await this._request("GET", path, {
            headers: { "X-Drive9-List": "true" }
        });
        const data = await resp.json();
        return data.entries || [];
    }

    /**
     * Get file/directory metadata.
     * 
     * @param {string} path - File or directory path
     * @returns {Promise<{size: number, isDir: boolean, revision: number}>}
     */
    async stat(path) {
        const resp = await this._request("HEAD", path);
        return {
            size: parseInt(resp.headers.get("X-Drive9-Size") || "0"),
            isDir: resp.headers.get("X-Drive9-IsDir") === "true",
            revision: parseInt(resp.headers.get("X-Drive9-Revision") || "0")
        };
    }

    /**
     * Copy a file (zero-copy within drive9).
     * 
     * @param {string} src - Source path
     * @param {string} dst - Destination path
     */
    async copy(src, dst) {
        await this._request("POST", dst, {
            headers: { "X-Drive9-Copy-Source": src }
        });
    }

    /**
     * Rename/move a file (metadata only).
     * 
     * @param {string} src - Source path
     * @param {string} dst - Destination path
     */
    async rename(src, dst) {
        await this._request("POST", dst, {
            headers: { "X-Drive9-Rename-Source": src }
        });
    }

    /**
     * Delete a file or directory.
     * 
     * @param {string} path - Path to delete
     */
    async delete(path) {
        await this._request("DELETE", path);
    }

    /**
     * Create a directory.
     * 
     * @param {string} path - Directory path
     */
    async mkdir(path) {
        await this._request("POST", path, {
            headers: { "X-Drive9-Mkdir": "true" }
        });
    }

    /**
     * Semantic/keyword search.
     * 
     * @param {string} query - Search query
     * @param {string} path - Search root path
     * @returns {Promise<Array>}
     */
    async grep(query, path = "/") {
        const url = new URL(this._url(path));
        url.searchParams.append("grep", query);
        
        const resp = await fetch(url, {
            headers: { "Authorization": `Bearer ${this.apiKey}` }
        });
        
        if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
        }
        return resp.json();
    }
}

// Example usage
async function example() {
    // Initialize client
    const client = new Drive9Client(
        "https://api.drive9.ai",
        "your-api-key"  // Get from 'drive9 create' or console
    );

    try {
        // Write file
        await client.write("/data/hello.txt", new TextEncoder().encode("Hello, drive9!"));
        console.log("✓ Written");

        // Read file
        const text = await client.readText("/data/hello.txt");
        console.log("✓ Read:", text);

        // List directory
        const entries = await client.list("/data/");
        console.log("✓ Found", entries.length, "entries");
        for (const e of entries) {
            console.log(`  - ${e.name} (${e.size} bytes)`);
        }

        // Copy (zero-copy)
        await client.copy("/data/hello.txt", "/data/hello-copy.txt");
        console.log("✓ Copied");

        // Rename
        await client.rename("/data/hello-copy.txt", "/data/hello-renamed.txt");
        console.log("✓ Renamed");

        // Delete
        await client.delete("/data/hello-renamed.txt");
        console.log("✓ Deleted");

        // Cleanup
        await client.delete("/data/hello.txt");
        console.log("✓ Cleanup done");

    } catch (err) {
        console.error("Error:", err.message);
    }
}

// Run example if in Node.js
if (typeof window === "undefined") {
    example();
}

// Export for both CommonJS and ES modules
if (typeof module !== "undefined" && module.exports) {
    module.exports = { Drive9Client };
}
