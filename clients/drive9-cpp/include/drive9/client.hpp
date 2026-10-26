#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <json.hpp>

namespace drive9 {
using Json = nlohmann::json;
using Deadline = std::chrono::steady_clock::time_point;
using Headers = std::map<std::string, std::string>;

// Extent codes are the server's errno values, not the client's host enum.
// HTTP errors carry status; transport errors use 0.
class Error : public std::runtime_error {
public:
    Error(std::string message, int code = 0, int status = 0)
        : std::runtime_error(std::move(message)), code(code), status(status) {}
    int code;
    int status;
};

struct Response {
    int status = 0;
    Headers headers; // lowercase names
    std::string body;
};

class HttpTransport {
public:
    virtual ~HttpTransport() = default;
    // No redirects or implicit mutation retries: metadata has an ambiguous
    // outcome on disconnect and must never be replayed automatically.
    virtual Response Request(const std::string &method, const std::string &url,
                             const Headers &headers, std::string_view body,
                             Deadline deadline, size_t max_response = 64 << 20) = 0;
};
std::shared_ptr<HttpTransport> MakeHttpTransport(size_t connections_per_origin = 32);

struct ClientOptions {
    std::string endpoint;
    std::string token;
    std::chrono::milliseconds timeout{30000};
    std::shared_ptr<HttpTransport> transport;
};

// All methods use existing public HTTP APIs. JsonRequest also exposes the rest
// of the API without a second hand-maintained copy of every server schema.
class Client {
public:
    explicit Client(ClientOptions options);
    Response Request(const std::string &method, const std::string &path,
                     const Headers &headers = {}, std::string_view body = {},
                     Deadline deadline = Deadline::max()) const;
    Json JsonRequest(const std::string &method, const std::string &path, const Json &body,
                     Deadline deadline = Deadline::max()) const;
    Json ExtentMeta(const std::string &op, const Json &body, Deadline deadline = Deadline::max()) const;
    Json DataCredential(Deadline deadline = Deadline::max()) const;
    Response ReadFile(const std::string &path, const Headers &conditions = {}) const;
    Response WriteFile(const std::string &path, std::string_view data, const Headers &conditions = {}) const;
    std::shared_ptr<HttpTransport> Transport() const;
private:
    ClientOptions options_;
};

// Absolute NFC UTF-8 path, matching the server's path rules. URI routing belongs
// to consumers, not to the filesystem protocol.
std::string CanonicalPath(const std::string &path);
std::string UrlEncode(std::string_view input, bool preserve_slashes = false);

enum class Lock { None, Shared, Exclusive };
struct OpenOptions {
    bool write = false;
    bool create = false;
    bool exclusive = false;
    bool truncate = false;
    Lock lock = Lock::None;
};
struct FileInfo {
    bool exists = false;
    bool directory = false;
    uint64_t inode = 0;
    uint64_t size = 0;
    int64_t modified = 0;
    uint8_t type = 0; // existing extent node type: regular=1, directory=2, symlink=3
};
struct DirectoryEntry { std::string name; FileInfo info; };
struct ExtentOptions {
    std::chrono::milliseconds heartbeat{12000};
    size_t cache_bytes = 64 << 20;
    std::chrono::milliseconds mapping_ttl{1000};
};

class ExtentFileSystem {
public:
    explicit ExtentFileSystem(std::shared_ptr<Client> client, ExtentOptions options = {});
    ~ExtentFileSystem();
    ExtentFileSystem(const ExtentFileSystem &) = delete;
    ExtentFileSystem &operator=(const ExtentFileSystem &) = delete;
    FileInfo Stat(const std::string &path);
    uint64_t Open(const std::string &path, OpenOptions options = {});
    FileInfo Info(uint64_t handle);
    size_t ReadAt(uint64_t handle, void *destination, size_t length, uint64_t offset);
    // Durable on return: immutable object PUTs and slice publication have both
    // been acknowledged. An ambiguous mutation permanently fails the session.
    void WriteAt(uint64_t handle, const void *source, size_t length, uint64_t offset);
    void Truncate(uint64_t handle, uint64_t length);
    void Sync(uint64_t handle);
    void Close(uint64_t handle);
    void Mkdir(const std::string &path);
    void Remove(const std::string &path);
    void Rename(const std::string &source, const std::string &target);
    std::vector<DirectoryEntry> List(const std::string &path);
    void Shutdown();
    uint64_t SessionID() const;
    bool SessionGuardSupported() const;
private:
    friend struct ExtentTestAccess;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace drive9
