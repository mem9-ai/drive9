#include "internal.hpp"
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <future>
#include <list>
#include <set>
#include <shared_mutex>
#include <thread>

namespace drive9::detail {
std::vector<Slice> DecodeSlices(const Json &value) {
    auto bytes = DecodeBase64(value);
    if (bytes.size() % 24) throw Error("Corrupt extent slice map");
    auto integer = [&](size_t offset, size_t length) {
        uint64_t n = 0;
        for (size_t i = 0; i < length; ++i) n = (n << 8) | uint8_t(bytes[offset + i]);
        return n;
    };
    struct Event { uint32_t pos; size_t index; bool begin; };
    std::vector<Slice> slices;
    std::vector<Event> events;
    for (size_t offset = 0; offset < bytes.size(); offset += 24) {
        Slice s{uint32_t(integer(offset, 4)), uint32_t(integer(offset + 12, 4)),
                uint32_t(integer(offset + 16, 4)), uint32_t(integer(offset + 20, 4)), integer(offset + 4, 8)};
        if (s.pos > (64U << 20) || s.length > (64U << 20) - s.pos ||
            (s.id && (s.size > (64U << 20) || s.off > s.size || s.length > s.size - s.off)))
            throw Error("Invalid extent slice bounds");
        if (!s.length) continue;
        events.push_back({s.pos, slices.size(), true});
        events.push_back({s.pos + s.length, slices.size(), false});
        slices.push_back(s);
    }
    std::sort(events.begin(), events.end(), [](const Event &a, const Event &b) { return a.pos < b.pos; });
    std::set<size_t> active;
    std::vector<Slice> result;
    for (size_t i = 0; i < events.size();) {
        auto pos = events[i].pos;
        do {
            const auto &event = events[i++];
            if (event.begin) active.insert(event.index); else active.erase(event.index);
        } while (i < events.size() && events[i].pos == pos);
        if (i == events.size() || active.empty()) continue;
        const auto &source = slices[*active.rbegin()]; // last publication wins
        if (!source.id) continue;
        Slice s{pos, source.size, source.off + pos - source.pos, events[i].pos - pos, source.id};
        if (!result.empty() && result.back().id == s.id && result.back().size == s.size &&
            result.back().pos + result.back().length == s.pos && result.back().off + result.back().length == s.off)
            result.back().length += s.length;
        else result.push_back(s);
    }
    return result;
}
} // namespace drive9::detail

namespace drive9 {
namespace {
constexpr uint64_t ChunkSize = 64ULL << 20;
constexpr uint32_t BlockSize = 4U << 20;
constexpr uint64_t MaxFileSize = ChunkSize * (uint64_t(UINT32_MAX) + 1);
int64_t WallSeconds() { return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()); }
int64_t Ticks(Deadline value) { return std::chrono::duration_cast<std::chrono::nanoseconds>(value.time_since_epoch()).count(); }
FileInfo ParseInfo(const Json &response) {
    const auto &attr = response.at("attr");
    return {true, attr.at("Typ").get<int>() == 2, response.at("inode").get<uint64_t>(),
            attr.at("Length").get<uint64_t>(), attr.value("Mtime", int64_t(0)), attr.at("Typ").get<uint8_t>()};
}
} // namespace
struct ExtentFileSystem::Impl {
    std::shared_ptr<Client> client;
    ExtentOptions options;
    detail::ObjectStore objects;
    uint64_t sid = 0, next_handle = 0;
    uint64_t next_slice = 0, slice_limit = 0, next_inode = 0, inode_limit = 0;
    std::mutex allocation_mu;
    bool guard_supported = false, directory_projection = false, hash_prefix = false, closed = false;
    std::atomic<bool> failed{false};
    std::atomic<int64_t> expiry_ticks{INT64_MAX};
    // Lifetime gate: positional reads share it; writes/close/shutdown exclude
    // readers. Heartbeats have their own gate so slow reads cannot lose a lease.
    std::shared_mutex lifetime;
    std::mutex heartbeat_mu;
    std::condition_variable heartbeat_cv;
    bool stopping = false;
    std::thread heartbeat;
    struct Mapping { Deadline expires; std::shared_ptr<const std::vector<detail::Slice>> slices; };
    struct Node {
        uint64_t inode = 0;
        FileInfo info;
        std::mutex info_mu;
        Deadline info_expires = Deadline::min();
        size_t refs = 0;
        std::mutex map_mu;
        std::map<uint32_t, Mapping> maps;
    };
    struct Handle { std::shared_ptr<Node> node; bool write; Lock lock; };
    std::map<uint64_t, Handle> handles;
    std::map<uint64_t, std::weak_ptr<Node>> nodes;
    struct Block {
        std::shared_future<std::shared_ptr<const std::string>> future;
        std::list<std::string>::iterator lru;
        size_t bytes = 0;
    };
    std::mutex cache_mu;
    std::list<std::string> lru;
    std::map<std::string, Block> blocks;
    size_t cache_bytes = 0;

    Impl(std::shared_ptr<Client> client, ExtentOptions options)
        : client(std::move(client)), options(options), objects(this->client) {
        if (options.heartbeat < std::chrono::seconds(1) || options.heartbeat > std::chrono::minutes(10) || options.mapping_ttl.count() < 0)
            throw Error("Invalid extent heartbeat or cache configuration");
        Meta("init", {{"format", {{"Name", "drive9"}, {"UUID", "drive9-extent"}, {"Storage", "file"},
                                  {"BlockSize", BlockSize}, {"TrashDays", 0}}}, {"force", false}});
        auto format = Json::parse(detail::DecodeBase64(Meta("load", Json::object()).at("format")));
        auto block_size = format.value("BlockSize", BlockSize);
        if ((block_size != BlockSize && block_size != 4096) || !format.value("Compression", "").empty() ||
            !format.value("EncryptKey", "").empty()) throw Error("Unsupported extent format; refusing to change it");
        hash_prefix = format.value("HashPrefix", false);
        sid = Meta("incr_counter", {{"name", "nextSession"}, {"value", 1}}).at("value").get<uint64_t>();
        if (!sid) throw Error("Invalid allocated session ID");
        auto expire = NewExpiry();
        auto registration = Meta("new_session", {{"sid", sid}, {"expire", expire}, {"update", false},
                                                  {"info", detail::EncodeBase64(Json({{"Version", "drive9-cpp/0.1.0"}}).dump())}});
        guard_supported = registration.value("session_guard", false);
        directory_projection = registration.value("directory_projection", false);
        Confirm(expire);
        if (options.heartbeat < std::chrono::seconds(12) && !guard_supported) {
            ExpireSession();
            throw Error("Short extent heartbeats require server session_guard support");
        }
        heartbeat = std::thread([this] { Heartbeat(); });
    }
    ~Impl() { try { Shutdown(); } catch (...) {} }
    int64_t NewExpiry() const { return WallSeconds() + (options.heartbeat.count() * 5 + 999) / 1000; }
    void Confirm(int64_t expire) {
        auto remaining = std::chrono::system_clock::time_point(std::chrono::seconds(expire)) - std::chrono::system_clock::now();
        auto margin = std::min(options.heartbeat, std::chrono::milliseconds(5000));
        expiry_ticks.store(Ticks(std::chrono::steady_clock::now() + remaining - margin));
    }
    void Healthy() {
        if (Ticks(std::chrono::steady_clock::now()) >= expiry_ticks.load()) failed.store(true);
        if (failed.load()) throw Error("Extent session failed or expired; close and reopen, do not replay writes");
    }
    Deadline DeadlineFor(std::chrono::seconds timeout = std::chrono::seconds(30)) {
        Healthy();
        return std::min(std::chrono::steady_clock::now() + timeout,
                        Deadline(std::chrono::nanoseconds(expiry_ticks.load())));
    }
    Json Meta(const std::string &op, Json body) {
        Healthy();
        if (sid && op != "new_session") body["caller_sid"] = sid;
        Json response;
        try {
            response = client->ExtentMeta(op, body, DeadlineFor());
            Healthy();
        } catch (...) {
            failed.store(true);
            throw;
        }
        int code = response.at("errno").get<int>();
        if (code) {
            if (code == 5 || code == 70 || code == 116) failed.store(true);
            throw Error("Extent " + op + " rejected (errno " + std::to_string(code) + ")", code);
        }
        return response;
    }
    void Heartbeat() {
        std::unique_lock<std::mutex> lock(heartbeat_mu);
        while (!heartbeat_cv.wait_for(lock, options.heartbeat, [&] { return stopping; })) {
            lock.unlock();
            try {
                Healthy(); // a process resumed after SIGSTOP cannot renew old ownership
                auto expire = NewExpiry();
                Meta("refresh_session", {{"sid", sid}, {"expire", expire}});
                Confirm(expire);
                // Cleanup does not establish our ownership. Bound the entire
                // best-effort batch so it cannot stall the next renewal.
                try {
                    auto budget = std::min(std::chrono::milliseconds(1000), options.heartbeat / 4);
                    auto until = std::min(DeadlineFor(), std::chrono::steady_clock::now() + budget);
                    auto stale = client->ExtentMeta("find_stale_sessions", {{"limit", 16}, {"caller_sid", sid}}, until);
                    if (stale.at("errno") == 0) for (auto &old : stale.at("sids")) {
                        auto target = old.get<uint64_t>();
                        if (target && target != sid)
                            client->ExtentMeta("clean_stale_session", {{"sid", target}, {"caller_sid", sid}}, until);
                    }
                } catch (...) { /* Another session's cleanup must not poison this lease. */ }
            } catch (...) { failed.store(true); }
            lock.lock();
            if (failed.load()) break;
        }
    }
    void ExpireSession() {
        // Teardown runs after the heartbeat and all operations quiesce. Expire
        // our own existing row, then use the server's normal stale cleanup.
        client->ExtentMeta("refresh_session", {{"sid", sid}, {"expire", 1}});
        client->ExtentMeta("clean_stale_session", {{"sid", sid}});
    }
    void Shutdown() {
        // Keep renewing ownership while existing I/O drains. This gate also
        // serializes concurrent shutdown calls before joining the thread.
        std::unique_lock<std::shared_mutex> lock(lifetime);
        if (closed) return;
        {
            std::lock_guard<std::mutex> lock(heartbeat_mu);
            stopping = true;
        }
        heartbeat_cv.notify_all();
        if (heartbeat.joinable()) heartbeat.join();
        closed = true;
        handles.clear();
        nodes.clear();
        ExpireSession();
    }
    void Ready() { if (closed) throw Error("Extent filesystem is closed"); Healthy(); }
    FileInfo Lookup(const std::string &path) {
        auto result = Meta("getattr", {{"inode", 1}});
        result["inode"] = 1;
        if (path == "/") return ParseInfo(result);
        auto info = ParseInfo(result);
        std::string resolved;
        for (size_t start = 1; start < path.size();) {
            if (!info.directory) throw Error("Path component is not a directory", 20);
            auto end = path.find('/', start);
            if (end == std::string::npos) end = path.size();
            auto name = path.substr(start, end - start);
            resolved += "/" + name;
            auto parent = info.inode;
            try { info = ParseInfo(Meta("lookup", {{"parent", parent}, {"name", name}})); }
            catch (const Error &error) {
                if (error.code != 2) throw;
                // Existing classic directories are mirrored through the same
                // mknod transaction; regular files are never auto-converted.
                Response stat;
                try { stat = client->Request("HEAD", "/v1/fs" + UrlEncode(resolved + "/", true), {}, {}, DeadlineFor()); }
                catch (const Error &head) { if (head.status == 404) throw Error("Extent entry does not exist", 2); throw; }
                Healthy();
                if (stat.headers["x-dat9-isdir"] != "true") throw Error("Extent entry does not exist", 2);
                auto mode = stat.headers.count("x-dat9-mode") ? std::stoul(stat.headers.at("x-dat9-mode")) : 0755UL;
                try {
                    info = ParseInfo(Meta("mknod", {{"parent", parent}, {"name", name}, {"type", 2}, {"mode", mode},
                        {"inode", NewInode()}, {"proj_path", resolved + "/"}, {"attr", Json::object()}}));
                } catch (const Error &race) {
                    if (race.code != 17) throw;
                    info = ParseInfo(Meta("lookup", {{"parent", parent}, {"name", name}}));
                }
            }
            start = end + 1;
        }
        return info;
    }
    std::pair<uint64_t, std::string> Parent(const std::string &path) {
        if (path == "/") throw Error("Root has no parent entry", 22);
        auto pos = path.rfind('/');
        auto parent = Lookup(pos ? path.substr(0, pos) : "/");
        if (!parent.directory) throw Error("Parent is not a directory", 20);
        return {parent.inode, path.substr(pos + 1)};
    }
    Handle &Get(uint64_t handle) {
        Ready();
        auto found = handles.find(handle);
        if (found == handles.end()) throw Error("Extent handle is closed", 9);
        return found->second;
    }
    FileInfo CurrentInfo(Node &node) {
        std::lock_guard<std::mutex> lock(node.info_mu);
        if (std::chrono::steady_clock::now() >= node.info_expires) {
            auto result = Meta("getattr", {{"inode", node.inode}}); result["inode"] = node.inode;
            auto current = ParseInfo(result);
            if (current.size != node.info.size) {
                std::lock_guard<std::mutex> maps(node.map_mu);
                node.maps.clear();
            }
            node.info = current;
            node.info_expires = std::chrono::steady_clock::now() + options.mapping_ttl;
        }
        return node.info;
    }
    void SetLock(uint64_t inode, uint64_t handle, Lock lock, bool unlock = false) {
        try {
            Meta("setlk", {{"inode", inode}, {"sid", sid}, {"owner", handle},
                           {"ltype", unlock ? int('U') : lock == Lock::Exclusive ? int('W') : int('R')},
                           {"start", 0}, {"end", uint64_t(INT64_MAX)}, {"pid", 0}});
        } catch (const Error &error) {
            if (error.code == 11 || error.code == 35) throw Error("File is locked by another handle or client", error.code);
            throw;
        }
    }
    void DropRef(const std::shared_ptr<Node> &node) {
        if (--node->refs == 0) {
            // Erase before the RPC, including when teardown fails closed.
            // An expired weak_ptr retains make_shared's combined allocation.
            nodes.erase(node->inode);
            Meta("delete_sustained", {{"inode", node->inode}, {"sid", sid}});
        }
    }
    uint64_t Allocate(const char *name, uint64_t batch, uint64_t &next, uint64_t &limit) {
        std::lock_guard<std::mutex> lock(allocation_mu);
        if (next == limit) {
            auto result = Meta("incr_counter", {{"name", name}, {"value", batch}});
            const auto &value = result.at("value");
            if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<int64_t>() < 0)) {
                failed.store(true); throw Error("Invalid allocated ID range");
            }
            limit = value.get<uint64_t>();
            if (limit <= batch || limit > uint64_t(INT64_MAX)) {
                failed.store(true); throw Error("Invalid allocated ID range");
            }
            // Exactly the existing JuiceFS/drive9 reservation convention:
            // incr_counter returns the EXCLUSIVE upper bound, not an ID.
            next = limit - batch;
        }
        return next++;
    }
    uint64_t NewInode() { return Allocate("nextInode", 1024, next_inode, inode_limit); }
    uint64_t NewSlice() { return Allocate("nextChunk", 4096, next_slice, slice_limit); }
    std::string Key(uint64_t id, uint32_t block, uint32_t size) const {
        auto key = std::to_string(id / 1000000) + "/" + std::to_string(id / 1000) + "/" +
                   std::to_string(id) + "_" + std::to_string(block) + "_" + std::to_string(size);
        if (hash_prefix) {
            char prefix[4]; std::snprintf(prefix, sizeof(prefix), "%02X/", unsigned(id % 256));
            key = prefix + std::to_string(id / 1000000) + "/" + std::to_string(id) + "_" +
                  std::to_string(block) + "_" + std::to_string(size);
        }
        return "chunks/" + key;
    }
    std::shared_ptr<const std::string> ReadBlock(const std::string &key, size_t size) {
        std::shared_future<std::shared_ptr<const std::string>> future;
        std::shared_ptr<std::promise<std::shared_ptr<const std::string>>> publish;
        {
            std::lock_guard<std::mutex> lock(cache_mu);
            auto found = blocks.find(key);
            if (found != blocks.end()) {
                lru.splice(lru.begin(), lru, found->second.lru);
                future = found->second.future;
            } else {
                publish = std::make_shared<std::promise<std::shared_ptr<const std::string>>>();
                future = publish->get_future().share();
                lru.push_front(key);
                blocks.emplace(key, Block{future, lru.begin(), 0});
            }
        }
        if (!publish) return future.get();
        try {
            auto data = std::make_shared<const std::string>(objects.Get(key, size, DeadlineFor(std::chrono::seconds(60))));
            Healthy();
            {
                std::lock_guard<std::mutex> lock(cache_mu);
                blocks.at(key).bytes = size;
                cache_bytes += size;
                // In-flight requests are not evicted: other readers coalesce
                // their downloads. Completed objects are bounded by cache_bytes.
                for (auto it = lru.end(); cache_bytes > options.cache_bytes && it != lru.begin();) {
                    --it;
                    auto found = blocks.find(*it);
                    if (!found->second.bytes) continue;
                    cache_bytes -= found->second.bytes;
                    blocks.erase(found);
                    it = lru.erase(it);
                }
            }
            publish->set_value(data);
            return data;
        } catch (...) {
            publish->set_exception(std::current_exception());
            std::lock_guard<std::mutex> lock(cache_mu);
            auto found = blocks.find(key);
            if (found != blocks.end()) { cache_bytes -= found->second.bytes; lru.erase(found->second.lru); blocks.erase(found); }
            throw;
        }
    }
    std::shared_ptr<const std::vector<detail::Slice>> Map(Node &node, uint32_t index, bool refresh = false) {
        std::lock_guard<std::mutex> lock(node.map_mu);
        auto &mapping = node.maps[index];
        if (refresh || !mapping.slices || std::chrono::steady_clock::now() >= mapping.expires) {
            auto response = Meta("read", {{"inode", node.inode}, {"indx", index}});
            mapping.slices = std::make_shared<const std::vector<detail::Slice>>(detail::DecodeSlices(response.at("slices")));
            mapping.expires = std::chrono::steady_clock::now() + options.mapping_ttl;
        }
        return mapping.slices;
    }
    void Truncate(Handle &handle, uint64_t length) {
        if (!handle.write) throw Error("Extent handle is read only", 13);
        if (length > MaxFileSize || length > uint64_t(INT64_MAX)) throw Error("Extent size overflow", 22);
        try {
            auto result = Meta("truncate", {{"inode", handle.node->inode}, {"length", length}});
            handle.node->info.size = result.at("attr").at("Length").get<uint64_t>();
            handle.node->info_expires = std::chrono::steady_clock::now() + options.mapping_ttl;
            handle.node->maps.clear();
        } catch (...) { failed.store(true); throw; }
    }
};

ExtentFileSystem::ExtentFileSystem(std::shared_ptr<Client> client, ExtentOptions options) {
    if (!client) throw Error("Extent filesystem requires an HTTP client");
    if (options.heartbeat < std::chrono::seconds(1) || options.heartbeat > std::chrono::minutes(10) || options.mapping_ttl.count() < 0)
        throw Error("Invalid extent heartbeat or cache configuration");
    impl_ = std::make_unique<Impl>(std::move(client), options);
}
ExtentFileSystem::~ExtentFileSystem() = default;
uint64_t ExtentFileSystem::SessionID() const { return impl_->sid; }
size_t ExtentTestAccess::NodeCount(ExtentFileSystem &fs) {
    std::shared_lock<std::shared_mutex> lock(fs.impl_->lifetime);
    return fs.impl_->nodes.size();
}
bool ExtentFileSystem::SessionGuardSupported() const { return impl_->guard_supported; }
void ExtentFileSystem::Shutdown() { impl_->Shutdown(); }
FileInfo ExtentFileSystem::Stat(const std::string &input) {
    auto &s = *impl_; std::shared_lock<std::shared_mutex> lock(s.lifetime); s.Ready();
    try { return s.Lookup(CanonicalPath(input)); }
    catch (const Error &error) { if (error.code == 2) return {}; throw; }
}
uint64_t ExtentFileSystem::Open(const std::string &input, OpenOptions options) {
    auto &s = *impl_; std::unique_lock<std::shared_mutex> lock(s.lifetime); s.Ready();
    auto path = CanonicalPath(input);
    if (options.truncate && !options.write) throw Error("Truncate requires a writable handle", 13);
    auto parent = s.Parent(path);
    FileInfo info;
    try { info = s.Lookup(path); }
    catch (const Error &error) { if (error.code != 2) throw; }
    if (info.exists && options.exclusive) throw Error("File already exists", 17);
    if (!info.exists && options.create) {
        try {
            info = ParseInfo(s.Meta("mknod", {{"parent", parent.first}, {"name", parent.second}, {"type", 1}, {"inode", s.NewInode()},
                                              {"mode", 0600}, {"proj_path", path}, {"attr", Json::object()}}));
        } catch (const Error &error) {
            if (error.code != 17 || options.exclusive) throw;
            info = s.Lookup(path);
        }
    }
    if (!info.exists) throw Error("Extent file does not exist", 2);
    if (info.directory) throw Error("Extent file is a directory", 21);
    if (info.type != 1) throw Error("Extent node is not a supported regular file", 95);
    auto entry = s.nodes.find(info.inode);
    auto node = entry == s.nodes.end() ? nullptr : entry->second.lock();
    if (!node) { node = std::make_shared<Impl::Node>(); node->inode = info.inode; }
    node->info = info;
    node->info_expires = std::chrono::steady_clock::now() + s.options.mapping_ttl;
    if (!node->refs) s.Meta("open_ref", {{"sid", s.sid}, {"inode", info.inode}});
    ++node->refs;
    auto id = ++s.next_handle;
    bool acquired = false;
    try {
        // A rejected open_ref must not leave an expired registry entry.
        s.nodes[info.inode] = node;
        if (options.lock != Lock::None) {
            s.SetLock(info.inode, id, options.lock);
            acquired = true;
            auto result = s.Meta("getattr", {{"inode", info.inode}}); result["inode"] = info.inode;
            node->info = ParseInfo(result);
            node->maps.clear(); // authoritative snapshot after ownership handoff
        }
        Impl::Handle handle{node, options.write, options.lock};
        if (options.truncate) s.Truncate(handle, 0);
        s.handles.emplace(id, std::move(handle));
        return id;
    } catch (...) {
        auto error = std::current_exception();
        try { if (acquired) s.SetLock(info.inode, id, options.lock, true); } catch (...) {}
        try { s.DropRef(node); } catch (...) {}
        std::rethrow_exception(error);
    }
}
FileInfo ExtentFileSystem::Info(uint64_t handle) {
    auto &s = *impl_; std::shared_lock<std::shared_mutex> lock(s.lifetime); return s.CurrentInfo(*s.Get(handle).node);
}
size_t ExtentFileSystem::ReadAt(uint64_t handle, void *buffer, size_t length, uint64_t offset) {
    auto &s = *impl_; std::shared_lock<std::shared_mutex> lock(s.lifetime); auto &h = s.Get(handle);
    if (offset > MaxFileSize || length > MaxFileSize - offset || (!buffer && length)) throw Error("Invalid extent read bounds", 22);
    auto info = s.CurrentInfo(*h.node);
    if (offset >= info.size || !length) return 0;
    length = size_t(std::min<uint64_t>(length, info.size - offset));
    std::memset(buffer, 0, length); // holes must clear a reused DuckDB buffer
    for (size_t done = 0; done < length;) {
        auto index = uint32_t((offset + done) / ChunkSize), pos = uint32_t((offset + done) % ChunkSize);
        auto count = uint32_t(std::min<uint64_t>(length - done, ChunkSize - pos));
        // A compactor may have replaced a cached mapping and reclaimed an old
        // object. Refresh once on 404; never treat missing data as a sparse hole.
        for (int attempt = 0; ; ++attempt) {
            auto mapping = s.Map(*h.node, index, attempt > 0);
            try {
                if (attempt) std::memset(static_cast<char *>(buffer) + done, 0, count);
                auto first = std::lower_bound(mapping->begin(), mapping->end(), pos,
                    [](const detail::Slice &slice, uint32_t p) { return slice.pos + slice.length <= p; });
                for (auto it = first; it != mapping->end() && it->pos < pos + count; ++it) {
                    auto begin = std::max(pos, it->pos), end = std::min(pos + count, it->pos + it->length);
                    for (auto at = begin; at < end;) {
                        auto source = it->off + at - it->pos;
                        auto block = source / BlockSize, within = source % BlockSize;
                        auto block_size = std::min(BlockSize, it->size - block * BlockSize);
                        auto n = std::min(end - at, block_size - within);
                        auto data = s.ReadBlock(s.Key(it->id, block, block_size), block_size);
                        std::memcpy(static_cast<char *>(buffer) + done + at - pos, data->data() + within, n);
                        at += n;
                    }
                }
                break;
            } catch (const Error &error) { if (attempt || error.status != 404) throw; }
        }
        done += count;
    }
    s.Healthy();
    return length;
}
void ExtentFileSystem::WriteAt(uint64_t handle, const void *buffer, size_t length, uint64_t offset) {
    auto &s = *impl_; std::unique_lock<std::shared_mutex> lock(s.lifetime); auto &h = s.Get(handle);
    if (!h.write) throw Error("Extent handle is read only", 13);
    if (offset > MaxFileSize || length > MaxFileSize - offset || (!buffer && length)) throw Error("Invalid extent write bounds", 22);
    try {
        for (size_t done = 0; done < length;) {
            auto index = uint32_t((offset + done) / ChunkSize), pos = uint32_t((offset + done) % ChunkSize);
            auto count = uint32_t(std::min<uint64_t>(length - done, ChunkSize - pos));
            auto id = s.NewSlice();
            for (uint32_t block = 0, at = 0; at < count; ++block) {
                auto n = std::min(BlockSize, count - at);
                s.objects.Put(s.Key(id, block, n), std::string_view(static_cast<const char *>(buffer) + done + at, n), s.DeadlineFor(std::chrono::seconds(60)));
                s.Healthy();
                at += n;
            }
            auto response = s.Meta("write", {{"inode", h.node->inode}, {"indx", index}, {"off", pos},
                                             {"slice", {{"Id", id}, {"Size", count}, {"Off", 0}, {"Len", count}}},
                                             {"mtime", detail::Timestamp()}});
            h.node->info.size = response.at("attr").at("Length").get<uint64_t>();
            h.node->info_expires = std::chrono::steady_clock::now() + s.options.mapping_ttl;
            if (response.contains("slices"))
                h.node->maps[index] = {std::chrono::steady_clock::now() + s.options.mapping_ttl,
                    std::make_shared<const std::vector<detail::Slice>>(detail::DecodeSlices(response.at("slices")))};
            else h.node->maps.erase(index);
            done += count;
        }
        s.Healthy();
    } catch (...) { s.failed.store(true); throw; }
}
void ExtentFileSystem::Truncate(uint64_t handle, uint64_t length) {
    auto &s = *impl_; std::unique_lock<std::shared_mutex> lock(s.lifetime); s.Truncate(s.Get(handle), length);
}
void ExtentFileSystem::Sync(uint64_t handle) {
    // Writes publish synchronously under the exclusive lifetime lock. Taking a
    // shared lock waits for an active write without waiting for immutable reads.
    // A global exclusive Sync can starve behind scans while DuckDB holds its
    // commit mutex, preventing both query completion and the WAL flush.
    auto &s = *impl_; std::shared_lock<std::shared_mutex> lock(s.lifetime); s.Get(handle);
}
void ExtentFileSystem::Close(uint64_t handle) {
    auto &s = *impl_; std::unique_lock<std::shared_mutex> lock(s.lifetime); auto h = s.Get(handle);
    if (h.lock != Lock::None) s.SetLock(h.node->inode, handle, h.lock, true);
    s.handles.erase(handle);
    s.DropRef(h.node);
}
void ExtentFileSystem::Mkdir(const std::string &input) {
    auto &s = *impl_; std::unique_lock<std::shared_mutex> lock(s.lifetime); s.Ready();
    auto path = CanonicalPath(input);
    if (!s.directory_projection) throw Error("Server lacks transactional directory projection support", 95);
    auto parent = s.Parent(path);
    s.Meta("mknod", {{"parent", parent.first}, {"name", parent.second}, {"type", 2}, {"mode", 0700}, {"inode", s.NewInode()},
                      {"create_projection", true}, {"proj_path", path + "/"}, {"attr", Json::object()}});
}
void ExtentFileSystem::Remove(const std::string &input) {
    auto &s = *impl_; std::unique_lock<std::shared_mutex> lock(s.lifetime); s.Ready();
    auto path = CanonicalPath(input); auto parent = s.Parent(path); auto info = s.Lookup(path);
    if (info.directory && !s.directory_projection)
        throw Error("Server lacks consistent directory removal support", 95);
    s.Meta(info.directory ? "rmdir" : "unlink", {{"parent", parent.first}, {"name", parent.second},
               {"proj_path", info.directory ? path + "/" : path}, {"sid", s.sid}, {"opened", false}});
}
void ExtentFileSystem::Rename(const std::string &source, const std::string &target) {
    auto &s = *impl_; std::unique_lock<std::shared_mutex> lock(s.lifetime); s.Ready();
    auto from = CanonicalPath(source), to = CanonicalPath(target);
    auto info = s.Lookup(from);
    if (info.directory && !s.directory_projection)
        throw Error("Server lacks consistent directory rename support", 95);
    try {
        auto destination = s.Lookup(to);
        if (info.directory != destination.directory)
            throw Error("Rename destination has a different type", destination.directory ? 21 : 20);
    } catch (const Error &error) { if (error.code != 2) throw; }
    auto a = s.Parent(from), b = s.Parent(to);
    if (info.directory) { from += '/'; to += '/'; }
    s.Meta("rename", {{"src_parent", a.first}, {"src_name", a.second}, {"dst_parent", b.first}, {"dst_name", b.second},
                      {"flags", 0}, {"src_path", from}, {"dst_path", to}, {"sid", s.sid}, {"dst_opened", false}});
}
std::vector<DirectoryEntry> ExtentFileSystem::List(const std::string &input) {
    auto &s = *impl_; std::shared_lock<std::shared_mutex> lock(s.lifetime); s.Ready();
    auto info = s.Lookup(CanonicalPath(input));
    if (!info.directory) throw Error("Path is not a directory", 20);
    auto result = s.Meta("readdir", {{"inode", info.inode}, {"plus", 1}, {"limit", -1}});
    std::vector<DirectoryEntry> entries;
    for (auto &entry : result.at("entries")) {
        auto name = detail::DecodeBase64(entry.at("Name"));
        if (name == "." || name == "..") continue;
        entries.push_back({name, ParseInfo({{"inode", entry.at("Inode")}, {"attr", entry.at("Attr")}})});
    }
    return entries;
}
} // namespace drive9
