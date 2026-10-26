#include "drive9/client.hpp"
#include "../src/internal.hpp"
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>

using namespace drive9;
using namespace std::chrono_literals;
void Check(bool value, const char *message) { if (!value) throw std::runtime_error(message); }
template <class Fn> void Fails(Fn fn, const char *message) {
    try { fn(); } catch (const std::exception &) { return; }
    throw std::runtime_error(message);
}
std::string Slice(uint32_t pos, uint64_t id, uint32_t size, uint32_t off, uint32_t len) {
    std::string result;
    auto put = [&](uint64_t value, int bytes) { for (int i = bytes - 1; i >= 0; --i) result += char(value >> (8 * i)); };
    put(pos, 4); put(id, 8); put(size, 4); put(off, 4); put(len, 4);
    return result;
}

// Model existing HTTP operations, deliberately not a second filesystem wire
// protocol. Barriers expose ack loss, late PUT completion, and teardown races.
class Fixture : public HttpTransport {
public:
    std::mutex mu;
    std::condition_variable cv;
    uint64_t counter = (1ULL << 53), sid = 0, inode = 42;
    std::map<std::string, std::vector<std::pair<uint64_t, uint64_t>>> reservations;
    Json attr = {{"Typ", 1}, {"Length", 0}, {"Mtime", 0}};
    std::map<uint32_t, std::string> maps;
    std::map<std::string, std::string> objects;
    std::vector<Json> calls;
    int refs = 0, puts = 0, gets = 0, publications = 0, mints = 0;
    bool exists = false, guarded = true, directory_projection = true, fail_ack = false, fail_put = false, removed = false;
    bool gate_put = false, put_entered = false, gate_get = false, get_entered = false;
    bool binding_change = false, refresh_auth = false;
    bool fail_cleanup = false;
    int cleanup_calls = 0;
    std::string corrupt_op;
    Json corrupt_errno;
    Json credential = {{"scheme", "s3"}, {"endpoint", "http://object.test"}, {"bucket", "bucket"},
                       {"prefix", "tenant/"}, {"access_key_id", "test-access"}, {"secret_access_key", "test-secret"},
                       {"region", "us-east-1"}, {"force_path_style", true}};
    Response Request(const std::string &method, const std::string &url, const Headers &headers,
                     std::string_view body, Deadline deadline, size_t) override {
        std::unique_lock<std::mutex> lock(mu);
        if (std::chrono::steady_clock::now() >= deadline) throw Error("fixture deadline");
        if (url == "http://fs.test/v1/data-credential") {
            ++mints;
            auto c = credential;
            if (binding_change && mints > 1) c["prefix"] = "another-tenant/";
            return {200, {}, c.dump()};
        }
        if (url.compare(0, std::string("http://fs.test/v1/fs/").size(), "http://fs.test/v1/fs/") == 0) return {404, {}, {}};
        if (url.compare(0, 18, "http://object.test") == 0) {
            Check(headers.at("Authorization").find("AWS4-HMAC-SHA256 ") == 0, "S3 SigV4 authorization absent");
            if (refresh_auth && mints == 1) return {403, {}, {}};
            if (method == "PUT") {
                ++puts; put_entered = true; cv.notify_all();
                cv.wait(lock, [&] { return !gate_put; });
                if (fail_put) return {500, {}, {}};
                objects[url] = std::string(body);
                return {200, {}, {}};
            }
            ++gets; get_entered = true; cv.notify_all();
            cv.wait(lock, [&] { return !gate_get; });
            auto it = objects.find(url);
            if (it == objects.end()) return {404, {}, {}};
            return {200, {}, it->second};
        }
        Check(url == "http://fs.test/v1/extent/meta", "unexpected API path");
        auto request = Json::parse(body);
        auto op = headers.at("X-Drive9-Extent-Op");
        if (op == "find_stale_sessions" && fail_cleanup) {
            ++cleanup_calls; cv.notify_all(); return {503, {}, {}};
        }
        calls.push_back({{"op", op}, {"body", request}});
        if (sid && op != "new_session" && op != "clean_stale_session" && !(op == "refresh_session" && request.value("expire", int64_t(0)) == 1))
            Check(request.at("caller_sid").get<uint64_t>() == sid, "caller_sid lost 64-bit identity");
        Json response = {{"errno", 0}};
        if (op == "load") response["format"] = detail::EncodeBase64(Json({{"BlockSize", 4194304}}).dump());
        else if (op == "incr_counter") {
            auto delta = request.at("value").get<uint64_t>();
            counter += delta; response["value"] = counter;
            reservations[request.at("name").get<std::string>()].push_back({counter, delta});
        }
        else if (op == "new_session") { sid = request.at("sid"); response["session_guard"] = guarded; response["directory_projection"] = directory_projection; }
        else if (op == "refresh_session" && removed) response["errno"] = 116;
        else if (op == "find_stale_sessions") response["sids"] = Json::array();
        else if (op == "getattr" || op == "lookup") {
            if (op == "lookup" && !exists) response["errno"] = 2;
            else {
                response["inode"] = op == "getattr" ? request.at("inode") : Json(inode);
                response["attr"] = response["inode"] == 1 ? Json({{"Typ", 2}, {"Length", 4096}}) : attr;
            }
        } else if (op == "mknod") { exists = true; inode = request.value("inode", uint64_t(42)); response["inode"] = inode; response["attr"] = attr; }
        else if (op == "open_ref") ++refs;
        else if (op == "delete_sustained") --refs;
        else if (op == "read") response["slices"] = detail::EncodeBase64(maps[request.at("indx").get<uint32_t>()]);
        else if (op == "write") {
            ++publications;
            if (removed) response["errno"] = 116;
            else {
                auto slice = request.at("slice");
                auto index = request.at("indx").get<uint32_t>();
                auto pos = request.at("off").get<uint32_t>();
                auto count = slice.at("Len").get<uint32_t>();
                maps[index] += Slice(pos, slice.at("Id"), slice.at("Size"), slice.at("Off"), count);
                attr["Length"] = std::max(attr.at("Length").get<uint64_t>(), (uint64_t(index) << 26) + pos + count);
                response["attr"] = attr; response["slices"] = detail::EncodeBase64(maps[index]);
                if (fail_ack) throw Error("write committed but acknowledgement was lost");
            }
        } else if (op == "truncate") { attr["Length"] = request.at("length"); response["attr"] = attr; maps.clear(); }
        if (op == corrupt_op) response["errno"] = corrupt_errno;
        return {200, {}, response.dump()};
    }
    std::shared_ptr<Client> ClientFor() {
        return std::make_shared<Client>(ClientOptions{"http://fs.test", "control-secret", 30000ms,
                                                      std::shared_ptr<HttpTransport>(this, [](HttpTransport *) {})});
    }
};

void PathsAndSlices() {
    Check(CanonicalPath("//a/e\xcc\x81/") == "/a/\xc3\xa9", "NFC path mismatch");
    for (auto path : {"/../x", "/./x", "/a\\b", "/\x01", "/\xff"}) Fails([&] { CanonicalPath(path); }, "invalid path accepted");
    auto blob = Slice(0, 1, 16, 0, 16) + Slice(4, 2, 4, 0, 4) + Slice(6, 0, 0, 0, 4);
    auto slices = detail::DecodeSlices(detail::EncodeBase64(blob));
    Check(slices.size() == 3 && slices[0].length == 4 && slices[1].pos == 4 && slices[1].length == 2 &&
          slices[2].pos == 10 && slices[2].off == 10, "overlay or holes decoded incorrectly");
    Fails([] { detail::DecodeSlices("YQ=="); }, "corrupt slice map accepted");
    Fails([] { detail::DecodeBase64("AA=A"); }, "malformed base64 accepted");
}
void DurableSparseAndReferences() {
    Fixture f; ExtentFileSystem fs(f.ClientFor());
    auto file = fs.Open("/data", {true, true});
    auto other = fs.Open("/data");
    Check(f.refs == 1, "open_ref must be once per inode/session");
    fs.WriteAt(file, "abc", 3, 123);
    Check(f.puts == 1 && f.publications == 1, "write did not publish after PUT");
    char data[128]; std::memset(data, 0xa5, sizeof(data));
    Check(fs.ReadAt(other, data, sizeof(data), 0) == 126, "wrong sparse length");
    for (int i = 0; i < 123; ++i) Check(data[i] == 0, "sparse hole retained dirty buffer bytes");
    Check(std::string(data + 123, 3) == "abc", "wrong extent data");
    fs.Close(file); Check(f.refs == 1, "released another handle's open ref");
    fs.Close(other); Check(f.refs == 0, "last close leaked open ref");
}
void LostPublicationAck() {
    Fixture f; ExtentFileSystem fs(f.ClientFor()); auto file = fs.Open("/data", {true, true});
    f.fail_ack = true;
    Fails([&] { fs.WriteAt(file, "abc", 3, 0); }, "lost mutation ack was accepted");
    Fails([&] { fs.WriteAt(file, "abc", 3, 0); }, "failed session replayed a mutation");
    Check(f.publications == 1 && f.puts == 1, "ambiguous mutation was replayed");
    Fails([&] { fs.Sync(file); }, "sync succeeded after ambiguous write");
}
void CompatibleIDReservations() {
    Fixture f; auto client = f.ClientFor(); ExtentFileSystem fs(client); auto file = fs.Open("/data", {true, true});
    const auto inodes = f.reservations.at("nextInode").at(0);
    Check(fs.Info(file).inode == inodes.first - inodes.second, "mknod did not use a reserved inode");
    fs.WriteAt(file, "one", 3, 0); fs.WriteAt(file, "two", 3, 3);
    const auto slices = f.reservations.at("nextChunk");
    Check(slices.size() == 1 && slices[0].second == 4096, "slice allocation was not amortized");
    std::vector<uint64_t> ids;
    for (auto &call : f.calls) if (call.at("op") == "write") ids.push_back(call.at("body").at("slice").at("Id"));
    Check(ids.size() == 2 && ids[0] == slices[0].first - 4096 && ids[1] == ids[0] + 1, "slice IDs escaped the half-open reservation");
    auto next = client->ExtentMeta("incr_counter", {{"name", "nextChunk"}, {"value", 4096}, {"caller_sid", fs.SessionID()}});
    auto next_begin = next.at("value").get<uint64_t>() - 4096;
    Check(ids.back() < next_begin, "IDs overlap the next Go/SDK allocation");
}
void MalformedAcknowledgement() {
    for (auto bad : {Json(uint64_t(1) << 32), Json(-1), Json("0"), Json(nullptr), Json(1.5)}) {
        Fixture f; ExtentFileSystem fs(f.ClientFor()); auto file = fs.Open("/data", {true, true});
        f.corrupt_op = "getattr"; f.corrupt_errno = bad;
        Fails([&] { fs.Stat("/data"); }, "malformed errno acknowledgement was accepted");
        Fails([&] { fs.Sync(file); }, "malformed metadata acknowledgement did not fail the session");
    }
}
void UploadFailure() {
    Fixture f; ExtentFileSystem fs(f.ClientFor()); auto file = fs.Open("/data", {true, true});
    f.fail_put = true;
    Fails([&] { fs.WriteAt(file, "abc", 3, 0); }, "failed upload acknowledged");
    Check(f.publications == 0, "slice published before upload acknowledgement");
}
void LatePutRejected() {
    Fixture f; ExtentFileSystem fs(f.ClientFor()); auto file = fs.Open("/data", {true, true});
    f.gate_put = true;
    auto writer = std::async(std::launch::async, [&] { Fails([&] { fs.WriteAt(file, "abc", 3, 0); }, "removed session published a slice"); });
    {
        std::unique_lock<std::mutex> lock(f.mu); f.cv.wait(lock, [&] { return f.put_entered; });
        f.removed = true; f.gate_put = false;
    }
    f.cv.notify_all(); writer.get();
    Check(f.attr["Length"] == 0, "late stale publication changed file size");
}
void ParallelReadsAndClose() {
    Fixture f; ExtentFileSystem fs(f.ClientFor()); auto file = fs.Open("/data", {true, true});
    fs.WriteAt(file, "abcdefgh", 8, 0); f.gate_get = true;
    std::vector<std::future<void>> readers;
    for (int i = 0; i < 8; ++i) readers.push_back(std::async(std::launch::async, [&] {
        char bytes[8]; Check(fs.ReadAt(file, bytes, 8, 0) == 8 && std::string(bytes, 8) == "abcdefgh", "parallel read mismatch");
    }));
    { std::unique_lock<std::mutex> lock(f.mu); f.cv.wait(lock, [&] { return f.get_entered; }); }
    { std::lock_guard<std::mutex> lock(f.mu); f.gate_get = false; } f.cv.notify_all();
    for (auto &reader : readers) reader.get();
    Check(f.gets == 1, "concurrent cache misses duplicated downloads");
    fs.WriteAt(file, "new data", 8, 0);
    { std::lock_guard<std::mutex> lock(f.mu); f.gate_get = true; f.get_entered = false; }
    auto read = std::async(std::launch::async, [&] { char bytes[8]; fs.ReadAt(file, bytes, 8, 0); });
    { std::unique_lock<std::mutex> lock(f.mu); f.cv.wait(lock, [&] { return f.get_entered; }); }
    auto close = std::async(std::launch::async, [&] { fs.Close(file); });
    Check(close.wait_for(20ms) == std::future_status::timeout, "close overtook a read");
    { std::lock_guard<std::mutex> lock(f.mu); f.gate_get = false; } f.cv.notify_all();
    read.get(); close.get();
    char data; Fails([&] { fs.ReadAt(file, &data, 1, 0); }, "read after close accepted");
}
void SyncDoesNotWaitForDownloads() {
    Fixture f; ExtentFileSystem fs(f.ClientFor()); auto file = fs.Open("/data", {true, true});
    fs.WriteAt(file, "data", 4, 0);
    f.gate_get = true;
    auto reader = std::async(std::launch::async, [&] {
        char data[4]; Check(fs.ReadAt(file, data, 4, 0) == 4, "gated read failed");
    });
    { std::unique_lock<std::mutex> lock(f.mu); f.cv.wait(lock, [&] { return f.get_entered; }); }
    auto synced = std::async(std::launch::async, [&] { fs.Sync(file); });
    auto status = synced.wait_for(1s);
    // Release the read even on failure so future destruction cannot deadlock.
    { std::lock_guard<std::mutex> lock(f.mu); f.gate_get = false; } f.cv.notify_all();
    reader.get(); synced.get();
    Check(status == std::future_status::ready, "Sync waited for an unrelated immutable download");

    f.gate_put = true; f.put_entered = false;
    auto writer = std::async(std::launch::async, [&] { fs.WriteAt(file, "next", 4, 0); });
    { std::unique_lock<std::mutex> lock(f.mu); f.cv.wait(lock, [&] { return f.put_entered; }); }
    std::promise<void> started;
    auto waiting = std::async(std::launch::async, [&] { started.set_value(); fs.Sync(file); });
    started.get_future().wait();
    status = waiting.wait_for(50ms);
    { std::lock_guard<std::mutex> lock(f.mu); f.gate_put = false; } f.cv.notify_all();
    writer.get(); waiting.get();
    Check(status == std::future_status::timeout, "Sync overtook an unpublished write");
    Check(f.publications == 2, "Sync returned before the active write was published");
}
void CredentialBindingAndRefresh() {
    Fixture f; f.refresh_auth = true; f.binding_change = true;
    ExtentFileSystem fs(f.ClientFor()); auto file = fs.Open("/data", {true, true});
    Fails([&] { fs.WriteAt(file, "abc", 3, 0); }, "storage binding change accepted");
    Check(f.publications == 0, "binding change published data in another tenant");
    Fixture success; success.refresh_auth = true; ExtentFileSystem refreshed(success.ClientFor());
    auto h = refreshed.Open("/data", {true, true}); refreshed.WriteAt(h, "x", 1, 0);
    Check(success.mints == 2 && success.publications == 1, "scoped credentials were not renewed");
}
void LeaseAndLegacy() {
    Fixture invalid;
    Fails([&] { ExtentFileSystem fs(invalid.ClientFor(), ExtentOptions{999ms}); }, "invalid heartbeat accepted");
    Check(invalid.mints == 0, "invalid heartbeat contacted the server");
    Fails([] { ExtentFileSystem fs(nullptr); }, "null client accepted");
    Fixture legacy; legacy.guarded = false;
    Fails([&] { ExtentFileSystem fs(legacy.ClientFor(), ExtentOptions{1000ms}); }, "short legacy lease accepted");
    Fixture expired; ExtentFileSystem fs(expired.ClientFor(), ExtentOptions{1000ms});
    auto file = fs.Open("/data", {true, true});
    { std::lock_guard<std::mutex> lock(expired.mu); expired.removed = true; }
    auto deadline = std::chrono::steady_clock::now() + 4s;
    bool stopped = false;
    while (std::chrono::steady_clock::now() < deadline) {
        try { fs.Sync(file); }
        catch (const Error &) { stopped = true; break; }
        std::this_thread::sleep_for(5ms);
    }
    Check(stopped, "session did not stop after failed renewal");
    Fails([&] { fs.WriteAt(file, "x", 1, 0); }, "failed renewal allowed writes");
    Check(expired.puts == 0, "write uploaded after losing its session");
}
void MaintenanceDoesNotRevokeOwnership() {
    Fixture f; f.fail_cleanup = true;
    ExtentFileSystem fs(f.ClientFor(), ExtentOptions{1000ms}); auto h = fs.Open("/data", {true, true});
    {
        std::unique_lock<std::mutex> lock(f.mu);
        Check(f.cv.wait_for(lock, 4s, [&] { return f.cleanup_calls >= 2; }), "maintenance failure stopped lease renewal");
    }
    fs.WriteAt(h, "still-owned", 11, 0); fs.Sync(h);
}
void RefreshLengthAndRejectSymlinks() {
    Fixture f; ExtentOptions options; options.mapping_ttl = 0ms;
    ExtentFileSystem fs(f.ClientFor(), options); auto h = fs.Open("/data", {true, true});
    fs.WriteAt(h, "abc", 3, 0);
    { std::lock_guard<std::mutex> lock(f.mu); f.attr["Length"] = 8; }
    char data[8]; std::memset(data, 0xa5, 8);
    Check(fs.ReadAt(h, data, 8, 0) == 8 && std::string(data, 8) == std::string("abc\0\0\0\0\0", 8), "external growth remained hidden behind cached EOF");
    { std::lock_guard<std::mutex> lock(f.mu); f.attr["Length"] = 1; }
    Check(fs.Info(h).size == 1 && fs.ReadAt(h, data, 8, 0) == 1, "external truncate retained bytes past EOF");
    Fixture link; link.exists = true; link.attr["Typ"] = 3; link.attr["Length"] = 9;
    ExtentFileSystem links(link.ClientFor());
    Fails([&] { links.Open("/symlink"); }, "symlink was accepted as a sparse regular file");
    Check(link.refs == 0, "unsupported node acquired an open reference");
}
void GCSScopedTransport() {
    struct GCS : HttpTransport {
        std::vector<std::string> urls;
        int mints = 0;
        Response Request(const std::string &method, const std::string &url, const Headers &headers,
                         std::string_view body, Deadline, size_t) override {
            if (url == "http://fs.test/v1/data-credential") {
                ++mints;
                return {200, {}, Json({{"scheme", "gcs"}, {"endpoint", "https://storage.test/storage/v1/"},
                    {"bucket", "my-bucket"}, {"prefix", "tenant/"}, {"access_token", "scoped-object-token"}}).dump()};
            }
            Check(headers.at("Authorization") == "Bearer scoped-object-token", "GCS used the control-plane token");
            urls.push_back(url);
            if (url.find("/upload/") != std::string::npos)
                Check(method == "POST" && body == std::string_view("binary\0bytes", 12), "GCS media upload method/body mismatch");
            return {200, {}, method == "GET" ? std::string("binary\0bytes", 12) : "{}"};
        }
    };
    auto transport = std::make_shared<GCS>();
    auto client = std::make_shared<Client>(ClientOptions{"http://fs.test", "control-secret", 30000ms, transport});
    detail::ObjectStore store(client);
    store.Put("chunks/a+b", std::string_view("binary\0bytes", 12), std::chrono::steady_clock::now() + 1s);
    Check(store.Get("chunks/a+b", 12, std::chrono::steady_clock::now() + 1s) == std::string("binary\0bytes", 12), "GCS download changed bytes");
    Check(transport->urls.at(0) == "https://storage.test/upload/storage/v1/b/my-bucket/o?name=tenant%2Fchunks%2Fa%2Bb&uploadType=media", "GCS upload URL mismatch");
    Check(transport->urls.at(1) == "https://storage.test/storage/v1/b/my-bucket/o/tenant%2Fchunks%2Fa%2Bb?alt=media", "GCS download URL mismatch");
}
void DefaultCloudCredentials() {
    struct Provider : HttpTransport {
        Json credential;
        std::string object_url;
        Headers object_headers;
        Response Request(const std::string &, const std::string &url, const Headers &headers,
                         std::string_view, Deadline, size_t) override {
            if (url == "http://fs.test/v1/data-credential") return {200, {}, credential.dump()};
            object_url = url; object_headers = headers;
            return {200, {}, "{}"};
        }
    };
    for (const auto &test : std::vector<std::pair<std::string, std::string>>{
             {"us-west-2", "amazonaws.com"}, {"cn-north-1", "amazonaws.com.cn"},
             {"eusc-de-east-1", "amazonaws.eu"}, {"us-gov-west-1", "amazonaws.com"},
             {"us-iso-east-1", "c2s.ic.gov"}, {"us-isob-east-1", "sc2s.sgov.gov"},
             {"eu-isoe-west-1", "cloud.adc-e.uk"}, {"us-isof-east-1", "csp.hci.ic.gov"}}) {
        auto provider = std::make_shared<Provider>();
        provider->credential = {{"scheme", "s3"}, {"bucket", "bucket"}, {"prefix", "tenant"}, {"region", test.first},
                                 {"access_key_id", "test-access"}, {"secret_access_key", "test-secret"}};
        auto client = std::make_shared<Client>(ClientOptions{"http://fs.test", "control-secret", 30000ms, provider});
        detail::ObjectStore store(client); store.Put("chunks/key", "data", std::chrono::steady_clock::now() + 1s);
        Check(provider->object_url == "https://bucket.s3." + test.first + "." + test.second + "/tenant/chunks/key", "default S3 partition or bucket style mismatch");
        Check(provider->object_headers.at("Authorization").find("/" + test.first + "/s3/aws4_request") != std::string::npos, "S3 signer ignored minted region");
        auto raw = provider->credential; raw["endpoint"] = "https://s3.amazonaws.com";
        Check(detail::NormalizeCredential(raw).at("endpoint") == "https://s3." + test.first + "." + test.second, "global endpoint overrode minted region");
    }
    auto provider = std::make_shared<Provider>();
    provider->credential = {{"scheme", "s3"}, {"endpoint", "https://custom.test"}, {"bucket", "dotted.bucket"},
        {"prefix", "tenant/"}, {"region", "eu-west-1"}, {"access_key_id", "test-access"}, {"secret_access_key", "test-secret"}};
    auto client = std::make_shared<Client>(ClientOptions{"http://fs.test", "control-secret", 30000ms, provider});
    for (auto endpoint : {"https://s3.dualstack.us-west-2.amazonaws.com", "https://s3-fips.us-west-2.amazonaws.com",
                          "https://vpce-001.s3.us-west-2.vpce.amazonaws.com", "https://custom.example:9000/root/"}) {
        auto c = provider->credential; c["endpoint"] = endpoint;
        Check(detail::NormalizeCredential(c).at("endpoint") == endpoint, "custom S3 endpoint was relocated");
    }
    { detail::ObjectStore store(client); store.Put("chunks/key", "data", std::chrono::steady_clock::now() + 1s); }
    Check(provider->object_url == "https://custom.test/dotted.bucket/tenant/chunks/key", "dotted bucket did not use TLS-safe path addressing");
    provider->credential = {{"scheme", "gcs"}, {"bucket", "bucket"}, {"prefix", "tenant/"}, {"access_token", "scoped-token"}};
    { detail::ObjectStore store(client); store.Put("chunks/key", "data", std::chrono::steady_clock::now() + 1s); }
    Check(provider->object_url == "https://storage.googleapis.com/upload/storage/v1/b/bucket/o?name=tenant%2Fchunks%2Fkey&uploadType=media", "default GCS JSON API endpoint mismatch");
    Check(provider->object_headers.at("Authorization") == "Bearer scoped-token", "GCS used control-plane credentials");
    for (auto endpoint : {"http://remote.test/storage/v1/", "https://remote.test/proxy/storage/v1/", "https://remote.test"}) {
        provider->credential["endpoint"] = endpoint;
        Fails([&] { detail::ObjectStore store(client); }, "unsafe GCS endpoint accepted");
    }
    provider->credential["endpoint"] = "http://127.0.0.1:8080/storage/v1/";
    { detail::ObjectStore store(client); }
}
void LegacyDirectoryRenameIsRejected() {
    Fixture f; f.directory_projection = false; f.exists = true; f.attr["Typ"] = 2;
    ExtentFileSystem fs(f.ClientFor());
    bool rejected = false;
    try { fs.Rename("/source", "/target"); }
    catch (const Error &error) { rejected = error.code == 95; }
    Check(rejected, "legacy server accepted an unsafe directory rename");
    rejected = false;
    try { fs.Remove("/source"); }
    catch (const Error &error) { rejected = error.code == 95; }
    Check(rejected, "legacy server accepted an unsafe directory removal");
    for (auto &call : f.calls)
        Check(call.at("op") != "rename" && call.at("op") != "rmdir", "legacy directory mutation reached the server");
}
void DirectoryProjectionCapability() {
    auto fixture = std::make_shared<Fixture>(); fixture->directory_projection = false;
    auto client = std::make_shared<Client>(ClientOptions{"http://fs.test", "token", 30s, fixture});
    ExtentFileSystem fs(client);
    Fails([&] { fs.Mkdir("/unsupported"); }, "legacy server silently created an unprojected directory");
    for (auto &call : fixture->calls) Check(call.at("op") != "mknod", "unsupported mkdir mutated the legacy namespace");
    OpenOptions writer; writer.create = writer.write = true;
    auto file = fs.Open("/regular", writer); fs.WriteAt(file, "ok", 2, 0); fs.Close(file);
}

void RenameNamespaceValidation() {
    struct NamespaceFixture : Fixture {
        std::map<std::string, int> entries;
        std::map<std::string, int> classic_dirs;
        Json rename;
        int lookup_error = 0;
        bool omit_capability = false;
        Response Request(const std::string &method, const std::string &url, const Headers &headers,
                         std::string_view body, Deadline deadline, size_t limit) override {
            if (method == "HEAD") {
                auto found = classic_dirs.find(url);
                if (found != classic_dirs.end()) return {200, {{"x-dat9-isdir", "true"}, {"x-dat9-mode", "493"}}, {}};
            }
            if (url == "http://fs.test/v1/extent/meta") {
                auto op = headers.at("X-Drive9-Extent-Op");
                auto request = Json::parse(body);
                if (op == "lookup") {
                    auto name = request.at("name").get<std::string>();
                    if (name == "target" && lookup_error) return {200, {}, Json({{"errno", lookup_error}}).dump()};
                    auto found = entries.find(name);
                    if (found == entries.end()) return {200, {}, "{\"errno\":2}"};
                    return {200, {}, Json({{"errno", 0}, {"inode", name == "source" ? 42 : 43},
                        {"attr", {{"Typ", found->second}, {"Length", 0}}}}).dump()};
                }
                if (op == "mknod") {
                    Check(!request.value("create_projection", false), "lookup must only mirror an existing directory");
                    entries[request.at("name")] = 2;
                    return {200, {}, Json({{"errno", 0}, {"inode", request.at("inode")},
                        {"attr", {{"Typ", 2}, {"Length", 4096}}}}).dump()};
                }
                if (op == "rename") { rename = request; return {200, {}, "{\"errno\":0}"}; }
                auto result = Fixture::Request(method, url, headers, body, deadline, limit);
                if (op == "new_session" && omit_capability) {
                    auto response = Json::parse(result.body); response.erase("directory_projection"); result.body = response.dump();
                }
                return result;
            }
            return Fixture::Request(method, url, headers, body, deadline, limit);
        }
    };
    auto expect_errno = [](auto fn, int code) {
        try { fn(); } catch (const Error &error) { Check(error.code == code, "rename returned the wrong errno"); return; }
        throw std::runtime_error("unsafe rename succeeded");
    };
    for (bool omitted : {false, true}) {
        NamespaceFixture f; f.directory_projection = false; f.omit_capability = omitted;
        // Lookup is still allowed to mirror a classic source on old servers.
        f.classic_dirs["http://fs.test/v1/fs/source/"] = 2;
        ExtentFileSystem fs(f.ClientFor());
        expect_errno([&] { fs.Rename("/source/", "/target/"); }, 95);
        Check(f.rename.is_null(), "legacy directory rename sent a mutation");
        f.entries["source"] = 1;
        fs.Rename("/source", "/target");
        Check(f.rename.at("src_path") == "/source" && f.rename.at("dst_path") == "/target", "legacy file rename stopped working");
    }
    for (bool capable : {false, true}) {
        NamespaceFixture f; f.directory_projection = capable; f.entries["source"] = 1;
        f.classic_dirs["http://fs.test/v1/fs/target/"] = 2;
        ExtentFileSystem fs(f.ClientFor());
        expect_errno([&] { fs.Rename("/source", "/target"); }, 21);
        Check(f.entries.count("target") == 1 && f.rename.is_null(), "classic directory destination was not validated");
    }
    NamespaceFixture f; f.entries = {{"source", 2}, {"target", 1}};
    ExtentFileSystem fs(f.ClientFor());
    expect_errno([&] { fs.Rename("/source", "/target"); }, 20);
    Check(f.rename.is_null(), "directory replaced a regular file");
    f.lookup_error = 13;
    expect_errno([&] { fs.Rename("/source", "/target"); }, 13);
    Check(f.rename.is_null(), "destination lookup error was ignored");
    f.lookup_error = 0; f.entries.erase("target");
    fs.Rename("/source", "/target");
    Check(f.rename.at("src_path") == "/source/" && f.rename.at("dst_path") == "/target/", "capable directory rename lost its paths");
}
int main() {
    try {
        DirectoryProjectionCapability(); LegacyDirectoryRenameIsRejected(); RenameNamespaceValidation();
        PathsAndSlices(); DurableSparseAndReferences(); CompatibleIDReservations(); LostPublicationAck(); MalformedAcknowledgement(); UploadFailure(); LatePutRejected();
        ParallelReadsAndClose(); SyncDoesNotWaitForDownloads(); CredentialBindingAndRefresh(); LeaseAndLegacy(); MaintenanceDoesNotRevokeOwnership();
        RefreshLengthAndRejectSymlinks(); GCSScopedTransport(); DefaultCloudCredentials();
        std::cout << "C++ SDK protocol and fault tests passed\n";
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
