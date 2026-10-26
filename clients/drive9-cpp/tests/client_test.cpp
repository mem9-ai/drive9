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
    uint64_t counter = (1ULL << 53), sid = 0;
    Json attr = {{"Typ", 1}, {"Length", 0}, {"Mtime", 0}};
    std::map<uint32_t, std::string> maps;
    std::map<std::string, std::string> objects;
    std::vector<Json> calls;
    int refs = 0, puts = 0, gets = 0, publications = 0, mints = 0;
    bool exists = false, guarded = true, fail_ack = false, fail_put = false, removed = false;
    bool gate_put = false, put_entered = false, gate_get = false, get_entered = false;
    bool binding_change = false, refresh_auth = false;
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
        calls.push_back({{"op", op}, {"body", request}});
        if (sid && op != "new_session" && op != "clean_stale_session" && !(op == "refresh_session" && request.value("expire", int64_t(0)) == 1))
            Check(request.at("caller_sid").get<uint64_t>() == sid, "caller_sid lost 64-bit identity");
        Json response = {{"errno", 0}};
        if (op == "load") response["format"] = detail::EncodeBase64(Json({{"BlockSize", 4194304}}).dump());
        else if (op == "incr_counter") response["value"] = ++counter;
        else if (op == "new_session") { sid = request.at("sid"); response["session_guard"] = guarded; }
        else if (op == "refresh_session" && removed) response["errno"] = 116;
        else if (op == "find_stale_sessions") response["sids"] = Json::array();
        else if (op == "getattr" || op == "lookup") {
            if (op == "lookup" && !exists) response["errno"] = 2;
            else {
                response["inode"] = op == "getattr" ? request.at("inode") : Json(uint64_t(42));
                response["attr"] = response["inode"] == 1 ? Json({{"Typ", 2}, {"Length", 4096}}) : attr;
            }
        } else if (op == "mknod") { exists = true; response["inode"] = 42; response["attr"] = attr; }
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
int main() {
    try {
        PathsAndSlices(); DurableSparseAndReferences(); LostPublicationAck(); MalformedAcknowledgement(); UploadFailure(); LatePutRejected();
        ParallelReadsAndClose(); CredentialBindingAndRefresh(); LeaseAndLegacy(); GCSScopedTransport();
        std::cout << "C++ SDK protocol and fault tests passed\n";
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
