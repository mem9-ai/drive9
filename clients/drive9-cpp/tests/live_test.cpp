#include "drive9/client.hpp"
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>

using namespace drive9;
using namespace std::chrono_literals;
void Check(bool value, const char *message) { if (!value) throw Error(message); }
template <class Fn> void Fails(Fn fn, const char *message) {
    try { fn(); } catch (const Error &) { return; }
    throw Error(message);
}
class Gate : public HttpTransport {
public:
    std::shared_ptr<HttpTransport> transport = MakeHttpTransport();
    std::mutex mu;
    std::condition_variable cv;
    bool armed = false, entered = false, released = false;
    Response Request(const std::string &method, const std::string &url, const Headers &headers,
                     std::string_view body, Deadline deadline, size_t max_response) override {
        {
            std::unique_lock<std::mutex> lock(mu);
            auto op = headers.find("X-Drive9-Extent-Op");
            if (armed && op != headers.end() && op->second == "write") {
                entered = true; cv.notify_all();
                if (!cv.wait_until(lock, deadline, [&] { return released; })) throw Error("Gate deadline exceeded");
            }
        }
        return transport->Request(method, url, headers, body, deadline, max_response);
    }
    void Release() { { std::lock_guard<std::mutex> lock(mu); released = true; } cv.notify_all(); }
};
int main() {
    auto endpoint = std::getenv("DRIVE9_BASE"), token = std::getenv("DRIVE9_API_KEY");
    if (!endpoint || !token) { std::cout << "Requires disposable DRIVE9_BASE and DRIVE9_API_KEY\n"; return 77; }
    try {
        auto client = std::make_shared<Client>(ClientOptions{endpoint, token});
        ExtentFileSystem a(client), b(client);
        auto root = "/cpp-conformance-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        a.Mkdir(root);
        auto path = root + "/binary";
        OpenOptions writer; writer.create = writer.write = true; writer.lock = Lock::Exclusive;
        OpenOptions reader; reader.lock = Lock::Shared;
        auto h = a.Open(path, writer);
        Fails([&] { b.Open(path, writer); }, "second writer acquired a record lock");
        Fails([&] { b.Open(path, reader); }, "reader acquired a writer's record lock");
        auto boundary = (64ULL << 20);
        const std::string pattern("\0\xff\x03" "abcdefgh", 11);
        a.WriteAt(h, pattern.data(), pattern.size(), boundary - 5);
        char data[32]; std::memset(data, 0xa5, sizeof(data));
        Check(a.ReadAt(h, data, sizeof(data), boundary - 12) == 18, "cross-chunk EOF mismatch");
        Check(std::string(data, 7) == std::string(7, '\0') && std::string(data + 7, 11) == pattern, "cross-chunk sparse/binary mismatch");
        a.Truncate(h, 2); a.Truncate(h, boundary + 6);
        std::memset(data, 0xa5, sizeof(data)); a.ReadAt(h, data, 18, boundary - 12);
        Check(std::string(data, 18) == std::string(18, '\0'), "truncate/regrow resurrected old slices");
        a.Truncate(h, 0); a.WriteAt(h, "fresh ownership", 15, 0); a.Close(h);
        auto first = b.Open(path, reader);
        Check(b.Info(first).size == 15, "lock handoff retained a stale file length");
        Check(b.ReadAt(first, data, 15, 0) == 15 && std::string(data, 15) == "fresh ownership", "lock handoff retained stale slices");
        auto second = a.Open(path, reader);
        Fails([&] { b.Open(path, writer); }, "writer bypassed shared record locks");
        b.Close(first); a.Close(second);
        auto old = a.Open(path);
        a.Remove(path);
        Check(!b.Stat(path).exists, "unlink left a visible path");
        Check(a.ReadAt(old, data, 15, 0) == 15, "unlink reclaimed an open inode");
        a.Close(old);
        auto source = a.Open(root + "/source", writer); a.WriteAt(source, "new", 3, 0); a.Close(source);
        auto target = a.Open(root + "/target", writer); a.WriteAt(target, "old", 3, 0); a.Close(target);
        auto held = b.Open(root + "/target");
        a.Rename(root + "/source", root + "/target");
        Check(b.ReadAt(held, data, 3, 0) == 3 && std::string(data, 3) == "old", "rename reclaimed replaced open inode");
        b.Close(held);
        Check(a.List(root).size() == 1, "directory listing disagrees with rename/unlink");
        a.Remove(root + "/target");

        if (a.SessionGuardSupported()) {
            auto gate = std::make_shared<Gate>();
            auto old_client = std::make_shared<Client>(ClientOptions{endpoint, token, 30000ms, gate});
            ExtentFileSystem stale(old_client);
            auto stale_file = stale.Open(root + "/fenced", writer);
            { std::lock_guard<std::mutex> lock(gate->mu); gate->armed = true; }
            auto delayed = std::async(std::launch::async, [&] {
                Fails([&] { stale.WriteAt(stale_file, "old", 3, 0); }, "old session published after cleanup and takeover");
            });
            try {
                {
                    std::unique_lock<std::mutex> lock(gate->mu);
                    Check(gate->cv.wait_for(lock, 15s, [&] { return gate->entered; }), "publication never reached gate");
                }
                auto expire = client->ExtentMeta("refresh_session", {{"sid", stale.SessionID()}, {"expire", 1}});
                Check(expire.at("errno") == 0, "could not expire isolated session");
                auto cleanup = client->ExtentMeta("clean_stale_session", {{"sid", stale.SessionID()}});
                Check(cleanup.at("errno") == 0, "could not clean expired session");
                auto current = a.Open(root + "/fenced", writer);
                a.WriteAt(current, "current", 7, 0);
                gate->Release(); delayed.get();
                auto fresh = b.Open(root + "/fenced");
                Check(b.ReadAt(fresh, data, 7, 0) == 7 && std::string(data, 7) == "current", "late publication overwrote current owner");
                b.Close(fresh); a.Close(current);
            } catch (...) { gate->Release(); delayed.wait(); throw; }
            a.Remove(root + "/fenced");
            std::cout << "Live transactional stale-session publication test passed\n";
        } else if (std::getenv("TIDBFS_REQUIRE_SESSION_GUARD")) throw Error("Server did not advertise session_guard");
        else std::cout << "Legacy server: transactional stale-session test unavailable\n";
        a.Remove(root);
        std::cout << "Live C++ extent compatibility, sparse/chunk boundaries, locks, handoff and open-reference tests passed\n";
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
