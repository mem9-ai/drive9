#include "../src/internal.hpp"
#include <httplib.h>
#include <future>
#include <iostream>
#include <thread>
#include <poll.h>
#include <atomic>
using namespace drive9;
using namespace std::chrono_literals;
template <class Fn> void Fails(Fn fn) { try { fn(); } catch (const Error &) { return; } throw Error("Expected HTTP failure"); }

// Only metadata is synthetic: signed S3/GCS object requests use the real
// transport and HTTP fixture, including its response-size enforcement.
class CompactionFixture : public HttpTransport {
public:
    std::shared_ptr<HttpTransport> network = MakeHttpTransport(1);
    std::string base, scheme, framing;
    int status;
    std::atomic<int> mappings{0}, old_gets{0}, new_gets{0}, mints{0};
    CompactionFixture(std::string base, std::string scheme, std::string framing, int status)
        : base(std::move(base)), scheme(std::move(scheme)), framing(std::move(framing)), status(status) {}
    Response Request(const std::string &method, const std::string &url, const Headers &headers,
                     std::string_view body, Deadline deadline, size_t max_response) override {
        if (url == "http://fs.test/v1/data-credential") {
            ++mints;
            Json credential = {{"scheme", scheme}, {"endpoint", base + (scheme == "gcs" ? "/storage/v1/" : "")},
                {"bucket", "bucket"}, {"prefix", "tenant/"}, {"force_path_style", true},
                {"access_key_id", "test-access"}, {"secret_access_key", "test-secret"},
                {"access_token", "scoped-token"}, {"region", "us-east-1"}};
            return {200, {}, credential.dump()};
        }
        if (url == "http://fs.test/v1/extent/meta") {
            auto op = headers.at("X-Drive9-Extent-Op");
            auto args = Json::parse(body);
            Json result = {{"errno", 0}};
            if (op == "load") result["format"] = detail::EncodeBase64(Json({{"BlockSize", 4194304}}).dump());
            else if (op == "incr_counter") result["value"] = 1;
            else if (op == "new_session") result["session_guard"] = true;
            else if (op == "find_stale_sessions") result["sids"] = Json::array();
            else if (op == "lookup" || op == "getattr") {
                auto inode = op == "lookup" ? uint64_t(42) : args.at("inode").get<uint64_t>();
                result["inode"] = inode;
                result["attr"] = {{"Typ", inode == 1 ? 2 : 1}, {"Length", 1}};
            } else if (op == "read") {
                // A second metadata read observes the compactor's replacement.
                auto id = ++mappings == 1 ? uint64_t(1) : uint64_t(2);
                std::string slice;
                auto put = [&](uint64_t value, int bytes) {
                    for (int i = bytes - 1; i >= 0; --i) slice += char(value >> (i * 8));
                };
                put(0, 4); put(id, 8); put(1, 4); put(0, 4); put(1, 4);
                result["slices"] = detail::EncodeBase64(slice);
            }
            return {200, {}, result.dump()};
        }
        if (url.compare(0, base.size(), base) != 0 || method != "GET") throw Error("Unexpected object request");
        auto old = url.find("1_0_1") != std::string::npos;
        std::string path = "/byte";
        if (old) path = ++old_gets == 1 ? "/old-byte" : "/error/" + std::to_string(status) + "/" + framing;
        else ++new_gets;
        return network->Request(method, base + path, headers, body, deadline, max_response);
    }
};

void ShortObjectErrors(const std::string &base) {
    for (const auto &scheme : {"s3", "gcs"}) {
        for (const auto &framing : {"length", "chunked"}) {
            for (int status : {200, 400, 401, 403, 404, 429, 503}) {
                auto fixture = std::make_shared<CompactionFixture>(base, scheme, framing, status);
                auto client = std::make_shared<Client>(ClientOptions{"http://fs.test", "control-secret", 2s, fixture});
                ExtentOptions options;
                options.mapping_ttl = 1h; // Recovery must not depend on TTL expiry.
                options.cache_bytes = 0; // Keep the mapping, evict the old block.
                ExtentFileSystem fs(client, options);
                auto handle = fs.Open("/data");
                char byte = 0;
                if (fs.ReadAt(handle, &byte, 1, 0) != 1 || byte != 'A') throw Error("Unable to warm old mapping");
                if (status == 404) {
                    if (fs.ReadAt(handle, &byte, 1, 0) != 1 || byte != 'B' || fixture->mappings != 2 ||
                        fixture->old_gets != 2 || fixture->new_gets != 1 || fixture->mints != 1)
                        throw Error("Short-object 404 did not refresh exactly once");
                    fs.ReadAt(handle, &byte, 1, 0);
                    if (fixture->mappings != 2 || fixture->old_gets != 2) throw Error("Refreshed mapping was not retained");
                } else {
                    bool rejected = false;
                    try { fs.ReadAt(handle, &byte, 1, 0); }
                    catch (const Error &error) { rejected = error.status == (status == 200 ? 0 : status); }
                    // An oversized 200 must fail rather than expose a partial
                    // block; permanent 400s must not refresh or retry mappings.
                    int attempts = status == 400 ? 1 : 3;
                    if (!rejected || fixture->mappings != 1 || fixture->new_gets != 0 ||
                        fixture->old_gets != attempts + 1 || fixture->mints != attempts)
                        throw Error("Bounded HTTP error changed object retry or refresh policy");
                }
                fs.Close(handle);
                // An aborted error body must not poison the next pooled request.
                if (fixture->network->Request("GET", base + "/ok", {}, {}, std::chrono::steady_clock::now() + 2s).body != "ok")
                    throw Error("Error response poisoned pooled connection");
            }
        }
    }
}

int main(int argc, char **argv) {
    try {
        if (argc != 4) throw Error("Expected HTTP/TLS endpoints and fixture CA");
        std::string base = argv[1]; auto transport = MakeHttpTransport(2);
        Client client({base, "control-secret", 500ms, transport});
        auto deadline = [] { return std::chrono::steady_clock::now() + 2s; };
        Fails([&] { client.Request("GET", "/redirect"); });
        Fails([&] { transport->Request("GET", std::string(argv[2]) + "/ok", {}, {}, deadline()); });
        Fails([&] { transport->Request("GET", base + "/large", {}, {}, deadline(), 32); });
        Fails([&] { client.Request("POST", "/drop", {}, "mutation"); });
        auto start = std::chrono::steady_clock::now();
        Fails([&] { transport->Request("GET", base + "/slow", {}, {}, start + 180ms); });
        if (std::chrono::steady_clock::now() - start > 1s) throw Error("Slow body escaped overall deadline");
        auto a = std::async(std::launch::async, [&] { transport->Request("GET", base + "/hold", {}, {}, deadline()); });
        auto b = std::async(std::launch::async, [&] { transport->Request("GET", base + "/hold", {}, {}, deadline()); });
        std::this_thread::sleep_for(30ms);
        Fails([&] { transport->Request("GET", base + "/ok", {}, {}, std::chrono::steady_clock::now() + 30ms); });
        a.get(); b.get();
        if (client.Request("GET", "/ok").body != "ok") throw Error("Pool failed to recover from error");
        // Reuse pooled HTTP/1.1 sockets for binary blocks larger than the
        // stream buffer, including coexisting downloads on another connection.
        std::string block;
        for (int i = 0; i < 262144; ++i) block += char(i % 256);
        auto download = [&] {
            for (int i = 0; i < 16; ++i) {
                auto response = transport->Request("GET", base + "/block", {}, {}, deadline(), block.size());
                if (response.body != block) throw Error("Pooled binary block download changed bytes");
            }
        };
        download();
        auto first = std::async(std::launch::async, download);
        auto second = std::async(std::launch::async, download);
        first.get(); second.get();
#if defined(__linux__) && defined(TCP_QUICKACK)
        // A real tiny TCP receive window reproduces the 2 KiB / delayed-ACK
        // cadence seen in concurrent Linux scans. Exercise the same ACK hook
        // without capping production receive buffers or disabling keep-alive.
        for (bool acknowledge : {false, true}) {
            httplib::Client constrained(base);
            constrained.set_keep_alive(true);
            constrained.set_read_timeout(2s);
            constrained.set_max_timeout(2s);
            auto ready = constrained.Get("/ok");
            if (!ready || ready->body != "ok") throw Error("Unable to establish the fixture connection");
            // Shrink after SYN negotiation so the original large MSS/window
            // scale remains, matching the stalled keep-alive connection.
            int window = 1024;
            if (::setsockopt(constrained.socket(), SOL_SOCKET, SO_RCVBUF, &window, sizeof(window)))
                throw Error("Unable to constrain the established receive window");
            std::string received;
            httplib::Request request; request.method = "GET"; request.path = "/block";
            request.content_receiver = [&](const char *data, size_t length, uint64_t, uint64_t) {
                received.append(data, length);
                if (acknowledge) detail::AcknowledgeRead(constrained.socket());
                return true;
            };
            httplib::Response response; httplib::Error error;
            auto before = std::chrono::steady_clock::now();
            bool ok = constrained.send(request, response, error);
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - before).count();
            std::cout << "Constrained TCP window, quick acknowledgements=" << acknowledge
                      << ": " << elapsed << " ms, " << received.size() << " bytes\n";
            if (acknowledge && (!ok || received != block))
                throw Error("Quick acknowledgements did not recover a constrained receive window");
        }
#endif
        // Force close_notify after the server applies the request but before
        // cpp-httplib's post-write peer check. The out-of-band HTTP barrier
        // removes timing guesses from this TLS acknowledgement-loss window.
        httplib::SSLClient tls("127.0.0.1", std::stoi(std::string(argv[2]).substr(std::string(argv[2]).rfind(':') + 1)));
        httplib::tls::session_t session = nullptr;
        tls.set_session_verifier([&](httplib::tls::session_t value) {
            session = value; return httplib::SSLVerifierResponse::NoDecisionMade;
        });
        tls.set_ca_cert_path(argv[3]);
        tls.set_max_timeout(5s);
        tls.set_read_timeout(3s);
        tls.set_write_timeout(3s);
        tls.set_keep_alive(true);
        httplib::Request mutation;
        mutation.method = "POST"; mutation.path = "/tls-drop";
        mutation.content_length_ = 8;
        int applications = 0;
        auto after_write = [&] {
            ++applications;
            auto closed = transport->Request("GET", base + "/await-tls-drop?count=" + std::to_string(applications), {}, {}, deadline());
            if (closed.status != 200 || !session) throw Error("TLS fixture did not apply and close");
            auto fd = SSL_get_fd(static_cast<SSL *>(session));
            auto until = std::chrono::steady_clock::now() + 2s;
            while (!httplib::tls::is_peer_closed(session, fd)) {
                if (std::chrono::steady_clock::now() >= until) throw Error("TLS close_notify was not observed");
                pollfd ready{fd, POLLIN, 0}; ::poll(&ready, 1, 50);
            }
        };
        mutation.content_provider_ = [&](size_t offset, size_t length, httplib::DataSink &sink) {
            const char payload[] = "mutation";
            if (!sink.write(&payload[offset], length)) return false;
            if (offset + length == 8) after_write();
            return true;
        };
        httplib::Response response; httplib::Error error;
        if (tls.send(mutation, response, error)) throw Error("Lost TLS acknowledgement was accepted");
        if (applications != 1) throw Error("TLS mutation was replayed after acknowledgement loss");
        ShortObjectErrors(base);
        std::cout << "HTTP deadline, pooling, redirect and acknowledgement-loss tests passed\n";
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
