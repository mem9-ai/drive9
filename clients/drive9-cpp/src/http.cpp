#include "internal.hpp"
#include <httplib.h>
#include <algorithm>
#include <condition_variable>
#include <mutex>

namespace drive9 {
namespace detail {
void AcknowledgeRead(int socket) {
#if defined(__linux__) && defined(TCP_QUICKACK)
    // Affected Linux loopback connections have been observed with sk_rcvbuf
    // collapsed to 1371 bytes, advertising only 2 KiB and retransmitting every
    // ~208 ms. ACK hints alone cannot repair that receive-window starvation.
    // Preserve normal autotuning; only pin a buffer on an abnormally small
    // connection. SO_RCVBUF remains subject to the host's rmem_max limit.
    int receive_buffer = 0;
    socklen_t length = sizeof(receive_buffer);
    if (::getsockopt(socket, SOL_SOCKET, SO_RCVBUF, &receive_buffer, &length) == 0 &&
        receive_buffer < 64 * 1024) {
        int restored = 1024 * 1024;
        if (::setsockopt(socket, SOL_SOCKET, SO_RCVBUF, &restored, sizeof(restored)) == 0) {
            // Linux can also clamp the advertised window when the receive
            // allocation collapses. Enlarging sk_rcvbuf alone leaves that
            // clamp in place, so the connection still advances 2 KiB/RTO.
            (void)::setsockopt(socket, IPPROTO_TCP, TCP_WINDOW_CLAMP, &restored, sizeof(restored));
            int enabled = 1;
            (void)::setsockopt(socket, IPPROTO_TCP, TCP_QUICKACK, &enabled, sizeof(enabled));
        }
    }
#else
    (void)socket;
#endif
}
} // namespace detail
namespace {
struct URL {
    std::string origin, path;
};
URL SplitURL(const std::string &url) {
    auto scheme = url.find("://");
    if (scheme == std::string::npos || (url.substr(0, scheme) != "https" && url.substr(0, scheme) != "http"))
        throw Error("HTTP URL requires http or https");
    auto end = url.find_first_of("/?#", scheme + 3);
    auto authority = url.substr(scheme + 3, end - scheme - 3);
    if (authority.empty() || authority.find_first_of("@\\ \t\r\n") != std::string::npos || url.find('#') != std::string::npos)
        throw Error("Invalid HTTP authority or fragment");
    return {url.substr(0, end), end == std::string::npos ? "/" : (url[end] == '?' ? "/" : "") + url.substr(end)};
}
class HttplibTransport final : public HttpTransport {
    struct Pool {
        std::mutex mu;
        std::condition_variable cv;
        size_t active = 0;
        std::vector<std::unique_ptr<httplib::Client>> idle;
    };
    size_t limit_;
    std::mutex mu_;
    std::map<std::string, std::shared_ptr<Pool>> pools_;
public:
    explicit HttplibTransport(size_t limit) : limit_(limit) {
        if (!limit) throw Error("HTTP connection limit must be positive");
    }
    Response Request(const std::string &method, const std::string &url, const Headers &headers,
                     std::string_view body, Deadline deadline, size_t max_response) override {
        const auto address = SplitURL(url);
        std::shared_ptr<Pool> pool;
        {
            std::lock_guard<std::mutex> guard(mu_);
            auto &entry = pools_[address.origin];
            if (!entry) entry = std::make_shared<Pool>();
            pool = entry;
        }
        std::unique_ptr<httplib::Client> client;
        {
            std::unique_lock<std::mutex> guard(pool->mu);
            if (!pool->cv.wait_until(guard, deadline, [&] { return pool->active < limit_; }))
                throw Error("HTTP connection pool deadline exceeded");
            ++pool->active;
            if (!pool->idle.empty()) {
                client = std::move(pool->idle.back());
                pool->idle.pop_back();
            }
        }
        // Always return a slot, including URL/client construction failures.
        struct Release {
            std::shared_ptr<Pool> pool;
            std::unique_ptr<httplib::Client> &client;
            ~Release() {
                std::lock_guard<std::mutex> guard(pool->mu);
                if (client) pool->idle.push_back(std::move(client));
                --pool->active;
                pool->cv.notify_one();
            }
        } release{pool, client};
        if (!client) {
            client = std::make_unique<httplib::Client>(address.origin);
            client->set_keep_alive(true);
            client->set_follow_location(false);
            client->set_decompress(false);
            client->set_path_encode(false); // URI and SigV4 paths are already encoded
            client->enable_server_certificate_verification(true);
        }
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) throw Error("HTTP deadline exceeded");
        client->set_connection_timeout(std::min(remaining, std::chrono::milliseconds(10000)));
        client->set_read_timeout(remaining);
        client->set_write_timeout(remaining);
        client->set_max_timeout(remaining);
        client->set_payload_max_length(max_response);
        httplib::Request request;
        // The low-level send overload does not initialize the request clock.
        // max_timeout must measure from now, never time_point::min().
        request.start_time_ = std::chrono::steady_clock::now();
        request.method = method;
        request.path = address.path;
        for (const auto &header : headers) request.headers.insert(header);
        if (!body.empty()) {
            request.content_length_ = body.size();
            request.content_provider_ = [body, deadline](size_t offset, size_t length, httplib::DataSink &sink) {
                return std::chrono::steady_clock::now() < deadline && offset <= body.size() &&
                       length <= body.size() - offset && sink.write(body.data() + offset, length);
            };
        }
        Response result;
        request.content_receiver = [&](const char *data, size_t length, uint64_t, uint64_t) {
            if (std::chrono::steady_clock::now() >= deadline || length > max_response - result.body.size()) return false;
            result.body.append(data, length);
            detail::AcknowledgeRead(client->socket());
            return true;
        };
        httplib::Response response;
        httplib::Error error;
        bool ok = client->send(request, response, error);
        if (!ok || std::chrono::steady_clock::now() >= deadline) {
            client.reset(); // never reuse a stream with an incomplete response
            // A short object's size cap may abort a much larger error document.
            // Its parsed HTTP status is still authoritative (notably 404 after
            // compaction); an incomplete successful response remains an error.
            throw Error("HTTP request failed or exceeded its deadline", 0,
                        response.status >= 400 ? response.status : 0);
        }
        result.status = response.status;
        for (auto &header : response.headers) {
            auto name = header.first;
            std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) { return char(std::tolower(c)); });
            result.headers.emplace(std::move(name), header.second);
        }
        return result;
    }
};
} // namespace
std::shared_ptr<HttpTransport> MakeHttpTransport(size_t connections_per_origin) {
    return std::make_shared<HttplibTransport>(connections_per_origin);
}
} // namespace drive9
