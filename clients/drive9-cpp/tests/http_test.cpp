#include "../src/internal.hpp"
#include <httplib.h>
#include <future>
#include <iostream>
#include <thread>
#include <poll.h>
using namespace drive9;
using namespace std::chrono_literals;
template <class Fn> void Fails(Fn fn) { try { fn(); } catch (const Error &) { return; } throw Error("Expected HTTP failure"); }
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
            request.start_time_ = std::chrono::steady_clock::now();
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
        mutation.start_time_ = std::chrono::steady_clock::now();
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
        std::cout << "HTTP deadline, pooling, redirect and acknowledgement-loss tests passed\n";
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
