#include "drive9/client.hpp"
#include <future>
#include <iostream>
#include <thread>
using namespace drive9;
using namespace std::chrono_literals;
template <class Fn> void Fails(Fn fn) { try { fn(); } catch (const Error &) { return; } throw Error("Expected HTTP failure"); }
int main(int argc, char **argv) {
    try {
        if (argc != 3) throw Error("Expected HTTP and untrusted TLS fixture endpoints");
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
        std::cout << "HTTP deadline, pooling, redirect and acknowledgement-loss tests passed\n";
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
