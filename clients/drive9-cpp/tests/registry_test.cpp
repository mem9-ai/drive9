#include "../src/internal.hpp"
#include <iostream>

using namespace drive9;
using namespace std::chrono_literals;
void Check(bool value, const char *message) { if (!value) throw std::runtime_error(message); }
enum class Failure { None, OpenRef, Lock, LockedAttr, Unlock, DropRef };

// No per-request history or inode map: retained memory must belong to the SDK.
class RegistryFixture : public HttpTransport {
public:
    Failure failure = Failure::None;
    int opens = 0, drops = 0, refs = 0;
    Response Request(const std::string &, const std::string &url, const Headers &headers,
                     std::string_view body, Deadline, size_t) override {
        if (url == "http://fs.test/v1/data-credential")
            return {200, {}, Json({{"scheme", "file"}, {"endpoint", "/tmp"}, {"prefix", "registry-test/"}}).dump()};
        auto op = headers.at("X-Drive9-Extent-Op");
        auto args = Json::parse(body);
        Json result = {{"errno", 0}};
        if (op == "load") result["format"] = detail::EncodeBase64(Json({{"BlockSize", 4194304}}).dump());
        else if (op == "incr_counter") result["value"] = 1;
        else if (op == "new_session") result["session_guard"] = true;
        else if (op == "find_stale_sessions") result["sids"] = Json::array();
        else if (op == "getattr" || op == "lookup") {
            auto ino = op == "getattr" ? args.at("inode").get<uint64_t>() : std::stoull(args.at("name").get<std::string>());
            result["inode"] = ino;
            result["attr"] = {{"Typ", ino == 1 ? 2 : 1}, {"Length", 0}};
            if (op == "getattr" && ino != 1 && (failure == Failure::LockedAttr || failure == Failure::Unlock))
                result["errno"] = 13;
        } else if (op == "open_ref") {
            if (failure == Failure::OpenRef) result["errno"] = 13;
            else { ++opens; ++refs; }
        } else if (op == "delete_sustained") {
            ++drops;
            if (failure == Failure::DropRef) result["errno"] = 13;
            else --refs;
        } else if (op == "setlk") {
            if (failure == Failure::Lock || (failure == Failure::Unlock && args.at("ltype") == int('U')))
                result["errno"] = 13;
        }
        return {200, {}, result.dump()};
    }
    std::shared_ptr<Client> ClientFor() {
        return std::make_shared<Client>(ClientOptions{"http://fs.test", "fixture", 30s,
            std::shared_ptr<HttpTransport>(this, [](HttpTransport *) {})});
    }
};

ExtentOptions Options() {
    ExtentOptions options;
    options.cache_bytes = 0;
    options.heartbeat = 10min; // Keep fault injection on the foreground lifecycle only.
    return options;
}

void RegistryChurn(Failure failure) {
    RegistryFixture f; f.failure = failure;
    ExtentFileSystem fs(f.ClientFor(), Options());
    OpenOptions open;
    if (failure == Failure::Lock || failure == Failure::LockedAttr || failure == Failure::Unlock) open.lock = Lock::Exclusive;
    auto cycle = [&](int ino) {
        uint64_t handle = 0;
        try { handle = fs.Open("/" + std::to_string(ino), open); }
        catch (const Error &error) {
            if (error.code != 13 || failure == Failure::None || failure == Failure::DropRef) throw;
            return;
        }
        Check(failure == Failure::None || failure == Failure::DropRef, "faulted Open unexpectedly succeeded");
        bool rejected = false;
        try { fs.Close(handle); }
        catch (const Error &error) { if (failure != Failure::DropRef || error.code != 13) throw; rejected = true; }
        Check(rejected == (failure == Failure::DropRef), "failed remote release was reported as success");
    };
    for (int i = 100; i < 2100; ++i) cycle(i);
    Check(ExtentTestAccess::NodeCount(fs) == 0, "successful close or failed Open retained inode entries");
    if (failure != Failure::DropRef) Check(f.refs == 0, "failed Open leaked the server open reference");
    Check(f.opens == f.drops, "open and sustained-reference release counts differ");
}

void SharedNodeSurvivesFailedOpen() {
    RegistryFixture f;
    ExtentFileSystem fs(f.ClientFor(), Options());
    auto a = fs.Open("/42"), b = fs.Open("/42");
    Check(f.opens == 1 && f.refs == 1, "same-inode handles did not share an open reference");
    f.failure = Failure::Lock;
    OpenOptions locked; locked.lock = Lock::Exclusive;
    bool rejected = false;
    try { fs.Open("/42", locked); } catch (const Error &error) { rejected = error.code == 13; }
    Check(rejected && f.drops == 0, "failed sibling open retired a live node");
    f.failure = Failure::None;
    fs.Close(a);
    auto c = fs.Open("/42");
    Check(f.opens == 1 && fs.Info(b).inode == 42 && fs.Info(c).inode == 42, "live registry entry was removed too early");
    fs.Close(b); Check(f.drops == 0, "non-final close dropped the shared reference");
    fs.Close(c); Check(f.refs == 0 && f.drops == 1, "final close did not drop the shared reference");
    auto d = fs.Open("/42");
    Check(f.opens == 2, "reopen did not acquire a fresh reference"); fs.Close(d);
}

void ShutdownReclaimsOpenNodes() {
    RegistryFixture f;
    ExtentFileSystem fs(f.ClientFor(), Options());
    for (int i = 100; i < 2100; ++i) fs.Open("/" + std::to_string(i));
    Check(ExtentTestAccess::NodeCount(fs) == 2000, "live inode entries disappeared before shutdown");
    fs.Shutdown();
    Check(ExtentTestAccess::NodeCount(fs) == 0, "shutdown retained open inode entries");
}

int main() {
    try {
        for (auto failure : {Failure::None, Failure::OpenRef, Failure::Lock, Failure::LockedAttr, Failure::Unlock, Failure::DropRef})
            RegistryChurn(failure);
        SharedNodeSurvivesFailedOpen(); ShutdownReclaimsOpenNodes();
        std::cout << "Inode registry reclamation and shared ownership tests passed\n";
    } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
