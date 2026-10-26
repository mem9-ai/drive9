#include "internal.hpp"
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <algorithm>
#include <array>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>

namespace drive9::detail {
std::string EncodeBase64(std::string_view data) {
    if (data.size() > size_t(INT_MAX)) throw Error("Base64 input too large");
    std::string out(4 * ((data.size() + 2) / 3), '\0');
    if (!data.empty()) EVP_EncodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                                     reinterpret_cast<const unsigned char *>(data.data()), int(data.size()));
    return out;
}
std::string DecodeBase64(const Json &value) {
    if (value.is_null()) return {};
    const auto &text = value.get_ref<const std::string &>();
    if (text.empty()) return {};
    if (text.size() % 4 || text.size() > size_t(INT_MAX)) throw Error("Invalid base64 wire value");
    std::string out((text.size() / 4) * 3, '\0');
    int n = EVP_DecodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                            reinterpret_cast<const unsigned char *>(text.data()), int(text.size()));
    if (n < 0) throw Error("Invalid base64 wire value");
    if (text.back() == '=') --n;
    if (text.size() > 1 && text[text.size() - 2] == '=') --n;
    out.resize(n);
    if (EncodeBase64(out) != text) throw Error("Non-canonical base64 wire value");
    return out;
}
std::string Timestamp() {
    auto now = std::chrono::system_clock::now();
    auto seconds = std::chrono::system_clock::to_time_t(now);
    std::tm utc{};
    gmtime_r(&seconds, &utc);
    char text[32];
    std::strftime(text, sizeof(text), "%Y-%m-%dT%H:%M:%SZ", &utc);
    return text;
}
namespace {
std::string Hex(std::string_view bytes) {
    const char alphabet[] = "0123456789abcdef";
    std::string out;
    for (unsigned char c : bytes) { out += alphabet[c >> 4]; out += alphabet[c & 15]; }
    return out;
}
std::string SHA256(std::string_view data) {
    unsigned char digest[EVP_MAX_MD_SIZE]; unsigned int length = 0;
    if (EVP_Digest(data.data(), data.size(), digest, &length, EVP_sha256(), nullptr) != 1) throw Error("SHA256 failed");
    return Hex(std::string_view(reinterpret_cast<char *>(digest), length));
}
std::string HMAC(std::string_view key, std::string_view message) {
    unsigned char digest[EVP_MAX_MD_SIZE]; unsigned int length = 0;
    if (!::HMAC(EVP_sha256(), key.data(), int(key.size()), reinterpret_cast<const unsigned char *>(message.data()),
                message.size(), digest, &length)) throw Error("HMAC failed");
    return std::string(reinterpret_cast<char *>(digest), length);
}
int64_t Expiry(const std::string &value) {
    if (value.empty()) return INT64_MAX;
    std::tm utc{};
    std::istringstream stream(value);
    stream >> std::get_time(&utc, "%Y-%m-%dT%H:%M:%S");
    auto end = value.find('Z', 19);
    if (stream.fail() || end == std::string::npos || end + 1 != value.size() ||
        (value.size() > 20 && value[19] != '.')) throw Error("Invalid credential expiry");
    return timegm(&utc);
}
std::string Identity(const Json &c) {
    return Json::array({c.value("scheme", ""), c.value("endpoint", ""), c.value("bucket", ""),
                        c.value("prefix", ""), c.value("tenant_id", ""), c.value("force_path_style", false),
                        c.value("region", ""), c.value("encryption_mode", ""), c.value("encryption_key_id", "")}).dump();
}
void Validate(const Json &c) {
    auto scheme = c.value("scheme", "");
    auto endpoint = c.value("endpoint", ""), prefix = c.value("prefix", "");
    if (prefix.empty() || prefix.front() == '/' || prefix.find("..") != std::string::npos ||
        prefix.find_first_of("\\\r\n") != std::string::npos) throw Error("Invalid extent storage prefix");
    if (scheme == "file") {
        if (endpoint.empty() || endpoint.front() != '/') throw Error("File storage requires an absolute root");
        return;
    }
    if (scheme != "s3" && scheme != "gcs" && scheme != "gs") throw Error("Unsupported extent object storage scheme");
    if ((endpoint.compare(0, 7, "http://") && endpoint.compare(0, 8, "https://")) ||
        endpoint.find_first_of("@?#\\\r\n\t ") != std::string::npos) throw Error("Invalid object storage endpoint");
    if (c.value("bucket", "").empty()) throw Error("Object storage bucket is missing");
    if (scheme == "s3") {
        if (c.value("access_key_id", "").empty() || c.value("secret_access_key", "").empty())
            throw Error("S3 credential is incomplete");
    } else if (endpoint.size() < 12 || endpoint.substr(endpoint.size() - 12) != "/storage/v1/" ||
               c.value("access_token", "").empty()) throw Error("GCS requires a JSON API base and scoped OAuth token");
}
} // namespace
ObjectStore::ObjectStore(std::shared_ptr<Client> client) : client_(std::move(client)) {
    Credential(false, std::chrono::steady_clock::now() + std::chrono::seconds(30));
}
Json ObjectStore::Credential(bool force, Deadline deadline) {
    std::lock_guard<std::mutex> guard(mu_);
    if (!force && std::chrono::steady_clock::now() < refresh_) return credential_;
    auto credential = client_->DataCredential(deadline);
    Validate(credential);
    auto identity = Identity(credential);
    if (!identity_.empty() && identity != identity_) throw Error("Extent storage binding changed during credential refresh");
    auto expiry = Expiry(credential.value("expires_at", ""));
    auto wall = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    if (expiry <= wall) throw Error("Object storage credential expired");
    // Refresh early, using a per-instance stagger to avoid a fleet mint burst.
    auto lead = 120 + (reinterpret_cast<uintptr_t>(this) % 61);
    refresh_ = expiry == INT64_MAX ? Deadline::max() : std::chrono::steady_clock::now() + std::chrono::seconds(std::max<int64_t>(0, expiry - wall - lead));
    identity_ = std::move(identity);
    credential_ = std::move(credential);
    return credential_;
}
Response ObjectStore::Request(const std::string &method, const std::string &key, std::string_view data,
                               size_t max_response, Deadline deadline) {
    if (key.empty() || key.front() == '/' || key.find("..") != std::string::npos || key.find('\\') != std::string::npos)
        throw Error("Invalid immutable object key");
    for (int attempt = 0; attempt < 3; ++attempt) {
        auto credential = Credential(attempt > 0, deadline);
        auto scheme = credential.at("scheme").get<std::string>();
        auto object = credential.at("prefix").get<std::string>() + key;
        auto endpoint = credential.at("endpoint").get<std::string>();
        if (scheme == "file") {
            auto path = std::filesystem::path(endpoint) / object;
            Response response; response.status = 200;
            if (method == "PUT") {
                std::filesystem::create_directories(path.parent_path());
                // Objects have unique slice IDs; publication follows this close.
                auto temporary = path.string() + ".cpp-pending";
                int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
                if (fd < 0) throw Error("Local object create failed");
                size_t done = 0;
                while (done < data.size()) {
                    auto n = ::write(fd, data.data() + done, data.size() - done);
                    if (n < 0 && errno == EINTR) continue;
                    if (n <= 0) { ::close(fd); throw Error("Local object write failed"); }
                    done += size_t(n);
                }
                auto synced = ::fsync(fd); auto closed = ::close(fd);
                if (synced || closed) throw Error("Local object sync failed");
                std::filesystem::rename(temporary, path);
                fd = ::open(path.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
                if (fd < 0) throw Error("Local object directory open failed");
                synced = ::fsync(fd); closed = ::close(fd);
                if (synced || closed) throw Error("Local object directory sync failed");
            } else {
                std::ifstream input(path, std::ios::binary);
                if (!input) throw Error("Immutable object is missing", 2, 404);
                response.body.resize(max_response);
                input.read(response.body.data(), max_response);
                auto n = input.gcount();
                if (input.bad() || (size_t(n) == max_response && input.peek() != EOF)) throw Error("Invalid immutable object length");
                response.body.resize(n);
            }
            return response;
        }
        Headers headers;
        std::string url, http_method = method;
        if (scheme == "s3") {
            while (endpoint.back() == '/') endpoint.pop_back();
            auto start = endpoint.find("://") + 3, split = endpoint.find('/', start);
            auto host = endpoint.substr(start, split - start);
            auto base = split == std::string::npos ? std::string() : endpoint.substr(split);
            auto bucket = credential.at("bucket").get<std::string>();
            bool path_style = credential.value("force_path_style", true);
            if (!path_style) host = bucket + "." + host;
            auto path = UrlEncode(base + "/" + (path_style ? bucket + "/" : "") + object, true);
            url = endpoint.substr(0, start) + host + path;
            auto timestamp = Timestamp();
            auto date = timestamp.substr(0, 4) + timestamp.substr(5, 2) + timestamp.substr(8, 2);
            auto time = date + "T" + timestamp.substr(11, 2) + timestamp.substr(14, 2) + timestamp.substr(17, 2) + "Z";
            headers["host"] = host;
            headers["x-amz-content-sha256"] = SHA256(data);
            headers["x-amz-date"] = time;
            auto token = credential.value("session_token", "");
            if (!token.empty()) headers["x-amz-security-token"] = token;
            std::string canonical, signed_headers;
            for (auto &h : headers) { canonical += h.first + ":" + h.second + "\n"; signed_headers += (signed_headers.empty() ? "" : ";") + h.first; }
            auto region = credential.value("region", "us-east-1");
            if (region.empty()) region = "us-east-1";
            auto scope = date + "/" + region + "/s3/aws4_request";
            auto request = method + "\n" + path + "\n\n" + canonical + "\n" + signed_headers + "\n" + headers.at("x-amz-content-sha256");
            auto signing = HMAC(HMAC(HMAC(HMAC("AWS4" + credential.at("secret_access_key").get<std::string>(), date), region), "s3"), "aws4_request");
            auto signature = Hex(HMAC(signing, "AWS4-HMAC-SHA256\n" + time + "\n" + scope + "\n" + SHA256(request)));
            headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + credential.at("access_key_id").get<std::string>() + "/" + scope + ", SignedHeaders=" + signed_headers + ", Signature=" + signature;
        } else {
            headers["Authorization"] = "Bearer " + credential.at("access_token").get<std::string>();
            auto bucket = UrlEncode(credential.at("bucket").get<std::string>());
            if (method == "GET") url = endpoint + "b/" + bucket + "/o/" + UrlEncode(object) + "?alt=media";
            else {
                // JSON API media upload, not a filesystem-specific protocol.
                http_method = "POST";
                endpoint.replace(endpoint.size() - 12, 12, "/upload/storage/v1/");
                url = endpoint + "b/" + bucket + "/o?name=" + UrlEncode(object) + "&uploadType=media";
            }
        }
        headers["Content-Type"] = "application/octet-stream";
        try {
            auto response = client_->Transport()->Request(http_method, url, headers, data, deadline, max_response);
            if (response.status >= 200 && response.status < 300) return response;
            if (response.status != 401 && response.status != 403 && response.status != 408 && response.status != 429 && response.status < 500)
                throw Error("Object storage request rejected", 0, response.status);
            if (attempt == 2) throw Error("Object storage request rejected", 0, response.status);
        } catch (const Error &error) {
            if (error.status != 0 || attempt == 2 || std::chrono::steady_clock::now() >= deadline) throw;
        }
        // Only immutable object PUT/GET is retried. Metadata publication is not.
    }
    throw Error("Object storage retry exhausted");
}
std::string ObjectStore::Get(const std::string &key, size_t size, Deadline deadline) {
    auto response = Request("GET", key, {}, size, deadline);
    if (response.body.size() != size) throw Error("Short immutable object read");
    return std::move(response.body);
}
void ObjectStore::Put(const std::string &key, std::string_view data, Deadline deadline) {
    Request("PUT", key, data, 1 << 20, deadline);
}
} // namespace drive9::detail
