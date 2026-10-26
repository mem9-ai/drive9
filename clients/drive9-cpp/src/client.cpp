#include "drive9/client.hpp"
#include <utf8proc.h>
#include <algorithm>
#include <cstdlib>

namespace drive9 {
std::string UrlEncode(std::string_view input, bool preserve_slashes) {
    const char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(input.size());
    for (unsigned char c : input) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.' || c == '~' || (preserve_slashes && c == '/')) out += char(c);
        else { out += '%'; out += hex[c >> 4]; out += hex[c & 15]; }
    }
    return out;
}
std::string CanonicalPath(const std::string &input) {
    if (input.empty() || input.front() != '/' || input.find('\\') != std::string::npos || input.find('\0') != std::string::npos)
        throw Error("Invalid absolute filesystem path", 22);
    for (unsigned char c : input) {
        if (c < 32 && c != '\n' && c != '\r' && c != '\t') throw Error("Invalid control byte in filesystem path", 22);
    }
    utf8proc_uint8_t *normalized = nullptr;
    auto size = utf8proc_map(reinterpret_cast<const utf8proc_uint8_t *>(input.data()), input.size(), &normalized,
                            utf8proc_option_t(UTF8PROC_STABLE | UTF8PROC_COMPOSE));
    if (size < 0) throw Error("Invalid UTF-8 filesystem path", 22);
    std::unique_ptr<utf8proc_uint8_t, decltype(&std::free)> owned(normalized, std::free);
    std::string path(reinterpret_cast<char *>(normalized), size), out;
    for (size_t start = 1; start <= path.size();) {
        auto end = path.find('/', start);
        if (end == std::string::npos) end = path.size();
        auto part = path.substr(start, end - start);
        if (part == ".." || part == ".") throw Error("Relative path components are forbidden", 22);
        if (!part.empty()) out += "/" + part;
        start = end + 1;
    }
    return out.empty() ? "/" : out;
}
Client::Client(ClientOptions options) : options_(std::move(options)) {
    while (!options_.endpoint.empty() && options_.endpoint.back() == '/') options_.endpoint.pop_back();
    if (options_.endpoint.compare(0, 7, "http://") && options_.endpoint.compare(0, 8, "https://"))
        throw Error("Client endpoint requires http or https");
    auto authority = options_.endpoint.substr(options_.endpoint.find("://") + 3);
    if (authority.empty() || authority.find_first_of("@?#\\ \r\n\t") != std::string::npos ||
        options_.token.find_first_of("\r\n") != std::string::npos || options_.timeout.count() <= 0)
        throw Error("Invalid client configuration");
    if (!options_.transport) options_.transport = MakeHttpTransport();
}
Response Client::Request(const std::string &method, const std::string &path, const Headers &headers,
                         std::string_view body, Deadline deadline) const {
    if (path.empty() || path.front() != '/' || path.find_first_of("\r\n#") != std::string::npos)
        throw Error("Invalid API path");
    auto all = headers;
    // The control-plane bearer is only ever sent to the configured endpoint.
    all["Authorization"] = "Bearer " + options_.token;
    deadline = std::min(deadline, std::chrono::steady_clock::now() + options_.timeout);
    auto response = options_.transport->Request(method, options_.endpoint + path, all, body, deadline);
    if (response.status < 200 || response.status >= 300)
        throw Error("Filesystem HTTP request rejected", 0, response.status);
    return response;
}
Json Client::JsonRequest(const std::string &method, const std::string &path, const Json &body, Deadline deadline) const {
    auto response = Request(method, path, {{"Content-Type", "application/json"}}, body.dump(), deadline);
    try { return Json::parse(response.body); }
    catch (const Json::exception &) { throw Error("Invalid filesystem JSON acknowledgement"); }
}
Json Client::ExtentMeta(const std::string &op, const Json &body, Deadline deadline) const {
    if (op.empty() || op.find_first_not_of("abcdefghijklmnopqrstuvwxyz_") != std::string::npos)
        throw Error("Invalid extent operation");
    auto response = Request("POST", "/v1/extent/meta", {{"Content-Type", "application/json"}, {"X-Drive9-Extent-Op", op}},
                            body.dump(), deadline);
    try {
        auto result = Json::parse(response.body);
        if (!result.is_object() || !result.contains("errno") || !result.at("errno").is_number_integer())
            throw Error("Invalid extent metadata acknowledgement");
        return result;
    } catch (const Json::exception &) { throw Error("Invalid extent metadata acknowledgement"); }
}
Json Client::DataCredential(Deadline deadline) const { return JsonRequest("POST", "/v1/data-credential", Json::object(), deadline); }
Response Client::ReadFile(const std::string &path, const Headers &conditions) const {
    return Request("GET", "/v1/fs" + UrlEncode(CanonicalPath(path), true), conditions);
}
Response Client::WriteFile(const std::string &path, std::string_view data, const Headers &conditions) const {
    auto headers = conditions;
    headers["Content-Type"] = "application/octet-stream";
    return Request("PUT", "/v1/fs" + UrlEncode(CanonicalPath(path), true), headers, data);
}
std::shared_ptr<HttpTransport> Client::Transport() const { return options_.transport; }
} // namespace drive9
