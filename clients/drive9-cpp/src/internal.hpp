#pragma once
#include "drive9/client.hpp"
#include <mutex>

namespace drive9::detail {
std::string DecodeBase64(const Json &value);
std::string EncodeBase64(std::string_view value);
std::string Timestamp();
Json NormalizeCredential(Json credential);
struct Slice {
    uint32_t pos, size, off, length;
    uint64_t id;
};
// Normalize append-ordered overlays to sorted disjoint intervals. Zero-id
// slices and uncovered intervals are holes, including truncate/regrow gaps.
std::vector<Slice> DecodeSlices(const Json &value);
class ObjectStore {
public:
    explicit ObjectStore(std::shared_ptr<Client> client);
    std::string Get(const std::string &key, size_t size, Deadline deadline);
    void Put(const std::string &key, std::string_view data, Deadline deadline);
private:
    std::shared_ptr<Client> client_;
    std::mutex mu_;
    Json credential_;
    std::string identity_;
    Deadline refresh_ = Deadline::min();
    Json Credential(bool force, Deadline deadline);
    Response Request(const std::string &method, const std::string &key, std::string_view data,
                     size_t max_response, Deadline deadline);
};
} // namespace drive9::detail
