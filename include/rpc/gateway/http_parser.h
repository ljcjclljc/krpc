#pragma once

// File purpose:
// Define an HTTP/1.x request parser used by gateway/net reactor path.

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

namespace rpc::gateway {
// http的请求解析器，支持http1.0和http1.1，暂不支持chunked编码
struct HttpParserLimits {
    std::size_t max_header_bytes{8 * 1024};
    std::size_t max_body_bytes{1024 * 1024};
};
// http解析结果
enum class HttpParseCode : std::uint8_t {
    NeedMoreData = 0,
    MessageComplete = 1,
    BadRequest = 2,
    HeaderTooLarge = 3,
    BodyTooLarge = 4,
};
// http请求结构体
struct HttpRequest {
    std::string method;
    std::string target;
    int version_major{1};
    int version_minor{1};
    std::unordered_map<std::string, std::string> headers;
    std::string body;
    bool keep_alive{true};

    std::string header_value(const std::string& key) const;
};
// http请求解析器
class HttpRequestParser {
public:
    explicit HttpRequestParser(HttpParserLimits limits = {});

    HttpParseCode parse(
        const std::string& input,
        std::size_t* consumed_bytes,
        HttpRequest* request
    ) const;

private:
    HttpParserLimits limits_{};
};

}  // namespace rpc::gateway
