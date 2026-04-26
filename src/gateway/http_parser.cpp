#include "rpc/gateway/http_parser.h"

// File purpose:
// Implement a strict and bounded HTTP/1.x parser for request line/headers/body.
// Parsing is delegated to Boost.Beast to reduce hand-written HTTP state logic.

#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/system/error_code.hpp>

#include <cctype>
#include <string_view>

namespace rpc::gateway {

namespace {

std::string lower_ascii(std::string_view value) {
    std::string lower;
    lower.reserve(value.size());
    for (char ch : value) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return lower;
}

namespace http = boost::beast::http;

}  // namespace

std::string HttpRequest::header_value(const std::string& key) const {
    const auto it = headers.find(lower_ascii(key));
    if (it == headers.end()) {
        return {};
    }
    return it->second;
}

HttpRequestParser::HttpRequestParser(HttpParserLimits limits) : limits_(limits) {}

HttpParseCode HttpRequestParser::parse(
    const std::string& input,
    std::size_t* consumed_bytes,
    HttpRequest* request
) const {
    if (consumed_bytes == nullptr || request == nullptr) {
        return HttpParseCode::BadRequest;
    }

    *consumed_bytes = 0;

    http::request_parser<http::string_body> parser;
    parser.header_limit(limits_.max_header_bytes);
    parser.body_limit(limits_.max_body_bytes);

    boost::system::error_code ec;
    const std::size_t consumed = parser.put(boost::asio::buffer(input.data(), input.size()), ec);

    if (ec == http::error::need_more) {
        return HttpParseCode::NeedMoreData;
    }
    if (ec == http::error::header_limit) {
        return HttpParseCode::HeaderTooLarge;
    }
    if (ec == http::error::body_limit) {
        return HttpParseCode::BodyTooLarge;
    }
    if (ec) {
        return HttpParseCode::BadRequest;
    }
    if (!parser.is_done()) {
        return HttpParseCode::NeedMoreData;
    }

    http::request<http::string_body> parsed_request = parser.release();
    if (parsed_request.target().empty()) {
        return HttpParseCode::BadRequest;
    }

    // Keep W8 behavior: currently only supports Content-Length framing.
    if (parsed_request.find(http::field::transfer_encoding) != parsed_request.end()) {
        return HttpParseCode::BadRequest;
    }

    HttpRequest parsed;
    parsed.method = std::string(parsed_request.method_string());
    parsed.target = std::string(parsed_request.target());
    parsed.version_major = parsed_request.version() / 10;
    parsed.version_minor = parsed_request.version() % 10;
    parsed.body = std::move(parsed_request.body());
    parsed.keep_alive = parsed_request.keep_alive();

    for (const auto& field : parsed_request.base()) {
        const std::string key = lower_ascii(field.name_string());
        const std::string value = std::string(field.value());

        auto header_it = parsed.headers.find(key);
        if (header_it == parsed.headers.end()) {
            parsed.headers.emplace(key, value);
        } else {
            header_it->second.push_back(',');
            header_it->second.append(value);
        }
    }

    *request = std::move(parsed);
    *consumed_bytes = consumed;
    return HttpParseCode::MessageComplete;
}

}  // namespace rpc::gateway
