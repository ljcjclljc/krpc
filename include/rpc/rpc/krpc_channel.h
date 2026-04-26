#pragma once

// 文件用途：
// 定义 KrpcChannel：提供 Protobuf 二进制编解码，以及同步/异步调用入口。

#include <cstddef>
#include <cstdint>
#include <future>
#include <functional>
#include <string>
#include <unordered_map>

#include "rpc/net/net.h"

namespace rpc::rpc {

// Krpc 请求帧（逻辑层模型）。
struct KrpcRequestFrame {
    std::string service;
    std::string method;
    std::string payload;
    std::unordered_map<std::string, std::string> metadata;
    std::uint64_t timeout_ms{0};
    std::size_t max_retries{0};
};

// Krpc 响应帧（逻辑层模型）。
struct KrpcResponseFrame {
    int code{0};
    std::string message;
    std::string payload;
    bool retryable{false};
    std::uint64_t effective_timeout_ms{0};
    bool cancelled{false};
    std::string cancel_reason;
};

// KrpcChannel 对下游调用请求封装。
struct KrpcTransportRequest {
    std::string endpoint;
    std::uint64_t downstream_timeout_ms{0};
    std::size_t attempt{1};
};

// KrpcChannel 对下游调用返回封装。
struct KrpcTransportResponse {
    int code{0};
    std::string message;
    std::string payload;
    bool retryable{false};
    std::uint64_t effective_timeout_ms{0};
    bool cancelled{false};
    std::string cancel_reason;
};

enum class KrpcCodec : std::uint8_t {
    Raw = 0,
    Protobuf = 1,
};

class KrpcChannel {
public:
    using NetInvoker = std::function<::rpc::net::NetCallResponse(const ::rpc::net::NetCallRequest&)>;

    explicit KrpcChannel(NetInvoker invoker = ::rpc::net::invoke_tcp);

    KrpcTransportResponse invoke(
        const KrpcTransportRequest& transport,
        const KrpcRequestFrame& request,
        KrpcCodec codec
    ) const;

    std::future<KrpcTransportResponse> invoke_async(
        KrpcTransportRequest transport,
        KrpcRequestFrame request,
        KrpcCodec codec
    ) const;

    static bool encode_request_protobuf(const KrpcRequestFrame& request, std::string* encoded);
    static bool decode_request_protobuf(const std::string& encoded, KrpcRequestFrame* request);
    static bool encode_response_protobuf(const KrpcResponseFrame& response, std::string* encoded);
    static bool decode_response_protobuf(const std::string& encoded, KrpcResponseFrame* response);

private:
    static KrpcTransportResponse invoke_with_invoker(
        const NetInvoker& invoker,
        const KrpcTransportRequest& transport,
        const KrpcRequestFrame& request,
        KrpcCodec codec
    );

    NetInvoker invoker_;
};

}  // namespace rpc::rpc
