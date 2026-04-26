#include "rpc/rpc/krpc_channel.h"

// 文件用途：
// 实现 KrpcChannel：使用 protoc 生成的消息类完成 Protobuf 编解码，
// 并提供同步/异步下游调用封装。

#include <limits>
#include <utility>

#include "krpc.pb.h"

namespace rpc::rpc {

namespace {

KrpcTransportResponse from_net_response(const ::rpc::net::NetCallResponse& response) {
    KrpcTransportResponse result;
    result.code = response.code;
    result.message = response.message;
    result.payload = response.payload;
    result.retryable = response.retryable;
    result.effective_timeout_ms = response.effective_timeout_ms;
    result.cancelled = response.cancelled;
    result.cancel_reason = response.cancel_reason;
    return result;
}

}  // namespace

KrpcChannel::KrpcChannel(NetInvoker invoker) : invoker_(std::move(invoker)) {
    if (!invoker_) {
        invoker_ = ::rpc::net::invoke_tcp;
    }
}

KrpcTransportResponse KrpcChannel::invoke(
    const KrpcTransportRequest& transport,
    const KrpcRequestFrame& request,
    KrpcCodec codec
) const {
    return invoke_with_invoker(invoker_, transport, request, codec);
}

std::future<KrpcTransportResponse> KrpcChannel::invoke_async(
    KrpcTransportRequest transport,
    KrpcRequestFrame request,
    KrpcCodec codec
) const {
    const NetInvoker invoker = invoker_;
    return std::async(
        std::launch::async,
        [invoker, transport = std::move(transport), request = std::move(request), codec]() mutable {
            return invoke_with_invoker(invoker, transport, request, codec);
        }
    );
}

bool KrpcChannel::encode_request_protobuf(const KrpcRequestFrame& request, std::string* encoded) {
    if (encoded == nullptr) {
        return false;
    }

    thread_local ::rpc::krpc::KrpcRequest message;
    message.Clear();
    message.set_service(request.service);
    message.set_method(request.method);
    message.set_payload(request.payload);
    message.set_timeout_ms(request.timeout_ms);
    message.set_max_retries(static_cast<std::uint64_t>(request.max_retries));

    auto* metadata = message.mutable_metadata();
    if (metadata == nullptr) {
        return false;
    }
    for (const auto& kv : request.metadata) {
        (*metadata)[kv.first] = kv.second;
    }

    const std::size_t size = static_cast<std::size_t>(message.ByteSizeLong());
    encoded->resize(size);
    if (size == 0) {
        return true;
    }
    if (size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return false;
    }
    return message.SerializeToArray(encoded->data(), static_cast<int>(size));
}

bool KrpcChannel::decode_request_protobuf(const std::string& encoded, KrpcRequestFrame* request) {
    if (request == nullptr) {
        return false;
    }

    ::rpc::krpc::KrpcRequest message;
    if (!message.ParseFromString(encoded)) {
        return false;
    }

    KrpcRequestFrame result;
    result.service = message.service();
    result.method = message.method();
    result.payload = message.payload();
    result.timeout_ms = message.timeout_ms();

    const std::uint64_t max_retries = message.max_retries();
    if (max_retries > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        return false;
    }
    result.max_retries = static_cast<std::size_t>(max_retries);

    result.metadata.reserve(static_cast<std::size_t>(message.metadata_size()));
    for (const auto& kv : message.metadata()) {
        if (!kv.first.empty()) {
            result.metadata.emplace(kv.first, kv.second);
        }
    }

    *request = std::move(result);
    return true;
}

bool KrpcChannel::encode_response_protobuf(const KrpcResponseFrame& response, std::string* encoded) {
    if (encoded == nullptr) {
        return false;
    }

    thread_local ::rpc::krpc::KrpcResponse message;
    message.Clear();
    message.set_code(response.code);
    message.set_message(response.message);
    message.set_payload(response.payload);
    message.set_retryable(response.retryable);
    message.set_effective_timeout_ms(response.effective_timeout_ms);
    message.set_cancelled(response.cancelled);
    message.set_cancel_reason(response.cancel_reason);

    const std::size_t size = static_cast<std::size_t>(message.ByteSizeLong());
    encoded->resize(size);
    if (size == 0) {
        return true;
    }
    if (size > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return false;
    }
    return message.SerializeToArray(encoded->data(), static_cast<int>(size));
}

bool KrpcChannel::decode_response_protobuf(const std::string& encoded, KrpcResponseFrame* response) {
    if (response == nullptr) {
        return false;
    }

    ::rpc::krpc::KrpcResponse message;
    if (!message.ParseFromString(encoded)) {
        return false;
    }

    KrpcResponseFrame result;
    result.code = message.code();
    result.message = message.message();
    result.payload = message.payload();
    result.retryable = message.retryable();
    result.effective_timeout_ms = message.effective_timeout_ms();
    result.cancelled = message.cancelled();
    result.cancel_reason = message.cancel_reason();

    *response = std::move(result);
    return true;
}

KrpcTransportResponse KrpcChannel::invoke_with_invoker(
    const NetInvoker& invoker,
    const KrpcTransportRequest& transport,
    const KrpcRequestFrame& request,
    KrpcCodec codec
) {
    ::rpc::net::NetCallRequest net_request;
    net_request.endpoint = transport.endpoint;
    net_request.downstream_timeout_ms = transport.downstream_timeout_ms;
    net_request.attempt = transport.attempt;

    if (codec == KrpcCodec::Protobuf) {
        if (!encode_request_protobuf(request, &net_request.payload)) {
            return KrpcTransportResponse{
                400,
                "krpc_protobuf_encode_failed",
                {},
                false,
                0,
                false,
                {},
            };
        }
    } else {
        net_request.payload = request.payload;
    }

    const ::rpc::net::NetCallResponse net_response = invoker(net_request);
    KrpcTransportResponse channel_response = from_net_response(net_response);
    if (codec != KrpcCodec::Protobuf || net_response.code != 0) {
        return channel_response;
    }

    KrpcResponseFrame decoded;
    if (!decode_response_protobuf(net_response.payload, &decoded)) {
        return KrpcTransportResponse{
            502,
            "krpc_protobuf_decode_failed",
            {},
            false,
            net_response.effective_timeout_ms,
            false,
            {},
        };
    }

    channel_response.code = decoded.code;
    channel_response.message = decoded.message;
    channel_response.payload = decoded.payload;
    channel_response.retryable = decoded.retryable;
    channel_response.effective_timeout_ms = decoded.effective_timeout_ms == 0
        ? net_response.effective_timeout_ms
        : decoded.effective_timeout_ms;
    channel_response.cancelled = decoded.cancelled;
    channel_response.cancel_reason = decoded.cancel_reason;
    return channel_response;
}

}  // namespace rpc::rpc
