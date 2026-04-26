#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "rpc/gateway/http_gateway.h"
#include "rpc/rpc/client.h"

// 文件用途：
// 验证 W9 网关能力：
// 1) Router/Servlet 路由稳定命中（含 URI 规范化）
// 2) RPC -> HTTP 错误码映射语义一致（4xx/5xx）
// 3) API Key/JWT 未授权请求可拦截并产生日志审计计数

namespace {

class MockRpcClient final : public rpc::client::IRpcClient {
public:
    rpc::client::RpcResponse invoke(const rpc::client::RpcRequest& request) override {
        ++invoke_count_;
        last_request_ = request;

        if (request.payload == "bad-request") {
            return rpc::client::RpcResponse{400, "bad_request", {}, 1, request.timeout_ms};
        }
        if (request.payload == "too-many") {
            return rpc::client::RpcResponse{429, "too_many_requests", {}, 1, request.timeout_ms};
        }
        if (request.payload == "unavailable") {
            return rpc::client::RpcResponse{503, "service_unavailable", {}, 1, request.timeout_ms};
        }
        if (request.payload == "deadline") {
            return rpc::client::RpcResponse{504, "deadline_exceeded", {}, 1, request.timeout_ms};
        }
        if (request.payload == "weird-5xx") {
            return rpc::client::RpcResponse{599, "mystery_failure", {}, 1, request.timeout_ms};
        }

        return rpc::client::RpcResponse{
            0,
            "ok",
            "mock:" + request.payload,
            1,
            request.timeout_ms,
        };
    }

    std::size_t invoke_count() const noexcept {
        return invoke_count_;
    }

    const rpc::client::RpcRequest& last_request() const noexcept {
        return last_request_;
    }

private:
    std::size_t invoke_count_{0};
    rpc::client::RpcRequest last_request_{};
};

std::string base64url_encode(const std::string& input) {
    static constexpr char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    std::string encoded;
    encoded.reserve(((input.size() + 2) / 3) * 4);

    std::size_t i = 0;
    while (i + 2 < input.size()) {
        const std::uint32_t chunk = (static_cast<std::uint32_t>(static_cast<unsigned char>(input[i])) << 16U)
            | (static_cast<std::uint32_t>(static_cast<unsigned char>(input[i + 1])) << 8U)
            | static_cast<std::uint32_t>(static_cast<unsigned char>(input[i + 2]));
        encoded.push_back(kAlphabet[(chunk >> 18U) & 0x3FU]);
        encoded.push_back(kAlphabet[(chunk >> 12U) & 0x3FU]);
        encoded.push_back(kAlphabet[(chunk >> 6U) & 0x3FU]);
        encoded.push_back(kAlphabet[chunk & 0x3FU]);
        i += 3;
    }

    const std::size_t remain = input.size() - i;
    if (remain == 1) {
        const std::uint32_t chunk = static_cast<std::uint32_t>(
            static_cast<unsigned char>(input[i])) << 16U;
        encoded.push_back(kAlphabet[(chunk >> 18U) & 0x3FU]);
        encoded.push_back(kAlphabet[(chunk >> 12U) & 0x3FU]);
        encoded.push_back('=');
        encoded.push_back('=');
    } else if (remain == 2) {
        const std::uint32_t chunk = (static_cast<std::uint32_t>(
                                         static_cast<unsigned char>(input[i])) << 16U)
            | (static_cast<std::uint32_t>(static_cast<unsigned char>(input[i + 1])) << 8U);
        encoded.push_back(kAlphabet[(chunk >> 18U) & 0x3FU]);
        encoded.push_back(kAlphabet[(chunk >> 12U) & 0x3FU]);
        encoded.push_back(kAlphabet[(chunk >> 6U) & 0x3FU]);
        encoded.push_back('=');
    }

    for (char& ch : encoded) {
        if (ch == '+') {
            ch = '-';
        } else if (ch == '/') {
            ch = '_';
        }
    }
    while (!encoded.empty() && encoded.back() == '=') {
        encoded.pop_back();
    }
    return encoded;
}

std::string build_jwt(
    const std::string& issuer,
    const std::string& audience,
    std::int64_t exp_epoch_seconds
) {
    const std::string header = R"({"alg":"none","typ":"JWT"})";
    const std::string payload = std::string("{\"iss\":\"") + issuer
        + "\",\"aud\":\"" + audience
        + "\",\"exp\":" + std::to_string(exp_epoch_seconds) + "}";
    return base64url_encode(header) + "." + base64url_encode(payload) + ".signature";
}

rpc::gateway::GatewayHttpRequest make_rpc_request(
    std::string target,
    std::string payload,
    std::unordered_map<std::string, std::string> headers = {}
) {
    rpc::gateway::GatewayHttpRequest request;
    request.method = "POST";
    request.target = std::move(target);
    request.body = std::move(payload);
    request.headers = std::move(headers);
    request.headers["x-rpc-service"] = "svc.echo";
    request.headers["x-rpc-method"] = "Echo";
    request.headers["x-rpc-timeout-ms"] = "180";
    return request;
}

}  // namespace

int main() {
    rpc::gateway::HttpGatewayOptions options;
    options.upstream_timeout = std::chrono::milliseconds(250);
    options.auth.enabled = true;
    options.auth.required_api_key = "w9-api-key";
    options.auth.jwt_issuer = "rpc-gateway";
    options.auth.jwt_audience = "rpc-client";
    options.auth.jwt_clock_skew = std::chrono::seconds(0);

    auto mock_client = std::make_shared<MockRpcClient>();
    rpc::gateway::HttpGateway gateway(options, mock_client);

    // 1) Router 稳定命中：query、尾斜杠和 method 大小写不影响 /rpc/invoke 命中。
    auto route_req_a = make_rpc_request(
        "/rpc/invoke?trace=1",
        "hello-route-a",
        {{"x-api-key", "w9-api-key"}}
    );
    route_req_a.method = "post";
    const rpc::gateway::GatewayHttpResponse route_rsp_a = gateway.handle(route_req_a);
    if (route_rsp_a.status != 200 || route_rsp_a.body != "mock:hello-route-a") {
        std::cerr << "route match A failed, status=" << route_rsp_a.status
                  << ", body=" << route_rsp_a.body << '\n';
        return 1;
    }

    const rpc::gateway::GatewayHttpResponse route_rsp_b = gateway.handle(
        make_rpc_request("/rpc/invoke/", "hello-route-b", {{"x-api-key", "w9-api-key"}})
    );
    if (route_rsp_b.status != 200 || route_rsp_b.body != "mock:hello-route-b") {
        std::cerr << "route match B failed, status=" << route_rsp_b.status
                  << ", body=" << route_rsp_b.body << '\n';
        return 1;
    }

    if (mock_client->invoke_count() != 2) {
        std::cerr << "unexpected invoke count after route checks, count="
                  << mock_client->invoke_count() << '\n';
        return 1;
    }

    if (mock_client->last_request().service != "svc.echo"
        || mock_client->last_request().method != "Echo") {
        std::cerr << "unexpected rpc request projection, service="
                  << mock_client->last_request().service
                  << ", method=" << mock_client->last_request().method << '\n';
        return 1;
    }

    // 2) 错误码映射：4xx/5xx 语义保持一致，未知 5xx 收敛到 502。
    const auto check_status = [&gateway](const std::string& payload, int expected_status) -> bool {
        const rpc::gateway::GatewayHttpResponse response = gateway.handle(
            make_rpc_request("/rpc/invoke", payload, {{"x-api-key", "w9-api-key"}})
        );
        if (response.status != expected_status) {
            std::cerr << "status mapping mismatch, payload=" << payload
                      << ", expected=" << expected_status
                      << ", actual=" << response.status << '\n';
            return false;
        }
        return true;
    };

    if (!check_status("bad-request", 400)
        || !check_status("too-many", 429)
        || !check_status("unavailable", 503)
        || !check_status("deadline", 504)
        || !check_status("weird-5xx", 502)) {
        return 1;
    }

    const rpc::gateway::GatewayHttpResponse not_found_rsp = gateway.handle(
        make_rpc_request("/unknown-route", "x", {{"x-api-key", "w9-api-key"}})
    );
    if (not_found_rsp.status != 404) {
        std::cerr << "expected 404 for unknown route, got " << not_found_rsp.status << '\n';
        return 1;
    }

    // 3) 未授权拦截与审计：无凭证/无效 API Key/无效 JWT 均应拦截并计数。
    const rpc::gateway::GatewayHttpResponse missing_cred_rsp = gateway.handle(
        make_rpc_request("/rpc/invoke", "unauthorized-0")
    );
    if (missing_cred_rsp.status != 401) {
        std::cerr << "expected 401 for missing credential, got " << missing_cred_rsp.status << '\n';
        return 1;
    }

    const rpc::gateway::GatewayHttpResponse bad_key_rsp = gateway.handle(
        make_rpc_request("/rpc/invoke", "unauthorized-1", {{"x-api-key", "wrong-key"}})
    );
    if (bad_key_rsp.status != 401) {
        std::cerr << "expected 401 for invalid api key, got " << bad_key_rsp.status << '\n';
        return 1;
    }

    const rpc::gateway::GatewayHttpResponse bad_jwt_rsp = gateway.handle(
        make_rpc_request("/rpc/invoke", "unauthorized-2", {{"authorization", "Bearer broken.token"}})
    );
    if (bad_jwt_rsp.status != 401) {
        std::cerr << "expected 401 for invalid jwt, got " << bad_jwt_rsp.status << '\n';
        return 1;
    }

    const auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    const std::string good_jwt = build_jwt("rpc-gateway", "rpc-client", now_seconds + 3600);
    const rpc::gateway::GatewayHttpResponse good_jwt_rsp = gateway.handle(
        make_rpc_request("/rpc/invoke", "jwt-ok", {{"authorization", "Bearer " + good_jwt}})
    );
    if (good_jwt_rsp.status != 200 || good_jwt_rsp.body != "mock:jwt-ok") {
        std::cerr << "expected jwt request pass, status=" << good_jwt_rsp.status
                  << ", body=" << good_jwt_rsp.body << '\n';
        return 1;
    }

    const rpc::gateway::AuthAuditSnapshot audit = gateway.auth_audit_snapshot();
    if (audit.unauthorized_total < 3 || audit.api_key_rejected < 1 || audit.jwt_rejected < 1
        || audit.last_reject_reason.empty()) {
        std::cerr << "audit snapshot unexpected"
                  << ", unauthorized_total=" << audit.unauthorized_total
                  << ", api_key_rejected=" << audit.api_key_rejected
                  << ", jwt_rejected=" << audit.jwt_rejected
                  << ", jwt_expired=" << audit.jwt_expired
                  << ", last_reason=" << audit.last_reject_reason << '\n';
        return 1;
    }

    std::cout << "w9_gateway_router_auth_test passed"
              << ", unauthorized_total=" << audit.unauthorized_total
              << ", api_key_rejected=" << audit.api_key_rejected
              << ", jwt_rejected=" << audit.jwt_rejected
              << ", invoke_count=" << mock_client->invoke_count()
              << '\n';
    return 0;
}
