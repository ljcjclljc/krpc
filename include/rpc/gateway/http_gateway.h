#pragma once

// 文件用途：
// 定义网关 HTTP 处理抽象：Router + Servlet + 鉴权拦截器 + RPC 错误码映射。

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>

#include "rpc/rpc/client.h"

namespace rpc::gateway {

// 网关统一请求模型（由 net 层解析后传入）。
struct GatewayHttpRequest {
    std::string method;
    std::string target;
    std::unordered_map<std::string, std::string> headers;
    std::string body;

    std::string header_value(const std::string& key) const;
};

// 网关统一响应模型（由 gateway 计算后返回 net 层封包）。
struct GatewayHttpResponse {
    int status{200};
    std::string reason{"OK"};
    std::string body;
    bool keep_alive{true};
};

// 路由维度聚合指标快照。
struct RouteMetricsSnapshot {
    std::string route;
    std::size_t requests{0};
    std::size_t errors{0};
    double error_rate{0.0};
    std::uint64_t latency_p50_ms{0};
    std::uint64_t latency_p90_ms{0};
    std::uint64_t latency_p99_ms{0};
};

// 网关最近一次请求追踪信息（用于 TraceId 关联）。
struct GatewayTraceSnapshot {
    std::string trace_id;
    std::string span_id;
    std::string route;
    int http_status{0};
    std::uint64_t latency_ms{0};
};

// 将 RPC 层状态码映射到 HTTP 语义状态码。
int map_rpc_code_to_http_status(int rpc_code) noexcept;

// 鉴权审计快照。
struct AuthAuditSnapshot {
    std::size_t unauthorized_total{0};
    std::size_t api_key_rejected{0};
    std::size_t jwt_rejected{0};
    std::size_t jwt_expired{0};
    std::string last_reject_reason;
};

struct AuthInterceptorOptions {
    bool enabled{false};
    std::string required_api_key;
    std::string jwt_issuer;
    std::string jwt_audience;
    std::chrono::seconds jwt_clock_skew{0};
};

// API Key/JWT 拦截器（OR 语义：任一方式通过即放行）。
class AuthInterceptor final {
public:
    explicit AuthInterceptor(AuthInterceptorOptions options = {});

    bool authorize(const GatewayHttpRequest& request, GatewayHttpResponse* reject_response);
    AuthAuditSnapshot snapshot() const;

private:
    bool validate_api_key(const GatewayHttpRequest& request) const;
    bool validate_jwt(const GatewayHttpRequest& request, std::string* reject_reason) const;
    void record_reject(
        const std::string& reason,
        bool api_key_reject,
        bool jwt_reject,
        bool jwt_expired
    );

    AuthInterceptorOptions options_{};

    mutable std::mutex audit_mutex_;
    std::atomic<std::size_t> unauthorized_total_{0};
    std::atomic<std::size_t> api_key_rejected_{0};
    std::atomic<std::size_t> jwt_rejected_{0};
    std::atomic<std::size_t> jwt_expired_{0};
    std::string last_reject_reason_;
};

class Servlet {
public:
    virtual ~Servlet() = default;
    virtual GatewayHttpResponse handle(const GatewayHttpRequest& request) = 0;
};

using ServletPtr = std::shared_ptr<Servlet>;

// 便捷 Servlet：直接用 lambda 组装处理逻辑。
class LambdaServlet final : public Servlet {
public:
    using Handler = std::function<GatewayHttpResponse(const GatewayHttpRequest&)>;

    explicit LambdaServlet(Handler handler);
    GatewayHttpResponse handle(const GatewayHttpRequest& request) override;

private:
    Handler handler_;
};

class Router {
public:
    struct MatchResult {
        bool matched{false};
        bool auth_required{false};
        ServletPtr servlet;
    };

    // 注册路由；uri_pattern 支持精确匹配和前缀匹配（以 '*' 结尾）。
    bool add_route(
        std::string method,
        std::string uri_pattern,
        ServletPtr servlet,
        bool auth_required
    );

    MatchResult match(const std::string& method, const std::string& target) const;

    static std::string normalize_method(std::string method);
    static std::string normalize_uri(std::string uri_or_target);

private:
    struct PrefixRoute {
        std::string method;
        std::string prefix;
        bool auth_required{false};
        ServletPtr servlet;
    };

    static std::string make_route_key(const std::string& method, const std::string& path);

    std::unordered_map<std::string, MatchResult> exact_routes_;
    std::vector<PrefixRoute> prefix_routes_;
};

struct HttpGatewayOptions {
    std::chrono::milliseconds upstream_timeout{300};
    AuthInterceptorOptions auth{};
};

// 网关 HTTP 入口：负责路由、鉴权、RPC 调用与错误码映射。
class HttpGateway {
public:
    explicit HttpGateway(
        HttpGatewayOptions options = {},
        std::shared_ptr<rpc::client::IRpcClient> rpc_client = {}
    );

    GatewayHttpResponse handle(const GatewayHttpRequest& request);
    AuthAuditSnapshot auth_audit_snapshot() const;
    std::vector<RouteMetricsSnapshot> route_metrics_snapshot() const;
    GatewayTraceSnapshot last_trace_snapshot() const;

    Router& router() noexcept;
    const Router& router() const noexcept;

private:
    GatewayHttpResponse handle_rpc_invoke(const GatewayHttpRequest& request);
    static GatewayHttpResponse make_response(
        int status,
        std::string reason,
        std::string body,
        bool keep_alive
    );
    static bool is_error_status(int status) noexcept;
    static std::string make_route_key(const GatewayHttpRequest& request);
    static std::string make_trace_id();
    static std::string make_span_id();
    void record_route_metrics(
        const std::string& route,
        std::uint64_t latency_ms,
        int http_status
    );
    void record_trace_snapshot(
        std::string trace_id,
        std::string span_id,
        std::string route,
        int http_status,
        std::uint64_t latency_ms
    );

    std::shared_ptr<rpc::client::IRpcClient> ensure_rpc_client();
    void register_builtin_routes();

    struct RouteMetricsBucket {
        std::size_t requests{0};
        std::size_t errors{0};
        std::vector<std::uint64_t> latencies_ms;
    };

    HttpGatewayOptions options_{};
    Router router_;
    AuthInterceptor auth_interceptor_;
    std::shared_ptr<rpc::client::IRpcClient> rpc_client_;
    std::atomic<std::uint64_t> request_id_seq_{0};
    mutable std::mutex metrics_mutex_;
    std::unordered_map<std::string, RouteMetricsBucket> route_metrics_;
    mutable std::mutex trace_mutex_;
    GatewayTraceSnapshot last_trace_snapshot_;
};

}  // namespace rpc::gateway
