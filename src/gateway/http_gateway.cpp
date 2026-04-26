#include "rpc/gateway/http_gateway.h"

// 文件用途：
// 实现网关处理链路：
// 1) Router + Servlet 稳定路由
// 2) API Key/JWT 鉴权拦截与审计
// 3) RPC 返回码到 HTTP 状态码映射

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/json.hpp>
#include <boost/lexical_cast/try_lexical_convert.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "rpc/gateway/gateway.h"
#include "rpc/infra/memory_pool.h"
#include "rpc/infra/structured_log.h"
#include "rpc/runtime/runtime.h"

namespace rpc::gateway {

namespace {

namespace beast_base64 = boost::beast::detail::base64;
namespace json = boost::json;
namespace uuids = boost::uuids;

std::atomic<std::uint64_t> g_span_seq{0};

const char* dominant_stage_name(
    std::uint64_t route_match_ms,
    std::uint64_t auth_ms,
    std::uint64_t handler_ms
) noexcept {
    if (handler_ms >= route_match_ms && handler_ms >= auth_ms) {
        return "handler";
    }
    if (auth_ms >= route_match_ms) {
        return "auth";
    }
    return "route_match";
}

std::string lower_ascii(std::string_view value) {
    std::string lower(value);
    boost::algorithm::to_lower(lower);
    return lower;
}

std::string upper_ascii(std::string value) {
    boost::algorithm::to_upper(value);
    return value;
}

bool starts_with(const std::string& value, const std::string& prefix) {
    return boost::algorithm::starts_with(value, prefix);
}

std::uint64_t parse_u64_or_default(const std::string& value, std::uint64_t default_value) {
    if (value.empty()) {
        return default_value;
    }

    std::uint64_t parsed = 0;
    if (!boost::conversion::try_lexical_convert(value, parsed)) {
        return default_value;
    }
    return parsed;
}

std::string base64url_to_base64(std::string input) {
    boost::algorithm::replace_all(input, "-", "+");
    boost::algorithm::replace_all(input, "_", "/");
    const std::size_t padding = (4 - (input.size() % 4)) % 4;
    input.append(padding, '=');
    return input;
}

bool decode_base64url(const std::string& input, std::string* output) {
    if (output == nullptr) {
        return false;
    }

    const std::string canonical = base64url_to_base64(input);
    std::string decoded;
    decoded.resize(beast_base64::decoded_size(canonical.size()));
    const auto result = beast_base64::decode(decoded.data(), canonical.data(), canonical.size());
    if (result.second != canonical.size()) {
        return false;
    }
    decoded.resize(result.first);
    *output = std::move(decoded);
    return true;
}

bool split_jwt(const std::string& token, std::string* header, std::string* payload, std::string* signature) {
    if (header == nullptr || payload == nullptr || signature == nullptr) {
        return false;
    }

    std::vector<std::string> segments;
    boost::split(segments, token, boost::is_any_of("."), boost::token_compress_off);
    if (segments.size() != 3) {
        return false;
    }
    if (segments[0].empty() || segments[1].empty() || segments[2].empty()) {
        return false;
    }

    *header = std::move(segments[0]);
    *payload = std::move(segments[1]);
    *signature = std::move(segments[2]);
    return true;
}

std::optional<std::string> find_json_string_claim(const json::object& obj, const char* key) {
    const auto it = obj.find(key);
    if (it == obj.end() || !it->value().is_string()) {
        return std::nullopt;
    }
    const json::string& value = it->value().as_string();
    return std::string(value.c_str(), value.size());
}

std::optional<std::int64_t> find_json_int_claim(const json::object& obj, const char* key) {
    const auto it = obj.find(key);
    if (it == obj.end()) {
        return std::nullopt;
    }

    const json::value& value = it->value();
    if (value.is_int64()) {
        return value.as_int64();
    }
    if (value.is_uint64()) {
        const std::uint64_t unsigned_value = value.as_uint64();
        if (unsigned_value <= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            return static_cast<std::int64_t>(unsigned_value);
        }
    }
    return std::nullopt;
}

bool extract_bearer_token(const std::string& authorization, std::string* token) {
    if (token == nullptr) {
        return false;
    }

    static constexpr char kBearerPrefix[] = "Bearer ";
    if (!boost::algorithm::istarts_with(authorization, kBearerPrefix)) {
        return false;
    }

    *token = authorization.substr(sizeof(kBearerPrefix) - 1);
    boost::algorithm::trim(*token);
    return !token->empty();
}

std::string normalize_uri_with_boost(std::string uri_or_target) {
    if (uri_or_target.empty()) {
        return "/";
    }
    const std::size_t trim_pos = uri_or_target.find_first_of("?#");
    if (trim_pos != std::string::npos) {
        uri_or_target.erase(trim_pos);
    }
    if (uri_or_target.empty()) {
        return "/";
    }
    if (uri_or_target.front() != '/') {
        uri_or_target.insert(uri_or_target.begin(), '/');
    }

    std::vector<std::string> segments;
    boost::split(
        segments,
        uri_or_target,
        boost::is_any_of("/"),
        boost::token_compress_on
    );

    std::string compact = "/";
    bool first_segment = true;
    for (const std::string& segment : segments) {
        if (segment.empty()) {
            continue;
        }
        if (!first_segment) {
            compact.push_back('/');
        }
        compact.append(segment);
        first_segment = false;
    }
    return compact;
}

const char* status_reason_phrase(int status) noexcept {
    switch (status) {
        case 200:
            return "OK";
        case 400:
            return "Bad Request";
        case 401:
            return "Unauthorized";
        case 403:
            return "Forbidden";
        case 404:
            return "Not Found";
        case 405:
            return "Method Not Allowed";
        case 408:
            return "Request Timeout";
        case 413:
            return "Payload Too Large";
        case 415:
            return "Unsupported Media Type";
        case 429:
            return "Too Many Requests";
        case 500:
            return "Internal Server Error";
        case 502:
            return "Bad Gateway";
        case 503:
            return "Service Unavailable";
        case 504:
            return "Gateway Timeout";
        default:
            return "Error";
    }
}

std::uint64_t percentile_from_sorted(
    const std::vector<std::uint64_t>& sorted_latencies,
    double percentile
) noexcept {
    if (sorted_latencies.empty()) {
        return 0;
    }
    if (percentile <= 0.0) {
        return sorted_latencies.front();
    }
    if (percentile >= 1.0) {
        return sorted_latencies.back();
    }

    const double rank = std::ceil(percentile * static_cast<double>(sorted_latencies.size()));
    std::size_t index = rank <= 1.0 ? 0 : static_cast<std::size_t>(rank - 1.0);
    if (index >= sorted_latencies.size()) {
        index = sorted_latencies.size() - 1;
    }
    return sorted_latencies[index];
}

}  // namespace

std::string GatewayHttpRequest::header_value(const std::string& key) const {
    const auto direct_it = headers.find(key);
    if (direct_it != headers.end()) {
        return direct_it->second;
    }

    bool maybe_has_upper = false;
    for (char ch : key) {
        if (std::isupper(static_cast<unsigned char>(ch))) {
            maybe_has_upper = true;
            break;
        }
    }
    if (!maybe_has_upper) {
        return {};
    }

    const std::string normalized = lower_ascii(std::string_view(key));
    const auto normalized_it = headers.find(normalized);
    if (normalized_it != headers.end()) {
        return normalized_it->second;
    }
    return {};
}

int map_rpc_code_to_http_status(int rpc_code) noexcept {
    if (rpc_code == 0) {
        return 200;
    }
    if (rpc_code >= 400 && rpc_code < 500) {
        return rpc_code;
    }
    if (rpc_code == 503 || rpc_code == 504) {
        return rpc_code;
    }
    if (rpc_code >= 500 && rpc_code < 600) {
        return 502;
    }
    return 502;
}

AuthInterceptor::AuthInterceptor(AuthInterceptorOptions options) : options_(std::move(options)) {}

bool AuthInterceptor::authorize(const GatewayHttpRequest& request, GatewayHttpResponse* reject_response) {
    if (!options_.enabled) {
        return true;
    }

    const bool has_api_key = !request.header_value("x-api-key").empty();
    const bool has_auth_header = !request.header_value("authorization").empty();

    if (validate_api_key(request)) {
        return true;
    }

    std::string jwt_reject_reason;
    if (validate_jwt(request, &jwt_reject_reason)) {
        return true;
    }

    bool api_key_reject = false;
    bool jwt_reject = false;
    bool jwt_expired = false;
    std::string reason = "unauthorized";

    if (!has_api_key && !has_auth_header) {
        reason = "missing_credentials";
    } else if (has_api_key && !has_auth_header) {
        api_key_reject = true;
        reason = "invalid_api_key";
    } else {
        jwt_reject = has_auth_header;
        api_key_reject = has_api_key;
        reason = jwt_reject_reason.empty() ? "invalid_jwt" : jwt_reject_reason;
        if (reason == "jwt_expired") {
            jwt_expired = true;
        }
    }

    record_reject(reason, api_key_reject, jwt_reject, jwt_expired);

    if (reject_response != nullptr) {
        *reject_response = GatewayHttpResponse{
            401,
            "Unauthorized",
            "unauthorized",
            true,
        };
    }
    return false;
}

AuthAuditSnapshot AuthInterceptor::snapshot() const {
    AuthAuditSnapshot snapshot;
    snapshot.unauthorized_total = unauthorized_total_.load(std::memory_order_acquire);
    snapshot.api_key_rejected = api_key_rejected_.load(std::memory_order_acquire);
    snapshot.jwt_rejected = jwt_rejected_.load(std::memory_order_acquire);
    snapshot.jwt_expired = jwt_expired_.load(std::memory_order_acquire);
    {
        std::lock_guard<std::mutex> lock(audit_mutex_);
        snapshot.last_reject_reason = last_reject_reason_;
    }
    return snapshot;
}

bool AuthInterceptor::validate_api_key(const GatewayHttpRequest& request) const {
    if (options_.required_api_key.empty()) {
        return false;
    }
    return request.header_value("x-api-key") == options_.required_api_key;
}

bool AuthInterceptor::validate_jwt(const GatewayHttpRequest& request, std::string* reject_reason) const {
    const std::string authorization = request.header_value("authorization");
    if (authorization.empty()) {
        return false;
    }

    std::string token;
    if (!extract_bearer_token(authorization, &token)) {
        if (reject_reason != nullptr) {
            *reject_reason = "invalid_authorization_header";
        }
        return false;
    }

    std::string encoded_header;
    std::string encoded_payload;
    std::string signature;
    if (!split_jwt(token, &encoded_header, &encoded_payload, &signature)) {
        if (reject_reason != nullptr) {
            *reject_reason = "invalid_jwt_format";
        }
        return false;
    }

    std::string payload_json;
    if (!decode_base64url(encoded_payload, &payload_json)) {
        if (reject_reason != nullptr) {
            *reject_reason = "invalid_jwt_payload";
        }
        return false;
    }

    boost::system::error_code parse_error;
    const json::value payload_value = json::parse(payload_json, parse_error);
    if (parse_error || !payload_value.is_object()) {
        if (reject_reason != nullptr) {
            *reject_reason = "invalid_jwt_payload";
        }
        return false;
    }
    const json::object& payload_object = payload_value.as_object();

    if (!options_.jwt_issuer.empty()) {
        const std::optional<std::string> issuer = find_json_string_claim(payload_object, "iss");
        if (!issuer || *issuer != options_.jwt_issuer) {
            if (reject_reason != nullptr) {
                *reject_reason = "jwt_issuer_mismatch";
            }
            return false;
        }
    }

    if (!options_.jwt_audience.empty()) {
        const std::optional<std::string> audience = find_json_string_claim(payload_object, "aud");
        if (!audience || *audience != options_.jwt_audience) {
            if (reject_reason != nullptr) {
                *reject_reason = "jwt_audience_mismatch";
            }
            return false;
        }
    }

    const std::optional<std::int64_t> exp = find_json_int_claim(payload_object, "exp");
    if (exp) {
        const auto now = std::chrono::system_clock::now();
        const auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(
            now.time_since_epoch()
        ).count();
        if (now_seconds > (*exp + options_.jwt_clock_skew.count())) {
            if (reject_reason != nullptr) {
                *reject_reason = "jwt_expired";
            }
            return false;
        }
    }

    // 当前实现仅用于工程内联调，不做 JWT 签名校验。
    return true;
}

void AuthInterceptor::record_reject(
    const std::string& reason,
    bool api_key_reject,
    bool jwt_reject,
    bool jwt_expired
) {
    unauthorized_total_.fetch_add(1, std::memory_order_relaxed);
    if (api_key_reject) {
        api_key_rejected_.fetch_add(1, std::memory_order_relaxed);
    }
    if (jwt_reject) {
        jwt_rejected_.fetch_add(1, std::memory_order_relaxed);
    }
    if (jwt_expired) {
        jwt_expired_.fetch_add(1, std::memory_order_relaxed);
    }

    std::lock_guard<std::mutex> lock(audit_mutex_);
    last_reject_reason_ = reason;
}

LambdaServlet::LambdaServlet(Handler handler) : handler_(std::move(handler)) {}

GatewayHttpResponse LambdaServlet::handle(const GatewayHttpRequest& request) {
    if (!handler_) {
        return GatewayHttpResponse{
            500,
            "Internal Server Error",
            "empty_servlet_handler",
            true,
        };
    }
    return handler_(request);
}

bool Router::add_route(
    std::string method,
    std::string uri_pattern,
    ServletPtr servlet,
    bool auth_required
) {
    if (!servlet) {
        return false;
    }

    method = normalize_method(std::move(method));
    uri_pattern = normalize_uri(std::move(uri_pattern));

    MatchResult record;
    record.matched = true;
    record.auth_required = auth_required;
    record.servlet = std::move(servlet);

    if (!uri_pattern.empty() && uri_pattern.back() == '*') {
        uri_pattern.pop_back();
        if (uri_pattern.empty()) {
            uri_pattern = "/";
        }
        prefix_routes_.push_back(PrefixRoute{method, uri_pattern, auth_required, record.servlet});

        std::sort(prefix_routes_.begin(), prefix_routes_.end(), [](const PrefixRoute& lhs, const PrefixRoute& rhs) {
            if (lhs.prefix.size() != rhs.prefix.size()) {
                return lhs.prefix.size() > rhs.prefix.size();
            }
            if (lhs.method != rhs.method) {
                return lhs.method < rhs.method;
            }
            return lhs.prefix < rhs.prefix;
        });
        return true;
    }

    exact_routes_[make_route_key(method, uri_pattern)] = std::move(record);
    return true;
}

Router::MatchResult Router::match(const std::string& method, const std::string& target) const {
    const std::string normalized_method = normalize_method(method);
    const std::string normalized_path = normalize_uri(target);

    const auto exact_it = exact_routes_.find(make_route_key(normalized_method, normalized_path));
    if (exact_it != exact_routes_.end()) {
        return exact_it->second;
    }

    const auto any_method_it = exact_routes_.find(make_route_key("*", normalized_path));
    if (any_method_it != exact_routes_.end()) {
        return any_method_it->second;
    }

    for (const PrefixRoute& route : prefix_routes_) {
        if (route.method != "*" && route.method != normalized_method) {
            continue;
        }
        if (starts_with(normalized_path, route.prefix)) {
            return MatchResult{
                true,
                route.auth_required,
                route.servlet,
            };
        }
    }

    return MatchResult{};
}

std::string Router::normalize_method(std::string method) {
    if (method.empty()) {
        return "GET";
    }
    return upper_ascii(std::move(method));
}

std::string Router::normalize_uri(std::string uri_or_target) {
    return normalize_uri_with_boost(std::move(uri_or_target));
}

std::string Router::make_route_key(const std::string& method, const std::string& path) {
    return method + " " + path;
}

HttpGateway::HttpGateway(
    HttpGatewayOptions options,
    std::shared_ptr<rpc::client::IRpcClient> rpc_client
)
    : options_(std::move(options)),
      auth_interceptor_(options_.auth),
      rpc_client_(std::move(rpc_client)) {
    register_builtin_routes();
}

GatewayHttpResponse HttpGateway::handle(const GatewayHttpRequest& request) {
    const auto started_at = std::chrono::steady_clock::now();
    const std::string route = make_route_key(request);
    std::uint64_t route_match_ms = 0;
    std::uint64_t auth_ms = 0;
    std::uint64_t handler_ms = 0;

    std::string trace_id = request.header_value("x-trace-id");
    std::string span_id = request.header_value("x-span-id");
    const GatewayHttpRequest* effective_request = &request;
    GatewayHttpRequest trace_enriched_request;
    if (trace_id.empty()) {
        trace_id = make_trace_id();
    }
    if (span_id.empty()) {
        span_id = make_span_id();
    }
    if (request.header_value("x-trace-id").empty() || request.header_value("x-span-id").empty()) {
        trace_enriched_request = request;
        trace_enriched_request.headers["x-trace-id"] = trace_id;
        trace_enriched_request.headers["x-span-id"] = span_id;
        effective_request = &trace_enriched_request;
    }

    GatewayHttpResponse final_response;
    const auto route_match_started = std::chrono::steady_clock::now();
    const Router::MatchResult matched = router_.match(
        effective_request->method,
        effective_request->target
    );
    route_match_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - route_match_started
        ).count()
    );

    if (!matched.matched || !matched.servlet) {
        final_response = make_response(404, "Not Found", "route_not_found", true);
    } else {
        bool can_dispatch = true;
        if (matched.auth_required) {
            const auto auth_started = std::chrono::steady_clock::now();
            GatewayHttpResponse reject;
            if (!auth_interceptor_.authorize(*effective_request, &reject)) {
                final_response = std::move(reject);
                can_dispatch = false;
            }
            auth_ms = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - auth_started
                ).count()
            );
        }

        if (can_dispatch) {
            const auto handler_started = std::chrono::steady_clock::now();
            try {
                final_response = matched.servlet->handle(*effective_request);
                if (final_response.reason.empty()) {
                    final_response.reason = status_reason_phrase(final_response.status);
                }
            } catch (...) {
                final_response = make_response(
                    500,
                    "Internal Server Error",
                    "gateway_handler_exception",
                    true
                );
            }
            handler_ms = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - handler_started
                ).count()
            );
        }
    }

    const auto elapsed = std::chrono::steady_clock::now() - started_at;
    const std::uint64_t latency_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
    );
    record_route_metrics(route, latency_ms, final_response.status);
    record_trace_snapshot(trace_id, span_id, route, final_response.status, latency_ms);

    const ::rpc::infra::MemoryPoolStats memory_stats = ::rpc::infra::AdaptiveMemoryPool::instance().stats();
    boost::json::object log_fields;
    log_fields["span_id"] = span_id;
    log_fields["route"] = route;
    log_fields["status"] = final_response.status;
    log_fields["route_match_ms"] = route_match_ms;
    log_fields["auth_ms"] = auth_ms;
    log_fields["handler_ms"] = handler_ms;
    log_fields["slow_stage"] = dominant_stage_name(route_match_ms, auth_ms, handler_ms);
    log_fields["mem_active_allocations"] = memory_stats.active_allocations;
    log_fields["mem_active_bytes_high_watermark"] = memory_stats.active_bytes_high_watermark;
    log_fields["mem_short_hit_ratio"] = memory_stats.short_lived.hit_ratio;
    log_fields["mem_long_hit_ratio"] = memory_stats.long_lived.hit_ratio;

    ::rpc::infra::structured_log(
        final_response.status >= 500
            ? ::rpc::infra::LogLevel::Error
            : (final_response.status >= 400 ? ::rpc::infra::LogLevel::Warn : ::rpc::infra::LogLevel::Info),
        "gateway.request",
        std::move(log_fields),
        trace_id,
        latency_ms
    );

    return final_response;
}

AuthAuditSnapshot HttpGateway::auth_audit_snapshot() const {
    return auth_interceptor_.snapshot();
}

std::vector<RouteMetricsSnapshot> HttpGateway::route_metrics_snapshot() const {
    std::vector<RouteMetricsSnapshot> snapshots;
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    snapshots.reserve(route_metrics_.size());

    for (const auto& entry : route_metrics_) {
        const std::string& route = entry.first;
        const RouteMetricsBucket& bucket = entry.second;

        RouteMetricsSnapshot snapshot;
        snapshot.route = route;
        snapshot.requests = bucket.requests;
        snapshot.errors = bucket.errors;
        snapshot.error_rate = bucket.requests == 0
            ? 0.0
            : static_cast<double>(bucket.errors) / static_cast<double>(bucket.requests);

        std::vector<std::uint64_t> sorted_latencies = bucket.latencies_ms;
        std::sort(sorted_latencies.begin(), sorted_latencies.end());
        snapshot.latency_p50_ms = percentile_from_sorted(sorted_latencies, 0.50);
        snapshot.latency_p90_ms = percentile_from_sorted(sorted_latencies, 0.90);
        snapshot.latency_p99_ms = percentile_from_sorted(sorted_latencies, 0.99);

        snapshots.push_back(std::move(snapshot));
    }

    std::sort(snapshots.begin(), snapshots.end(), [](const RouteMetricsSnapshot& lhs, const RouteMetricsSnapshot& rhs) {
        return lhs.route < rhs.route;
    });
    return snapshots;
}

GatewayTraceSnapshot HttpGateway::last_trace_snapshot() const {
    std::lock_guard<std::mutex> lock(trace_mutex_);
    return last_trace_snapshot_;
}

Router& HttpGateway::router() noexcept {
    return router_;
}

const Router& HttpGateway::router() const noexcept {
    return router_;
}

GatewayHttpResponse HttpGateway::handle_rpc_invoke(const GatewayHttpRequest& request) {
    const auto rpc_client = ensure_rpc_client();
    if (!rpc_client) {
        return make_response(503, "Service Unavailable", "rpc_client_not_ready", true);
    }

    rpc::client::RpcRequest rpc_request;
    rpc_request.service = request.header_value("x-rpc-service");
    rpc_request.method = request.header_value("x-rpc-method");
    rpc_request.payload = request.body;

    if (rpc_request.service.empty()) {
        rpc_request.service = "gateway.backend";
    }
    if (rpc_request.method.empty()) {
        rpc_request.method = "Echo";
    }

    const std::uint64_t downstream_timeout_ms = parse_u64_or_default(
        request.header_value("x-rpc-timeout-ms"),
        static_cast<std::uint64_t>(options_.upstream_timeout.count())
    );
    rpc_request.timeout_ms = downstream_timeout_ms;

    const std::uint64_t max_retries = parse_u64_or_default(request.header_value("x-rpc-max-retries"), 0);
    rpc_request.max_retries = static_cast<std::size_t>(max_retries);

    std::string trace_id = request.header_value("x-trace-id");
    if (trace_id.empty()) {
        trace_id = make_trace_id();
    }
    std::string span_id = request.header_value("x-span-id");
    if (span_id.empty()) {
        span_id = make_span_id();
    }
    rpc_request.metadata["x-trace-id"] = trace_id;
    rpc_request.metadata["trace_id"] = trace_id;
    rpc_request.metadata["x-span-id"] = span_id;
    rpc_request.metadata["span_id"] = span_id;

    const auto forward_metadata_header = [&](const char* header_name, const char* metadata_key = nullptr) {
        const std::string value = request.header_value(header_name);
        if (value.empty()) {
            return;
        }
        rpc_request.metadata[metadata_key == nullptr ? header_name : metadata_key] = value;
    };

    // W13: 灰度分流 + 流量染色。
    forward_metadata_header("x-traffic-tag");
    forward_metadata_header("x-user-id");
    forward_metadata_header("x-gray-percent");
    forward_metadata_header("x-gray-tag");
    forward_metadata_header("x-stable-tag");

    // W13: 自适应负载权重参数。
    forward_metadata_header("x-lb-weight-cpu");
    forward_metadata_header("x-lb-weight-mem");
    forward_metadata_header("x-lb-weight-qps");
    forward_metadata_header("x-lb-weight-latency");
    forward_metadata_header("x-lb-target-qps");
    forward_metadata_header("x-lb-target-latency-ms");

    // W13: 熔断状态机参数。
    forward_metadata_header("x-cb-failure-threshold");
    forward_metadata_header("x-cb-open-ms");
    forward_metadata_header("x-cb-half-open-success");
    forward_metadata_header("x-cb-half-open-max-probes");

    // W13: 降级策略参数。
    forward_metadata_header("x-fallback-cache");
    forward_metadata_header("x-fallback-static");
    forward_metadata_header("x-fallback-cache-key");
    forward_metadata_header("x-fallback-cache-ttl-ms");

    const std::uint64_t upstream_timeout_ms = parse_u64_or_default(
        request.header_value("x-upstream-timeout-ms"),
        static_cast<std::uint64_t>(options_.upstream_timeout.count())
    );

    std::string request_id = request.header_value("x-request-id");
    if (request_id.empty()) {
        request_id = "gateway-http-" + std::to_string(
            request_id_seq_.fetch_add(1, std::memory_order_relaxed) + 1
        );
    }
    rpc_request.metadata["x-request-id"] = request_id;

    const auto context = rpc::runtime::create_deadline_context(
        std::move(request_id),
        std::chrono::milliseconds(upstream_timeout_ms)
    );
    rpc::runtime::set_deadline_context_value(context, "trace_id", trace_id);
    rpc::runtime::ScopedDeadlineContext scoped_context(context);

    const auto rpc_invoke_started = std::chrono::steady_clock::now();
    const rpc::client::RpcResponse rpc_response = rpc_client->invoke(rpc_request);
    const std::uint64_t rpc_invoke_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - rpc_invoke_started
        ).count()
    );

    const int status = map_rpc_code_to_http_status(rpc_response.code);
    std::string body;
    if (rpc_response.code == 0) {
        body = rpc_response.payload;
    } else {
        body = rpc_response.message.empty() ? "upstream_error" : rpc_response.message;
    }

    boost::json::object rpc_log_fields;
    rpc_log_fields["span_id"] = span_id;
    rpc_log_fields["service"] = rpc_request.service;
    rpc_log_fields["method"] = rpc_request.method;
    rpc_log_fields["http_status"] = status;
    rpc_log_fields["rpc_code"] = rpc_response.code;
    rpc_log_fields["attempts"] = rpc_response.attempts;
    rpc_log_fields["selected_endpoint"] = rpc_response.selected_endpoint;
    rpc_log_fields["degraded"] = rpc_response.degraded;
    rpc_log_fields["degrade_strategy"] = rpc_response.degrade_strategy;
    rpc_log_fields["traffic_lane"] = rpc_response.traffic_lane;
    rpc_log_fields["circuit_state"] = rpc_response.circuit_state;
    rpc_log_fields["slow_stage"] = "rpc_invoke";

    ::rpc::infra::structured_log(
        rpc_response.code == 0
            ? ::rpc::infra::LogLevel::Info
            : (rpc_response.code >= 500 ? ::rpc::infra::LogLevel::Error : ::rpc::infra::LogLevel::Warn),
        "gateway.rpc_invoke",
        std::move(rpc_log_fields),
        trace_id,
        rpc_invoke_ms
    );

    return make_response(status, status_reason_phrase(status), std::move(body), true);
}

GatewayHttpResponse HttpGateway::make_response(
    int status,
    std::string reason,
    std::string body,
    bool keep_alive
) {
    GatewayHttpResponse response;
    response.status = status;
    response.reason = std::move(reason);
    response.body = std::move(body);
    response.keep_alive = keep_alive;
    return response;
}

bool HttpGateway::is_error_status(int status) noexcept {
    return status >= 400;
}

std::string HttpGateway::make_route_key(const GatewayHttpRequest& request) {
    return Router::normalize_method(request.method) + " " + Router::normalize_uri(request.target);
}

std::string HttpGateway::make_trace_id() {
    thread_local uuids::random_generator generator;
    try {
        return uuids::to_string(generator());
    } catch (...) {
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        return "trace-fallback-" + std::to_string(now);
    }
}

std::string HttpGateway::make_span_id() {
    const std::uint64_t seq = g_span_seq.fetch_add(1, std::memory_order_relaxed) + 1;
    return "span-" + std::to_string(seq);
}

void HttpGateway::record_route_metrics(
    const std::string& route,
    std::uint64_t latency_ms,
    int http_status
) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    RouteMetricsBucket& bucket = route_metrics_[route];
    ++bucket.requests;
    if (is_error_status(http_status)) {
        ++bucket.errors;
    }
    bucket.latencies_ms.push_back(latency_ms);
}

void HttpGateway::record_trace_snapshot(
    std::string trace_id,
    std::string span_id,
    std::string route,
    int http_status,
    std::uint64_t latency_ms
) {
    std::lock_guard<std::mutex> lock(trace_mutex_);
    last_trace_snapshot_.trace_id = std::move(trace_id);
    last_trace_snapshot_.span_id = std::move(span_id);
    last_trace_snapshot_.route = std::move(route);
    last_trace_snapshot_.http_status = http_status;
    last_trace_snapshot_.latency_ms = latency_ms;
}

std::shared_ptr<rpc::client::IRpcClient> HttpGateway::ensure_rpc_client() {
    if (rpc_client_) {
        return rpc_client_;
    }
    rpc_client_ = rpc::client::default_client();
    return rpc_client_;
}

void HttpGateway::register_builtin_routes() {
    const auto build_echo_body = [](const GatewayHttpRequest& request) {
        const std::string normalized_target = Router::normalize_uri(request.target);
        const std::string body_size = std::to_string(request.body.size());

        std::string body;
        body.reserve(
            22 + request.method.size() + normalized_target.size() + body_size.size()
        );
        body.append("ok method=");
        body.append(request.method);
        body.append(" target=");
        body.append(normalized_target);
        body.append(" body_bytes=");
        body.append(body_size);
        return body;
    };

    router_.add_route(
        "GET",
        "/health",
        std::make_shared<LambdaServlet>([](const GatewayHttpRequest&) {
            return make_response(200, "OK", "healthy", true);
        }),
        false
    );

    router_.add_route(
        "POST",
        "/short",
        std::make_shared<LambdaServlet>([build_echo_body](const GatewayHttpRequest& request) {
            return make_response(200, "OK", build_echo_body(request), true);
        }),
        false
    );

    router_.add_route(
        "POST",
        "/slow",
        std::make_shared<LambdaServlet>([build_echo_body](const GatewayHttpRequest& request) {
            std::this_thread::sleep_for(std::chrono::milliseconds(40));
            return make_response(200, "OK", build_echo_body(request), true);
        }),
        false
    );

    router_.add_route(
        "POST",
        "/keep-*",
        std::make_shared<LambdaServlet>([build_echo_body](const GatewayHttpRequest& request) {
            return make_response(200, "OK", build_echo_body(request), true);
        }),
        false
    );

    router_.add_route(
        "POST",
        "/rpc/invoke",
        std::make_shared<LambdaServlet>([this](const GatewayHttpRequest& request) {
            return handle_rpc_invoke(request);
        }),
        true
    );
}

}  // namespace rpc::gateway
