#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/system/error_code.hpp>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rpc/net/reactor_http_server.h"
#include "rpc/rpc/client.h"
#include "rpc/runtime/runtime.h"

namespace {

namespace http = boost::beast::http;
using namespace std::chrono_literals;

class StaticDiscovery final : public rpc::client::IServiceDiscovery {
public:
    explicit StaticDiscovery(std::unordered_map<std::string, std::vector<rpc::client::ServiceNode>> services)
        : services_(std::move(services)) {}

    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override {
        const auto it = services_.find(service_name);
        if (it == services_.end()) {
            return {};
        }
        return it->second;
    }

private:
    std::unordered_map<std::string, std::vector<rpc::client::ServiceNode>> services_;
};

class FirstNodeLoadBalancer final : public rpc::client::ILoadBalancer {
public:
    std::size_t select_node(
        const std::vector<rpc::client::ServiceNode>& /*nodes*/,
        const rpc::client::RpcRequest& /*request*/
    ) override {
        return 0;
    }
};

class ScopedEchoBackend final {
public:
    ScopedEchoBackend() = default;
    ~ScopedEchoBackend() {
        stop();
    }

    bool start() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return false;
        }

        const int reuse = 1;
        (void)::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, static_cast<socklen_t>(sizeof(reuse)));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(0);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
            stop();
            return false;
        }
        if (::listen(listen_fd_, 16) != 0) {
            stop();
            return false;
        }

        sockaddr_in bound{};
        socklen_t bound_len = static_cast<socklen_t>(sizeof(bound));
        if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &bound_len) != 0) {
            stop();
            return false;
        }
        port_ = ntohs(bound.sin_port);
        if (port_ == 0) {
            stop();
            return false;
        }

        worker_ = std::thread([this]() {
            while (true) {
                sockaddr_in peer{};
                socklen_t peer_len = static_cast<socklen_t>(sizeof(peer));
                const int client_fd = ::accept(
                    listen_fd_,
                    reinterpret_cast<sockaddr*>(&peer),
                    &peer_len
                );
                if (client_fd < 0) {
                    return;
                }

                char buffer[4096];
                std::string request_payload;
                while (true) {
                    const ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
                    if (n > 0) {
                        request_payload.append(buffer, static_cast<std::size_t>(n));
                        if (static_cast<std::size_t>(n) < sizeof(buffer)) {
                            break;
                        }
                        continue;
                    }
                    if (n == 0) {
                        break;
                    }
                    if (errno == EINTR) {
                        continue;
                    }
                    break;
                }

                const std::string response = "tcp-echo:" + request_payload;
                std::size_t sent = 0;
                while (sent < response.size()) {
                    const ssize_t n = ::send(
                        client_fd,
                        response.data() + static_cast<std::ptrdiff_t>(sent),
                        response.size() - sent,
                        MSG_NOSIGNAL
                    );
                    if (n > 0) {
                        sent += static_cast<std::size_t>(n);
                        continue;
                    }
                    if (n < 0 && errno == EINTR) {
                        continue;
                    }
                    break;
                }

                ::close(client_fd);
            }
        });

        return true;
    }

    void stop() {
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    std::uint16_t port() const noexcept {
        return port_;
    }

private:
    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::thread worker_;
};

bool set_recv_timeout(int fd, std::chrono::milliseconds timeout) {
    timeval tv{};
    tv.tv_sec = static_cast<long>(timeout.count() / 1000);
    tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);
    return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, static_cast<socklen_t>(sizeof(tv))) == 0;
}

int connect_to_server(std::uint16_t port) {
    for (int attempt = 0; attempt < 60; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) == 0) {
            return fd;
        }

        ::close(fd);
        std::this_thread::sleep_for(10ms);
    }
    return -1;
}

bool send_all(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(
            fd,
            data.data() + static_cast<std::ptrdiff_t>(sent),
            data.size() - sent,
            MSG_NOSIGNAL
        );
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

struct ParsedResponse {
    int status{0};
    std::string body;
};

class HttpResponseReader {
public:
    explicit HttpResponseReader(int fd) : fd_(fd) {}

    bool read_one(ParsedResponse* response) {
        if (response == nullptr) {
            return false;
        }

        http::response_parser<http::string_body> parser;
        parser.body_limit(4 * 1024 * 1024);

        while (true) {
            boost::system::error_code ec;
            const std::size_t consumed = parser.put(boost::asio::buffer(buffer_.data(), buffer_.size()), ec);
            if (consumed > 0) {
                buffer_.erase(0, consumed);
            }

            if (ec == http::error::need_more) {
                if (!recv_more()) {
                    return false;
                }
                continue;
            }
            if (ec) {
                return false;
            }
            if (!parser.is_done()) {
                if (!recv_more()) {
                    return false;
                }
                continue;
            }

            http::response<http::string_body> parsed = parser.release();
            response->status = parsed.result_int();
            response->body = std::move(parsed.body());
            return true;
        }
    }

private:
    bool recv_more() {
        char temp[4096];
        const ssize_t n = ::recv(fd_, temp, sizeof(temp), 0);
        if (n > 0) {
            buffer_.append(temp, static_cast<std::size_t>(n));
            return true;
        }
        if (n < 0 && errno == EINTR) {
            return recv_more();
        }
        return false;
    }

    int fd_{-1};
    std::string buffer_;
};

std::string build_post_request(
    const std::string& target,
    const std::string& body,
    const std::vector<std::pair<std::string, std::string>>& headers
) {
    http::request<http::string_body> request;
    request.version(11);
    request.method(http::verb::post);
    request.target(target);
    request.set(http::field::host, "127.0.0.1");
    request.set(http::field::connection, "close");

    for (const auto& header : headers) {
        request.set(header.first, header.second);
    }

    request.body() = body;
    request.prepare_payload();

    std::ostringstream stream;
    stream << request;
    return stream.str();
}

bool send_request_once(
    std::uint16_t port,
    const std::string& request,
    ParsedResponse* response
) {
    const int fd = connect_to_server(port);
    if (fd < 0) {
        return false;
    }

    (void)set_recv_timeout(fd, 5s);

    const bool sent = send_all(fd, request);
    bool received = false;
    if (sent) {
        HttpResponseReader reader(fd);
        received = reader.read_one(response);
    }

    ::close(fd);
    return sent && received;
}

const rpc::gateway::RouteMetricsSnapshot* find_route_metric(
    const std::vector<rpc::gateway::RouteMetricsSnapshot>& metrics,
    const std::string& route
) {
    for (const auto& metric : metrics) {
        if (metric.route == route) {
            return &metric;
        }
    }
    return nullptr;
}

}  // namespace

int main() {
    // In restricted sandboxes, socket syscalls may be blocked (EPERM/EACCES).
    // Skip this integration-style test in that environment.
    int probe_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (probe_fd < 0 && (errno == EPERM || errno == EACCES)) {
        std::cout << "w10_gateway_rpc_trace_metrics_test skipped: socket operations are not permitted\n";
        return 0;
    }
    if (probe_fd >= 0) {
        ::close(probe_fd);
    }

    ScopedEchoBackend tcp_backend;
    if (!tcp_backend.start()) {
        std::cerr << "failed to start local tcp echo backend\n";
        return 1;
    }

    rpc::client::ClientInitOptions client_options;
    client_options.discovery = std::make_shared<StaticDiscovery>(
        std::unordered_map<std::string, std::vector<rpc::client::ServiceNode>>{
            {"svc.echo", {rpc::client::ServiceNode{"a", "127.0.0.1", tcp_backend.port()}}},
            {"svc.tcp-echo", {rpc::client::ServiceNode{"tcp-a", "127.0.0.1", tcp_backend.port()}}},
        }
    );
    client_options.load_balancer = std::make_shared<FirstNodeLoadBalancer>();
    rpc::client::init_client(std::move(client_options));

    rpc::net::ReactorHttpServerOptions options;
    options.bind_address = "127.0.0.1";
    options.port = 0;
    options.max_connections = 256;
    options.ingress_queue_capacity = 32;
    options.worker_threads = 1;
    options.max_header_bytes = 8 * 1024;
    options.max_body_bytes = 1024 * 1024;
    options.max_connection_requests_inflight = 8;
    options.max_connection_write_buffer_bytes = 64 * 1024;
    options.enable_auth = false;
    options.gateway_upstream_timeout_ms = 300;

    rpc::net::ReactorHttpServer server(options);
    if (!server.start()) {
        std::cerr << "failed to start reactor http server\n";
        return 1;
    }

    const std::uint16_t port = server.listen_port();
    if (port == 0) {
        std::cerr << "invalid listen port\n";
        return 1;
    }

    // 1) HTTP -> RPC -> HTTP with explicit TraceId.
    ParsedResponse explicit_trace_response;
    const std::string explicit_trace_id = "trace-w10-explicit";
    if (!send_request_once(
            port,
            build_post_request(
                "/rpc/invoke",
                "hello-w10-explicit",
                {
                    {"x-rpc-service", "svc.echo"},
                    {"x-rpc-method", "Echo"},
                    {"x-trace-id", explicit_trace_id},
                }
            ),
            &explicit_trace_response)) {
        std::cerr << "explicit trace request failed\n";
        return 1;
    }

    if (explicit_trace_response.status != 200
        || explicit_trace_response.body.find("hello-w10-explicit") == std::string::npos) {
        std::cerr << "unexpected explicit trace response, status="
                  << explicit_trace_response.status
                  << ", body=" << explicit_trace_response.body << '\n';
        return 1;
    }

    const rpc::gateway::GatewayTraceSnapshot gateway_trace_a = server.last_trace_snapshot();
    const rpc::client::RpcInvokeAuditSnapshot rpc_trace_a = rpc::client::last_invoke_audit_snapshot();
    if (gateway_trace_a.trace_id != explicit_trace_id || rpc_trace_a.trace_id != explicit_trace_id) {
        std::cerr << "explicit trace not correlated, gateway_trace=" << gateway_trace_a.trace_id
                  << ", rpc_trace=" << rpc_trace_a.trace_id << '\n';
        return 1;
    }
    if (gateway_trace_a.span_id.empty()
        || rpc_trace_a.span_id.empty()
        || gateway_trace_a.span_id != rpc_trace_a.span_id) {
        std::cerr << "explicit span not correlated, gateway_span=" << gateway_trace_a.span_id
                  << ", rpc_span=" << rpc_trace_a.span_id << '\n';
        return 1;
    }

    // 2) Missing TraceId should be generated and propagated.
    ParsedResponse generated_trace_response;
    if (!send_request_once(
            port,
            build_post_request(
                "/rpc/invoke",
                "hello-w10-generated",
                {
                    {"x-rpc-service", "svc.echo"},
                    {"x-rpc-method", "Echo"},
                }
            ),
            &generated_trace_response)) {
        std::cerr << "generated trace request failed\n";
        return 1;
    }

    if (generated_trace_response.status != 200) {
        std::cerr << "unexpected generated trace response status="
                  << generated_trace_response.status << '\n';
        return 1;
    }

    const rpc::gateway::GatewayTraceSnapshot gateway_trace_b = server.last_trace_snapshot();
    const rpc::client::RpcInvokeAuditSnapshot rpc_trace_b = rpc::client::last_invoke_audit_snapshot();
    if (gateway_trace_b.trace_id.empty()
        || rpc_trace_b.trace_id.empty()
        || gateway_trace_b.trace_id != rpc_trace_b.trace_id) {
        std::cerr << "generated trace not propagated, gateway_trace=" << gateway_trace_b.trace_id
                  << ", rpc_trace=" << rpc_trace_b.trace_id << '\n';
        return 1;
    }
    if (gateway_trace_b.trace_id == explicit_trace_id) {
        std::cerr << "generated trace id unexpectedly equals explicit trace id\n";
        return 1;
    }
    if (gateway_trace_b.span_id.empty()
        || rpc_trace_b.span_id.empty()
        || gateway_trace_b.span_id != rpc_trace_b.span_id
        || gateway_trace_b.span_id == gateway_trace_a.span_id) {
        std::cerr << "generated span not propagated, gateway_span=" << gateway_trace_b.span_id
                  << ", rpc_span=" << rpc_trace_b.span_id << '\n';
        return 1;
    }

    // 3) Route-level error metrics on /rpc/invoke.
    ParsedResponse error_response;
    if (!send_request_once(
            port,
            build_post_request(
                "/rpc/invoke",
                "hello-w10-error",
                {
                    {"x-rpc-service", "svc.not-found"},
                    {"x-rpc-method", "Echo"},
                }
            ),
            &error_response)) {
        std::cerr << "error case request failed\n";
        return 1;
    }

    if (error_response.status != 503) {
        std::cerr << "expected 503 for unknown service, actual=" << error_response.status << '\n';
        return 1;
    }

    // 4) Route-level latency quantiles on /slow.
    for (int i = 0; i < 3; ++i) {
        ParsedResponse slow_response;
        if (!send_request_once(
                port,
                build_post_request("/slow", "body-" + std::to_string(i), {}),
                &slow_response)) {
            std::cerr << "slow request #" << i << " failed\n";
            return 1;
        }
        if (slow_response.status != 200) {
            std::cerr << "slow request #" << i << " unexpected status=" << slow_response.status << '\n';
            return 1;
        }
    }

    const std::vector<rpc::gateway::RouteMetricsSnapshot> metrics = server.route_metrics_snapshot();
    const rpc::gateway::RouteMetricsSnapshot* rpc_route = find_route_metric(metrics, "POST /rpc/invoke");
    if (rpc_route == nullptr) {
        std::cerr << "missing route metric for POST /rpc/invoke\n";
        return 1;
    }

    if (rpc_route->requests < 3 || rpc_route->errors < 1 || rpc_route->error_rate <= 0.0) {
        std::cerr << "rpc route metric mismatch, requests=" << rpc_route->requests
                  << ", errors=" << rpc_route->errors
                  << ", error_rate=" << rpc_route->error_rate << '\n';
        return 1;
    }

    const rpc::gateway::RouteMetricsSnapshot* slow_route = find_route_metric(metrics, "POST /slow");
    if (slow_route == nullptr) {
        std::cerr << "missing route metric for POST /slow\n";
        return 1;
    }

    if (slow_route->requests < 3
        || slow_route->latency_p50_ms < 20
        || slow_route->latency_p90_ms < 20
        || slow_route->latency_p99_ms < 20) {
        std::cerr << "slow route latency quantile mismatch, requests=" << slow_route->requests
                  << ", p50=" << slow_route->latency_p50_ms
                  << ", p90=" << slow_route->latency_p90_ms
                  << ", p99=" << slow_route->latency_p99_ms << '\n';
        return 1;
    }

    // 5) Real TCP transport path: user -> gateway -> rpc -> backend.
    ParsedResponse tcp_transport_response;
    if (!send_request_once(
            port,
            build_post_request(
                "/rpc/invoke",
                "hello-w10-tcp",
                {
                    {"x-rpc-service", "svc.tcp-echo"},
                    {"x-rpc-method", "Echo"},
                }
            ),
            &tcp_transport_response)) {
        std::cerr << "tcp transport request failed\n";
        return 1;
    }

    if (tcp_transport_response.status != 200
        || tcp_transport_response.body != "tcp-echo:hello-w10-tcp") {
        std::cerr << "unexpected tcp transport response, status="
                  << tcp_transport_response.status
                  << ", body=" << tcp_transport_response.body << '\n';
        return 1;
    }

    const rpc::client::RpcInvokeAuditSnapshot rpc_trace_tcp = rpc::client::last_invoke_audit_snapshot();
    if (rpc_trace_tcp.endpoint.find(std::to_string(tcp_backend.port())) == std::string::npos) {
        std::cerr << "tcp transport did not hit expected backend endpoint, endpoint="
                  << rpc_trace_tcp.endpoint << '\n';
        return 1;
    }

    const rpc::runtime::RuntimeSchedulerSnapshot runtime_snapshot = rpc::runtime::runtime_scheduler_snapshot();
    if (!runtime_snapshot.running
        || runtime_snapshot.worker_count == 0
        || runtime_snapshot.completed_coroutines == 0) {
        std::cerr << "unexpected runtime scheduler snapshot, running=" << runtime_snapshot.running
                  << ", worker_count=" << runtime_snapshot.worker_count
                  << ", completed=" << runtime_snapshot.completed_coroutines << '\n';
        return 1;
    }

    server.stop();

    std::cout << "w10_gateway_rpc_trace_metrics_test passed"
              << ", rpc_route_error_rate=" << rpc_route->error_rate
              << ", slow_p50_ms=" << slow_route->latency_p50_ms
              << ", trace=" << gateway_trace_b.trace_id
              << '\n';
    return 0;
}
