#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rpc/rpc/client.h"

namespace {

using namespace std::chrono_literals;

class ToggleEchoBackend final {
public:
    explicit ToggleEchoBackend(std::string name) : name_(std::move(name)) {}

    ~ToggleEchoBackend() {
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
        if (::listen(listen_fd_, 64) != 0) {
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

        running_.store(true, std::memory_order_release);
        worker_ = std::thread([this]() { this->run_loop(); });
        return true;
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    void set_fail_mode(bool fail_mode) {
        fail_mode_.store(fail_mode, std::memory_order_release);
    }

    std::uint16_t port() const noexcept {
        return port_;
    }

private:
    void run_loop() {
        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in peer{};
            socklen_t peer_len = static_cast<socklen_t>(sizeof(peer));
            const int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
            if (client_fd < 0) {
                if (!running_.load(std::memory_order_acquire)) {
                    return;
                }
                if (errno == EINTR) {
                    continue;
                }
                return;
            }

            handle_client(client_fd);
            ::close(client_fd);
        }
    }

    void handle_client(int client_fd) {
        char buffer[4096];
        std::string payload;
        while (true) {
            const ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
            if (n > 0) {
                payload.append(buffer, static_cast<std::size_t>(n));
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
            return;
        }

        if (fail_mode_.load(std::memory_order_acquire)) {
            return;
        }

        const std::string response = "echo-" + name_ + ":" + payload;
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
            return;
        }
    }

    std::string name_;
    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::atomic<bool> running_{false};
    std::atomic<bool> fail_mode_{false};
    std::thread worker_;
};

class W13Discovery final : public rpc::client::IServiceDiscovery {
public:
    W13Discovery(
        std::uint16_t stable_port,
        std::uint16_t gray_port,
        std::uint16_t flaky_port,
        std::uint16_t good_port
    ) : stable_port_(stable_port),
        gray_port_(gray_port),
        flaky_port_(flaky_port),
        good_port_(good_port) {}

    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override {
        if (service_name == "svc.adaptive") {
            return {
                rpc::client::ServiceNode{
                    "stable-a",
                    "127.0.0.1",
                    stable_port_,
                    {{"lane", "stable"}, {"cluster", "prod"}},
                    0.16,
                    0.22,
                    58.0,
                    14.0,
                },
                rpc::client::ServiceNode{
                    "gray-b",
                    "127.0.0.1",
                    gray_port_,
                    {{"lane", "gray"}, {"cluster", "prod"}},
                    0.86,
                    0.84,
                    420.0,
                    180.0,
                },
            };
        }

        if (service_name == "svc.circuit") {
            return {
                rpc::client::ServiceNode{
                    "bad-node",
                    "127.0.0.1",
                    flaky_port_,
                    {{"lane", "stable"}},
                    0.08,
                    0.10,
                    10.0,
                    5.0,
                },
                rpc::client::ServiceNode{
                    "good-node",
                    "127.0.0.1",
                    good_port_,
                    {{"lane", "stable"}},
                    0.64,
                    0.62,
                    220.0,
                    95.0,
                },
            };
        }

        if (service_name == "svc.degrade") {
            return {
                rpc::client::ServiceNode{
                    "flaky-only",
                    "127.0.0.1",
                    flaky_port_,
                    {{"lane", "stable"}},
                    0.32,
                    0.30,
                    80.0,
                    18.0,
                },
            };
        }

        return {};
    }

private:
    std::uint16_t stable_port_{0};
    std::uint16_t gray_port_{0};
    std::uint16_t flaky_port_{0};
    std::uint16_t good_port_{0};
};

rpc::client::RpcRequest make_request(
    std::string service,
    std::string payload,
    std::unordered_map<std::string, std::string> metadata = {}
) {
    rpc::client::RpcRequest request;
    request.service = std::move(service);
    request.method = "Echo";
    request.payload = std::move(payload);
    request.timeout_ms = 300;
    request.max_retries = 0;
    request.metadata = std::move(metadata);
    return request;
}

std::string endpoint_of(std::uint16_t port) {
    return "127.0.0.1:" + std::to_string(port);
}

}  // namespace

int main() {
    int probe_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (probe_fd < 0 && (errno == EPERM || errno == EACCES)) {
        std::cout << "w13_adaptive_lb_circuit_breaker_gray_test skipped: socket operations are not permitted\n";
        return 0;
    }
    if (probe_fd >= 0) {
        ::close(probe_fd);
    }

    ToggleEchoBackend stable_backend("stable");
    ToggleEchoBackend gray_backend("gray");
    ToggleEchoBackend flaky_backend("flaky");
    ToggleEchoBackend good_backend("good");

    if (!stable_backend.start() || !gray_backend.start() || !flaky_backend.start() || !good_backend.start()) {
        std::cerr << "failed to start local backends for w13 test\n";
        return 1;
    }

    rpc::client::ClientInitOptions options;
    options.discovery = std::make_shared<W13Discovery>(
        stable_backend.port(),
        gray_backend.port(),
        flaky_backend.port(),
        good_backend.port()
    );
    rpc::client::init_client(std::move(options));

    const auto client = rpc::client::default_client();
    if (!client) {
        std::cerr << "default client not initialized\n";
        return 1;
    }

    // 验收 1.1：实时状态加权后流量向低负载节点收敛。
    std::size_t stable_hits = 0;
    std::size_t gray_hits = 0;
    for (int i = 0; i < 40; ++i) {
        rpc::client::RpcRequest request = make_request(
            "svc.adaptive",
            "adaptive-" + std::to_string(i),
            {{"x-fallback-cache", "0"}}
        );

        const rpc::client::RpcResponse response = client->invoke(request);
        if (response.code != 0) {
            std::cerr << "adaptive convergence call failed, code=" << response.code
                      << ", message=" << response.message << '\n';
            return 1;
        }
        if (response.selected_endpoint == endpoint_of(stable_backend.port())) {
            ++stable_hits;
        }
        if (response.selected_endpoint == endpoint_of(gray_backend.port())) {
            ++gray_hits;
        }
    }

    if (stable_hits < 30) {
        std::cerr << "adaptive convergence not observed, stable_hits=" << stable_hits
                  << ", gray_hits=" << gray_hits << '\n';
        return 1;
    }

    // 验收 1.2：灰度发布与流量染色（标签/用户/比例）生效。
    {
        rpc::client::RpcRequest gray_by_user = make_request(
            "svc.adaptive",
            "gray-user",
            {
                {"x-gray-percent", "100"},
                {"x-user-id", "u-100"},
                {"x-gray-tag", "gray"},
            }
        );
        const rpc::client::RpcResponse response = client->invoke(gray_by_user);
        if (response.code != 0 || response.selected_endpoint != endpoint_of(gray_backend.port())) {
            std::cerr << "gray ratio/user route failed, code=" << response.code
                      << ", endpoint=" << response.selected_endpoint << '\n';
            return 1;
        }
    }
    {
        rpc::client::RpcRequest stable_by_tag = make_request(
            "svc.adaptive",
            "stable-tag",
            {{"x-traffic-tag", "stable"}}
        );
        const rpc::client::RpcResponse response = client->invoke(stable_by_tag);
        if (response.code != 0 || response.selected_endpoint != endpoint_of(stable_backend.port())) {
            std::cerr << "stable tag route failed, code=" << response.code
                      << ", endpoint=" << response.selected_endpoint << '\n';
            return 1;
        }
    }

    // 验收 1.3：故障节点可自动熔断，流量切走到健康节点。
    flaky_backend.set_fail_mode(true);
    std::size_t circuit_failures = 0;
    std::size_t failover_success = 0;
    for (int i = 0; i < 6; ++i) {
        rpc::client::RpcRequest request = make_request(
            "svc.circuit",
            "circuit-" + std::to_string(i),
            {
                {"x-cb-failure-threshold", "2"},
                {"x-cb-open-ms", "180"},
                {"x-fallback-cache", "0"},
            }
        );
        const rpc::client::RpcResponse response = client->invoke(request);
        if (response.code != 0) {
            ++circuit_failures;
        } else if (response.selected_endpoint == endpoint_of(good_backend.port())) {
            ++failover_success;
        }
    }

    if (circuit_failures == 0 || failover_success == 0) {
        std::cerr << "circuit breaker auto-failover not observed"
                  << ", failures=" << circuit_failures
                  << ", failover_success=" << failover_success << '\n';
        return 1;
    }

    // 验收 2.1：故障时自动降级（缓存兜底 + 静态兜底）。
    flaky_backend.set_fail_mode(false);
    {
        rpc::client::RpcRequest seed = make_request(
            "svc.degrade",
            "seed",
            {
                {"x-fallback-cache", "1"},
                {"x-fallback-cache-key", "w13-cache-key"},
            }
        );
        const rpc::client::RpcResponse seeded = client->invoke(seed);
        if (seeded.code != 0) {
            std::cerr << "cache seed request failed, code=" << seeded.code
                      << ", message=" << seeded.message << '\n';
            return 1;
        }
    }

    flaky_backend.set_fail_mode(true);
    {
        rpc::client::RpcRequest cache_fallback = make_request(
            "svc.degrade",
            "seed",
            {
                {"x-cb-failure-threshold", "1"},
                {"x-cb-open-ms", "120"},
                {"x-fallback-cache", "1"},
                {"x-fallback-cache-key", "w13-cache-key"},
            }
        );
        const rpc::client::RpcResponse response = client->invoke(cache_fallback);
        if (response.code != 0 || !response.degraded || response.degrade_strategy != "cache") {
            std::cerr << "cache fallback failed, code=" << response.code
                      << ", degraded=" << (response.degraded ? "true" : "false")
                      << ", strategy=" << response.degrade_strategy
                      << ", message=" << response.message << '\n';
            return 1;
        }
    }

    {
        rpc::client::RpcRequest static_fallback = make_request(
            "svc.degrade",
            "static-case",
            {
                {"x-cb-failure-threshold", "1"},
                {"x-cb-open-ms", "120"},
                {"x-fallback-cache", "0"},
                {"x-fallback-static", "static-fallback-body"},
            }
        );
        const rpc::client::RpcResponse response = client->invoke(static_fallback);
        if (response.code != 0 || !response.degraded || response.degrade_strategy != "static"
            || response.payload != "static-fallback-body") {
            std::cerr << "static fallback failed, code=" << response.code
                      << ", degraded=" << (response.degraded ? "true" : "false")
                      << ", strategy=" << response.degrade_strategy
                      << ", payload=" << response.payload << '\n';
            return 1;
        }
    }

    // 验收 2.2：半开探测后快速恢复。
    std::this_thread::sleep_for(180ms);
    flaky_backend.set_fail_mode(false);

    rpc::client::RpcRequest recovery = make_request(
        "svc.degrade",
        "recover",
        {
            {"x-cb-failure-threshold", "1"},
            {"x-cb-open-ms", "120"},
            {"x-cb-half-open-success", "1"},
            {"x-fallback-cache", "0"},
        }
    );

    const rpc::client::RpcResponse recovered = client->invoke(recovery);
    if (recovered.code != 0 || recovered.degraded) {
        std::cerr << "half-open recovery failed, code=" << recovered.code
                  << ", degraded=" << (recovered.degraded ? "true" : "false")
                  << ", message=" << recovered.message << '\n';
        return 1;
    }

    const rpc::client::LoadBalancerDecisionSnapshot lb_snapshot =
        rpc::client::last_load_balancer_decision_snapshot();
    if (!lb_snapshot.has_selection) {
        std::cerr << "unexpected empty load balancer decision snapshot after recovery\n";
        return 1;
    }

    std::cout << "w13_adaptive_lb_circuit_breaker_gray_test passed"
              << ", stable_hits=" << stable_hits
              << ", gray_hits=" << gray_hits
              << ", circuit_failures=" << circuit_failures
              << ", failover_success=" << failover_success
              << ", recovery_endpoint=" << recovered.selected_endpoint
              << '\n';

    return 0;
}
