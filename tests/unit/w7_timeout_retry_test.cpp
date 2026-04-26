#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "rpc/gateway/gateway.h"
#include "rpc/infra/infra.h"
#include "rpc/net/net.h"
#include "rpc/rpc/client.h"
#include "rpc/runtime/runtime.h"

// 文件用途：
// 验证 W7 两条验收标准：
// 1) 上游 deadline 小于下游 timeout 配置时，按剩余预算生效
// 2) 压测下不会发生无限重试（由重试预算控制）

namespace {

bool ends_with(const std::string& value, const std::string& suffix) {
    if (suffix.size() > value.size()) {
        return false;
    }
    return value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

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
            (void)::send(
                client_fd,
                response.data(),
                response.size(),
                MSG_NOSIGNAL
            );
            ::close(client_fd);
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

}  // namespace

int main() {
    using namespace std::chrono_literals;

    rpc::infra::init_infra();
    rpc::runtime::init_runtime();
    rpc::net::init_network();
    rpc::gateway::init_gateway();

    ScopedEchoBackend backend;
    if (!backend.start()) {
        if (errno == EPERM || errno == EACCES) {
            std::cout << "w7_timeout_retry_test skipped: tcp backend bind/listen not permitted\n";
            return 0;
        }
        std::cerr << "failed to start local tcp backend\n";
        return 1;
    }

    rpc::client::ClientInitOptions client_options;
    client_options.discovery = std::make_shared<StaticDiscovery>(
        std::unordered_map<std::string, std::vector<rpc::client::ServiceNode>>{
            {"gateway.backend", {rpc::client::ServiceNode{"ok", "127.0.0.1", backend.port()}}},
            {"gateway.unreachable", {rpc::client::ServiceNode{"bad", "127.0.0.1", 1}}},
        }
    );
    client_options.load_balancer = std::make_shared<FirstNodeLoadBalancer>();
    rpc::client::init_client(std::move(client_options));

    // 验收 1：上游 deadline(120ms) < 下游 timeout(500ms) 时，应使用剩余预算。
    rpc::client::RpcRequest timeout_request;
    timeout_request.service = "gateway.backend";
    timeout_request.method = "Echo";
    timeout_request.payload = "budget-check";
    timeout_request.timeout_ms = 500;

    const rpc::client::RpcResponse timeout_response = rpc::gateway::invoke_with_deadline(
        timeout_request,
        120ms,
        "w7-timeout-case"
    );

    if (timeout_response.code != 0) {
        std::cerr << "timeout layering case failed, code=" << timeout_response.code
                  << ", message=" << timeout_response.message << '\n';
        return 1;
    }

    if (timeout_response.effective_timeout_ms == 0 || timeout_response.effective_timeout_ms > 500
        || timeout_response.effective_timeout_ms > 150) {
        std::cerr << "unexpected effective timeout, response_effective="
                  << timeout_response.effective_timeout_ms
                  << ", net_last=" << rpc::net::last_effective_timeout_ms() << '\n';
        return 1;
    }

    if (timeout_response.effective_timeout_ms != rpc::net::last_effective_timeout_ms()) {
        std::cerr << "effective timeout mismatch between rpc/net layers"
                  << ", rpc=" << timeout_response.effective_timeout_ms
                  << ", net=" << rpc::net::last_effective_timeout_ms() << '\n';
        return 1;
    }

    if (!ends_with(timeout_response.payload, "budget-check")) {
        std::cerr << "unexpected payload in timeout layering case: " << timeout_response.payload << '\n';
        return 1;
    }

    // 验收 2：压测下无无限重试（重试预算生效）。
    rpc::client::RpcRequest retry_request;
    retry_request.service = "gateway.unreachable";
    retry_request.method = "RetryCase";
    retry_request.payload = "force-fail";
    retry_request.timeout_ms = 400;
    retry_request.max_retries = 100000; // 故意配置很大，验证预算会兜底

    constexpr int kCalls = 200;
    std::size_t max_attempts = 0;
    std::size_t total_attempts = 0;
    std::size_t budget_exhausted = 0;

    const auto stress_start = std::chrono::steady_clock::now();
    for (int i = 0; i < kCalls; ++i) {
        const rpc::client::RpcResponse response = rpc::gateway::invoke_with_deadline(
            retry_request,
            1000ms,
            "w7-retry-case-" + std::to_string(i)
        );

        if (response.code != 429 && response.code != 503 && response.code != 504) {
            std::cerr << "unexpected response code in retry stress case, code=" << response.code
                      << ", message=" << response.message << '\n';
            return 1;
        }

        if (response.code == 429) {
            ++budget_exhausted;
        }

        max_attempts = std::max(max_attempts, response.attempts);
        total_attempts += response.attempts;
    }
    const auto stress_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - stress_start
    ).count();

    if (budget_exhausted == 0) {
        std::cerr << "retry budget did not trigger under stress\n";
        return 1;
    }

    if (max_attempts > 80) {
        std::cerr << "retry attempts too large, possible runaway retry, max_attempts="
                  << max_attempts << '\n';
        return 1;
    }

    if (stress_elapsed_ms > 4000) {
        std::cerr << "retry stress case took too long, possible retry runaway, elapsed_ms="
                  << stress_elapsed_ms << '\n';
        return 1;
    }

    std::cout << "w7_timeout_retry_test passed"
              << ", timeout_effective_ms=" << timeout_response.effective_timeout_ms
              << ", budget_exhausted=" << budget_exhausted
              << ", max_attempts=" << max_attempts
              << ", total_attempts=" << total_attempts
              << ", stress_elapsed_ms=" << stress_elapsed_ms
              << '\n';

    return 0;
}
