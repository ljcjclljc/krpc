#include <iostream>
#include <cerrno>
#include <memory>
#include <string_view>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

// 文件用途：
// 验证 W1 RPC 抽象层的可替换性：
// 1) 可注入自定义服务发现实现
// 2) 可注入自定义负载均衡实现
// 3) 统一 invoke 接口在正常与异常场景下行为正确

#include "rpc/rpc/client.h"

namespace {

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
            while (true) {
                sockaddr_in peer{};
                socklen_t peer_len = static_cast<socklen_t>(sizeof(peer));
                const int client_fd = ::accept(
                    listen_fd_,
                    reinterpret_cast<sockaddr*>(&peer),
                    &peer_len
                );
                if (client_fd < 0) {
                    break;
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
                (void)::send(client_fd, response.data(), response.size(), MSG_NOSIGNAL);
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

class TestDiscovery final : public rpc::client::IServiceDiscovery {
public:
    explicit TestDiscovery(std::uint16_t backend_port) : backend_port_(backend_port) {}

    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override {
        // 仅为 svc.echo 返回可用节点，其他服务返回空。
        if (service_name != "svc.echo") {
            return {};
        }
        return {
            rpc::client::ServiceNode{"a", "127.0.0.1", 1},
            rpc::client::ServiceNode{"b", "127.0.0.1", backend_port_},
        };
    }

private:
    std::uint16_t backend_port_{0};
};

class LastNodeLoadBalancer final : public rpc::client::ILoadBalancer {
public:
    std::size_t select_node(
        const std::vector<rpc::client::ServiceNode>& nodes,
        const rpc::client::RpcRequest& /*request*/
    ) override {
        if (nodes.empty()) {
            return 0;
        }
        // 测试策略：始终选择最后一个节点，便于断言结果。
        return nodes.size() - 1;
    }
};

}  // namespace

int main() {
    ScopedEchoBackend backend;
    if (!backend.start()) {
        if (errno == EPERM || errno == EACCES) {
            std::cout << "rpc_abstraction_test skipped: tcp backend bind/listen not permitted\n";
            return 0;
        }
        std::cerr << "failed to start local tcp backend\n";
        return 1;
    }

    // 注入测试版服务发现与负载均衡实现。
    rpc::client::ClientInitOptions options;
    options.discovery = std::make_shared<TestDiscovery>(backend.port());
    options.load_balancer = std::make_shared<LastNodeLoadBalancer>();
    rpc::client::init_client(std::move(options));

    const auto client = rpc::client::default_client();
    if (!client) {
        std::cerr << "default client is null\n";
        return 1;
    }

    // 正常请求：应命中最后一个节点（127.0.0.1:backend_port）。
    rpc::client::RpcRequest request;
    request.service = "svc.echo";
    request.method = "Echo";
    request.payload = "ping";
    request.timeout_ms = 100;

    const rpc::client::RpcResponse response = client->invoke(request);
    if (response.code != 0) {
        std::cerr << "invoke failed, code=" << response.code << ", message=" << response.message << '\n';
        return 1;
    }

    if (response.payload != "tcp-echo:ping") {
        std::cerr << "unexpected payload: " << response.payload << '\n';
        return 1;
    }

    // 服务名缺失：应返回 400。
    rpc::client::RpcRequest missing_service;
    const rpc::client::RpcResponse bad_request = client->invoke(missing_service);
    if (bad_request.code != 400) {
        std::cerr << "expected bad request code 400, got " << bad_request.code << '\n';
        return 1;
    }

    // 服务不存在：应返回 503。
    rpc::client::RpcRequest unknown_service;
    unknown_service.service = "svc.unknown";
    const rpc::client::RpcResponse unavailable = client->invoke(unknown_service);
    if (unavailable.code != 503) {
        std::cerr << "expected unavailable code 503, got " << unavailable.code << '\n';
        return 1;
    }

    std::cout << "rpc_abstraction_test passed\n";
    return 0;
}
