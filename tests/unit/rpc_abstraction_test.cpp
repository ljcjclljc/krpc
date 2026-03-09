#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// 文件用途：
// 验证 W1 RPC 抽象层的可替换性：
// 1) 可注入自定义服务发现实现
// 2) 可注入自定义负载均衡实现
// 3) 统一 invoke 接口在正常与异常场景下行为正确

#include "rpc/rpc/client.h"

namespace {

class TestDiscovery final : public rpc::client::IServiceDiscovery {
public:
    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override {
        // 仅为 svc.echo 返回可用节点，其他服务返回空。
        if (service_name != "svc.echo") {
            return {};
        }
        return {
            rpc::client::ServiceNode{"a", "10.0.0.1", 8080},
            rpc::client::ServiceNode{"b", "10.0.0.2", 8081},
        };
    }
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
    // 注入测试版服务发现与负载均衡实现。
    rpc::client::ClientInitOptions options;
    options.discovery = std::make_shared<TestDiscovery>();
    options.load_balancer = std::make_shared<LastNodeLoadBalancer>();
    rpc::client::init_client(std::move(options));

    const auto client = rpc::client::default_client();
    if (!client) {
        std::cerr << "default client is null\n";
        return 1;
    }

    // 正常请求：应命中最后一个节点（10.0.0.2:8081）。
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

    if (response.payload != "10.0.0.2:8081:ping") {
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
