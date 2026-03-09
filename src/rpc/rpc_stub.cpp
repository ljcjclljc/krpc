#include "rpc/rpc/client.h"

// 文件用途：
// 提供 W1 阶段的最小 RPC 客户端实现，用于验证抽象层可用性：
// - 服务发现接口可替换
// - 负载均衡接口可替换
// - 客户端调用入口统一

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace rpc::client {

namespace {

class InMemoryServiceDiscovery final : public IServiceDiscovery {
public:
    InMemoryServiceDiscovery() {
        // 预置一组本地节点，便于开发阶段直接联调。
        services_.emplace(
            "gateway.backend",
            std::vector<ServiceNode>{
                ServiceNode{"node-a", "127.0.0.1", 9000},
                ServiceNode{"node-b", "127.0.0.1", 9001},
            }
        );
    }

    std::vector<ServiceNode> list_nodes(const std::string& service_name) override {
        // 按服务名返回节点列表；不存在时返回空集合。
        const auto it = services_.find(service_name);
        if (it == services_.end()) {
            return {};
        }
        return it->second;
    }

private:
    std::unordered_map<std::string, std::vector<ServiceNode>> services_;
};

class RoundRobinLoadBalancer final : public ILoadBalancer {
public:
    std::size_t select_node(
        const std::vector<ServiceNode>& nodes,
        const RpcRequest& /*request*/
    ) override {
        if (nodes.empty()) {
            // 上层会再做兜底保护，这里返回 0 仅保持接口约定。
            return 0;
        }
        // 原子计数器轮询取模，完成简单均衡。
        const std::size_t counter = next_.fetch_add(1, std::memory_order_relaxed);
        return counter % nodes.size();
    }

private:
    std::atomic<std::size_t> next_{0};
};

class DefaultRpcClient final : public IRpcClient {
public:
    DefaultRpcClient(
        std::shared_ptr<IServiceDiscovery> discovery,
        std::shared_ptr<ILoadBalancer> load_balancer
    )
        : discovery_(std::move(discovery)), load_balancer_(std::move(load_balancer)) {
        // 客户端必须依赖发现与负载均衡实现。
        if (!discovery_ || !load_balancer_) {
            throw std::invalid_argument("rpc dependencies cannot be null");
        }
    }

    RpcResponse invoke(const RpcRequest& request) override {
        // 入参校验：服务名是必填项。
        if (request.service.empty()) {
            return RpcResponse{400, "service name is empty", {}};
        }

        // 1) 服务发现
        const std::vector<ServiceNode> nodes = discovery_->list_nodes(request.service);
        if (nodes.empty()) {
            return RpcResponse{503, "no available service nodes", {}};
        }

        // 2) 负载均衡选点
        std::size_t selected = load_balancer_->select_node(nodes, request);
        if (selected >= nodes.size()) {
            // 防御式兜底：负载均衡返回非法下标时回退到 0。
            selected = 0;
        }

        // 3) 当前为最小实现：返回“节点地址 + 请求载荷”模拟调用结果。
        const ServiceNode& node = nodes[selected];
        RpcResponse response;
        response.code = 0;
        response.message = "ok";
        response.payload = node.host + ":" + std::to_string(node.port) + ":" + request.payload;
        return response;
    }

private:
    std::shared_ptr<IServiceDiscovery> discovery_;
    std::shared_ptr<ILoadBalancer> load_balancer_;
};

std::mutex g_client_mutex;
std::shared_ptr<IRpcClient> g_default_client;

}  // namespace

void init_client(ClientInitOptions options) {
    // 全局默认客户端初始化过程需要互斥保护。
    std::lock_guard<std::mutex> lock(g_client_mutex);

    if (!options.discovery) {
        // 未注入时使用内存版服务发现。
        options.discovery = std::make_shared<InMemoryServiceDiscovery>();
    }
    if (!options.load_balancer) {
        // 未注入时使用轮询负载均衡。
        options.load_balancer = std::make_shared<RoundRobinLoadBalancer>();
    }

    g_default_client = std::make_shared<DefaultRpcClient>(
        std::move(options.discovery),
        std::move(options.load_balancer)
    );
}

std::shared_ptr<IRpcClient> default_client() {
    // 快路径：若已初始化直接返回。
    {
        std::lock_guard<std::mutex> lock(g_client_mutex);
        if (g_default_client) {
            return g_default_client;
        }
    }

    // 懒加载初始化：保证即使调用方忘记 init，也能拿到默认客户端。
    init_client();

    std::lock_guard<std::mutex> lock(g_client_mutex);
    return g_default_client;
}

}  // namespace rpc::client
