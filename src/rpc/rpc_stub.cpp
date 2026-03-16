#include "rpc/rpc/client.h"

// 文件用途：
// 提供 W1/W7 阶段 RPC 客户端实现：
// - 保持服务发现/负载均衡抽象可替换
// - 通过 net 层打通超时分层（gateway -> rpc -> net）
// - 集成重试预算基础控制，防止故障时无限重试

#include <atomic>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

#include "rpc/net/net.h"
#include "rpc/runtime/retry_budget.h"

namespace rpc::client {

namespace {

std::size_t parse_metadata_size(
    const RpcRequest& request,
    const char* key,
    std::size_t default_value
) {
    const auto it = request.metadata.find(key);
    if (it == request.metadata.end() || it->second.empty()) {
        return default_value;
    }

    try {
        const unsigned long long parsed = std::stoull(it->second);
        if (parsed > static_cast<unsigned long long>(std::numeric_limits<std::size_t>::max())) {
            return default_value;
        }
        return static_cast<std::size_t>(parsed);
    } catch (...) {
        return default_value;
    }
}

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
        services_.emplace(
            "svc.echo",
            std::vector<ServiceNode>{
                ServiceNode{"a", "10.0.0.1", 8080},
                ServiceNode{"b", "10.0.0.2", 8081},
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
        : discovery_(std::move(discovery)),
          load_balancer_(std::move(load_balancer)),
          retry_budget_(rpc::runtime::RetryBudgetOptions{}) {
        // 客户端必须依赖发现与负载均衡实现。
        if (!discovery_ || !load_balancer_) {
            throw std::invalid_argument("rpc dependencies cannot be null");
        }
    }

    RpcResponse invoke(const RpcRequest& request) override {
        // 入参校验：服务名是必填项。
        if (request.service.empty()) {
            return RpcResponse{400, "service name is empty", {}, 0, 0};
        }

        // 1) 服务发现
        const std::vector<ServiceNode> nodes = discovery_->list_nodes(request.service);
        if (nodes.empty()) {
            return RpcResponse{503, "no available service nodes", {}, 0, 0};
        }

        // 2) 负载均衡选点
        std::size_t selected = load_balancer_->select_node(nodes, request);
        if (selected >= nodes.size()) {
            // 防御式兜底：负载均衡返回非法下标时回退到 0。
            selected = 0;
        }

        const ServiceNode& node = nodes[selected];
        const std::string endpoint = node.host + ":" + std::to_string(node.port);

        // 3) 重试配置：默认不重试，可通过字段或 metadata 显式开启。
        std::size_t max_retries = request.max_retries;
        if (max_retries == 0) {
            max_retries = parse_metadata_size(request, "x-max-retries", 0);
        }
        const std::size_t fail_before_success = parse_metadata_size(
            request,
            "x-fail-before-success",
            0
        );

        // 4) 进入重试窗口前先记录请求基数。
        retry_budget_.record_request();

        RpcResponse last_response{503, "upstream_error", {}, 0, 0};
        for (std::size_t attempt = 1;; ++attempt) {
            if (attempt > 1) {
                if (attempt - 1 > max_retries) {
                    if (last_response.code == 0) {
                        last_response.code = 503;
                        last_response.message = "retry_exhausted";
                    }
                    return last_response;
                }
                if (!retry_budget_.try_acquire_retry_token()) {
                    return RpcResponse{
                        429,
                        "retry_budget_exhausted",
                        {},
                        attempt - 1,
                        last_response.effective_timeout_ms,
                    };
                }
            }

            rpc::net::NetCallRequest net_request;
            net_request.endpoint = endpoint;
            net_request.payload = request.payload;
            net_request.downstream_timeout_ms = request.timeout_ms;
            net_request.attempt = attempt;
            net_request.fail_before_success = fail_before_success;

            const rpc::net::NetCallResponse net_response = rpc::net::invoke_stub(net_request);
            if (net_response.code == 0) {
                return RpcResponse{
                    0,
                    "ok",
                    net_response.payload,
                    attempt,
                    net_response.effective_timeout_ms,
                };
            }

            last_response = RpcResponse{
                net_response.code,
                net_response.message,
                net_response.payload,
                attempt,
                net_response.effective_timeout_ms,
            };

            if (!net_response.retryable) {
                return last_response;
            }
        }
    }

private:
    std::shared_ptr<IServiceDiscovery> discovery_;
    std::shared_ptr<ILoadBalancer> load_balancer_;
    rpc::runtime::RetryBudget retry_budget_;
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
