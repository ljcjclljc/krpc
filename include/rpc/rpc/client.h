#pragma once

// 文件用途：
// 定义 RPC 客户端抽象层接口，用于隔离网关与具体服务发现、负载均衡、
// 传输实现之间的耦合关系。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rpc/runtime/retry_budget.h"

namespace rpc::client {

// 一次 RPC 调用请求模型。
struct RpcRequest {
    // 目标服务名，例如 "user.service"。
    std::string service;

    // 目标方法名，例如 "GetUser"。
    std::string method;

    // 请求体（当前示例中使用字符串承载，后续可替换为 Protobuf 二进制）。
    std::string payload;

    // 扩展元数据（TraceId、鉴权信息等）。
    std::unordered_map<std::string, std::string> metadata;

    // 请求超时时间（毫秒），0 表示未设置。
    std::uint64_t timeout_ms{0};

    // 最大重试次数（不含首发）。
    std::size_t max_retries{0};
};

// 一次 RPC 调用响应模型。
struct RpcResponse {
    // 业务/框架状态码：0 表示成功，非 0 表示失败。
    int code{0};

    // 错误或状态说明。
    std::string message;

    // 响应数据载荷。
    std::string payload;

    // 本次调用总尝试次数（含首发）。
    std::size_t attempts{0};

    // 本次生效的 net 超时预算（毫秒）。
    std::uint64_t effective_timeout_ms{0};

    // 请求是否被取消（主动取消或 deadline 取消）。
    bool cancelled{false};

    // 取消原因（未取消时为空）。
    std::string cancel_reason;

    // 重试预算窗口观测信息。
    std::size_t retry_budget_request_count{0};
    std::size_t retry_budget_retry_count{0};
    std::size_t retry_budget_max_tokens{0};
    std::size_t retry_budget_available_tokens{0};

    // 是否触发降级路径。
    bool degraded{false};

    // 降级策略：cache/static/none。
    std::string degrade_strategy;

    // 选中的上游节点标识（host:port）。
    std::string selected_endpoint;

    // 流量染色命中标签（例如 stable/gray）。
    std::string traffic_lane;

    // 选中节点调用时的熔断状态。
    std::string circuit_state;
};

enum class CircuitBreakerState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
};

// 最近一次负载均衡决策快照（用于 W13 验收可观测）。
struct LoadBalancerDecisionSnapshot {
    bool has_selection{false};
    bool blocked_by_circuit_breaker{false};
    bool gray_routed{false};
    std::string selected_node_id;
    std::string selected_endpoint;
    std::string traffic_lane;
    CircuitBreakerState circuit_state{CircuitBreakerState::Closed};
    double score{0.0};
};

struct LoadBalancerRuntimeStats {
    std::size_t select_calls{0};
    std::size_t blocked_by_circuit_breaker{0};
    std::size_t gray_routed{0};
    std::size_t feedback_calls{0};
    std::uint64_t select_lock_wait_ns_total{0};
    std::uint64_t feedback_lock_wait_ns_total{0};
    std::unordered_map<std::string, std::size_t> selected_endpoint_counts;
};

// 最近一次 RPC 调用审计快照（用于链路透传与日志关联验收）。
struct RpcInvokeAuditSnapshot {
    std::string trace_id;
    std::string span_id;
    std::string service;
    std::string method;
    std::string endpoint;
    int code{0};
    std::string message;
    std::size_t attempts{0};
    bool degraded{false};
    std::string degrade_strategy;
    std::string traffic_lane;
    std::string circuit_state;
};

// 服务节点模型（服务发现结果）。
struct ServiceNode {
    // 节点唯一 ID。
    std::string id;

    // 节点地址。
    std::string host;

    // 节点端口。
    std::uint16_t port{0};

    // 节点标签（用于灰度/染色/分组）。
    std::unordered_map<std::string, std::string> labels;

    // 采集上报的实时状态（[0,1] 或实际值）；-1 表示未知。
    double cpu_utilization{-1.0};
    double memory_utilization{-1.0};
    double qps{-1.0};
    double latency_ms{-1.0};
};

// 服务发现抽象接口：
// 输入服务名，输出可用节点列表。
class IServiceDiscovery {
public:
    virtual ~IServiceDiscovery() = default;
    virtual std::vector<ServiceNode> list_nodes(const std::string& service_name) = 0;
};

// 负载均衡抽象接口：
// 根据节点列表和请求上下文，返回被选中的节点下标。
class ILoadBalancer {
public:
    virtual ~ILoadBalancer() = default;
    virtual std::size_t select_node(
        const std::vector<ServiceNode>& nodes,
        const RpcRequest& request
    ) = 0;
};

// 负载均衡运行时反馈接口：
// 用于接收调用结果，驱动实时状态收敛（熔断/恢复/负载因子更新）。
class ILoadBalancerRuntimeFeedback {
public:
    virtual ~ILoadBalancerRuntimeFeedback() = default;
    virtual void on_invoke_result(
        const ServiceNode& node,
        const RpcRequest& request,
        const RpcResponse& response,
        std::uint64_t observed_latency_ms
    ) = 0;
    virtual LoadBalancerDecisionSnapshot last_decision_snapshot() const = 0;
};

// RPC 客户端抽象接口：
// 上层统一通过 invoke 发起调用，不感知底层策略实现。
class IRpcClient {
public:
    virtual ~IRpcClient() = default;
    virtual RpcResponse invoke(const RpcRequest& request) = 0;
};

// 客户端初始化可注入项：
// 若为空则使用默认内存实现（便于本地开发验证）。
struct ClientInitOptions {
    std::shared_ptr<IServiceDiscovery> discovery;
    std::shared_ptr<ILoadBalancer> load_balancer;
    rpc::runtime::RetryBudgetOptions retry_budget_options{};
};

// 初始化全局默认客户端实例。
void init_client(ClientInitOptions options = {});

// 获取全局默认客户端实例。
std::shared_ptr<IRpcClient> default_client();

// 读取最近一次 RPC 调用审计快照。
RpcInvokeAuditSnapshot last_invoke_audit_snapshot();

// 读取最近一次负载均衡决策快照。
LoadBalancerDecisionSnapshot last_load_balancer_decision_snapshot();

// 读取负载均衡运行态剖面快照。
LoadBalancerRuntimeStats load_balancer_runtime_stats();

}  // namespace rpc::client
