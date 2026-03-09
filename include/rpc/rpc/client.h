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
};

// 一次 RPC 调用响应模型。
struct RpcResponse {
    // 业务/框架状态码：0 表示成功，非 0 表示失败。
    int code{0};

    // 错误或状态说明。
    std::string message;

    // 响应数据载荷。
    std::string payload;
};

// 服务节点模型（服务发现结果）。
struct ServiceNode {
    // 节点唯一 ID。
    std::string id;

    // 节点地址。
    std::string host;

    // 节点端口。
    std::uint16_t port{0};
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
};

// 初始化全局默认客户端实例。
void init_client(ClientInitOptions options = {});

// 获取全局默认客户端实例。
std::shared_ptr<IRpcClient> default_client();

}  // namespace rpc::client
