#pragma once

// 文件用途：
// 声明 gateway 模块的初始化入口，用于应用启动阶段完成网关层组件装配。

#include <chrono>
#include <string>

#include "rpc/rpc/client.h"

namespace rpc::gateway {

// 初始化网关模块。
// 当前为占位实现，后续可在此接入路由表、Servlet 注册、HTTP 配置加载等逻辑。
void init_gateway();

// 模拟网关入口：创建上游 deadline 上下文，并转发到 RPC 层调用。
rpc::client::RpcResponse invoke_with_deadline(
    rpc::client::RpcRequest request,
    std::chrono::milliseconds upstream_timeout,
    std::string request_id = "gateway-request"
);

}  // namespace rpc::gateway
