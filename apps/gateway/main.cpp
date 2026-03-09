#include <iostream>

// 文件用途：
// 网关应用主入口，负责按依赖顺序初始化各模块并输出启动信息。

#include "rpc/common/version.h"
#include "rpc/gateway/gateway.h"
#include "rpc/infra/infra.h"
#include "rpc/net/net.h"
#include "rpc/rpc/client.h"
#include "rpc/runtime/runtime.h"

int main() {
    // 初始化顺序说明：
    // 1) infra: 日志/配置/监控等基础设施
    // 2) runtime: 协程和调度运行时
    // 3) net: 网络层
    // 4) gateway: HTTP 网关层
    // 5) client: RPC 客户端层
    rpc::infra::init_infra();
    rpc::runtime::init_runtime();
    rpc::net::init_network();
    rpc::gateway::init_gateway();
    rpc::client::init_client();

    // 输出版本号，便于日志中定位构建版本。
    std::cout << "rpc_gateway_app started, version=" << rpc::common::version() << '\n';
    return 0;
}
