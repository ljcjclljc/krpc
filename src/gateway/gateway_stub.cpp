#include "rpc/gateway/gateway.h"

// 文件用途：
// gateway 模块最小实现：
// - 保留初始化入口
// - 提供带 deadline 的网关转发入口，验证 gateway -> rpc -> net 超时传递

#include <utility>

#include "rpc/runtime/runtime.h"

namespace rpc::gateway {

void init_gateway() {
    // 当前阶段无实际初始化逻辑，保留空实现用于打通启动链路。
}

rpc::client::RpcResponse invoke_with_deadline(
    rpc::client::RpcRequest request,
    std::chrono::milliseconds upstream_timeout,
    std::string request_id
) {
    const auto context = rpc::runtime::create_deadline_context(
        std::move(request_id),
        upstream_timeout
    );
    rpc::runtime::ScopedDeadlineContext scoped_context(context);
    return rpc::client::default_client()->invoke(request);
}

}  // namespace rpc::gateway
