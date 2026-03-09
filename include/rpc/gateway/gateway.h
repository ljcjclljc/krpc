#pragma once

// 文件用途：
// 声明 gateway 模块的初始化入口，用于应用启动阶段完成网关层组件装配。

namespace rpc::gateway {

// 初始化网关模块。
// 当前为占位实现，后续可在此接入路由表、Servlet 注册、HTTP 配置加载等逻辑。
void init_gateway();

}  // namespace rpc::gateway
