#pragma once

// 文件用途：
// 声明网络模块初始化入口，用于启动期装配 Socket/Epoll/连接管理相关能力。

namespace rpc::net {

// 初始化网络模块。
// 当前为占位实现，后续可在此初始化监听器、连接池、事件循环线程等组件。
void init_network();

}  // namespace rpc::net
