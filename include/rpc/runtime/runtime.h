#pragma once

// 文件用途：
// 声明运行时模块初始化入口，用于启动协程调度器、定时器、Hook 等运行时基础能力。

namespace rpc::runtime {

// 初始化运行时模块。
// 当前为占位实现，后续可在此创建调度器实例并绑定线程上下文。
void init_runtime();

}  // namespace rpc::runtime
