#pragma once

// 文件用途：
// 声明运行时 Hook 能力：TLS 开关控制与 IOManager 绑定入口。

#include <cstdint>

namespace rpc::runtime {

class IOManager;

// 设置当前线程 Hook 开关（TLS）。
void set_hook_enabled(bool enabled) noexcept;

// 获取当前线程 Hook 开关（TLS）。
bool hook_enabled() noexcept;

// 作用域开关：构造时设置，析构时恢复。
class ScopedHookEnable {
public:
    explicit ScopedHookEnable(bool enabled) noexcept;
    ~ScopedHookEnable();

    ScopedHookEnable(const ScopedHookEnable&) = delete;
    ScopedHookEnable& operator=(const ScopedHookEnable&) = delete;
    ScopedHookEnable(ScopedHookEnable&&) = delete;
    ScopedHookEnable& operator=(ScopedHookEnable&&) = delete;

private:
    bool previous_{false};
};

// 绑定全局 IOManager（供 Hook 等待 IO/Timer 事件使用）。
void set_hook_io_manager(IOManager* io_manager) noexcept;

// 获取当前绑定的 IOManager。
IOManager* hook_io_manager() noexcept;

// 设置当前线程 connect Hook 超时（毫秒）。
// 0 表示不设置固定 connect 超时（仅受上游 deadline 约束）。
void set_hook_connect_timeout_ms(std::uint64_t timeout_ms) noexcept;

// 获取当前线程 connect Hook 超时（毫秒）。
std::uint64_t hook_connect_timeout_ms() noexcept;

}  // namespace rpc::runtime
