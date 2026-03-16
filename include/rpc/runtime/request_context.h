#pragma once

// 文件用途：
// 定义请求 deadline 上下文基础结构，支持取消标记、TLS 传递和超时自动取消绑定。

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "rpc/runtime/timer.h"

namespace rpc::runtime {

class IOManager;

class RequestContext : public std::enable_shared_from_this<RequestContext> {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    // 创建带绝对 deadline 的上下文。
    static std::shared_ptr<RequestContext> create(std::string request_id, TimePoint deadline);
    // 创建带相对超时的上下文（deadline = now + timeout）。
    static std::shared_ptr<RequestContext> create_with_timeout(
        std::string request_id,
        std::chrono::milliseconds timeout
    );

    const std::string& request_id() const noexcept;
    TimePoint deadline() const noexcept;

    // 取消状态检查。
    bool cancelled() const noexcept;
    // 主动取消：返回 true 表示由当前调用触发首次取消。
    bool cancel(std::string reason = "cancelled");
    // 获取取消原因。
    std::string cancel_reason() const;
    // 周期性超时检查（业务逻辑可轮询调用）。
    bool check_deadline_and_cancel();

    // 最小业务数据存取接口。
    void set_value(std::string key, std::string value);
    std::optional<std::string> value(const std::string& key) const;

    // 注册 deadline 超时定时器（到期后自动 cancel）。
    bool bind_deadline_timer(IOManager& io_manager);
    // 解绑已注册的 deadline 定时器。
    void clear_deadline_timer(IOManager& io_manager);

private:
    RequestContext(std::string request_id, TimePoint deadline);

    mutable std::mutex mutex_;
    std::string request_id_;
    TimePoint deadline_{TimePoint::max()};
    std::atomic<bool> cancelled_{false};
    std::string cancel_reason_;
    std::unordered_map<std::string, std::string> values_;
    TimerId deadline_timer_id_{0};
};

using RequestContextPtr = std::shared_ptr<RequestContext>;

// TLS 上下文读写接口：用于跨函数传递当前请求上下文。
void set_current_request_context(RequestContextPtr context);
RequestContextPtr current_request_context();

// 作用域绑定工具：构造时设置 TLS，上下文离开作用域后恢复原值。
class ScopedRequestContext {
public:
    explicit ScopedRequestContext(RequestContextPtr context);
    ~ScopedRequestContext();

    ScopedRequestContext(const ScopedRequestContext&) = delete;
    ScopedRequestContext& operator=(const ScopedRequestContext&) = delete;
    ScopedRequestContext(ScopedRequestContext&&) = delete;
    ScopedRequestContext& operator=(ScopedRequestContext&&) = delete;

private:
    RequestContextPtr previous_;
};

}  // namespace rpc::runtime
