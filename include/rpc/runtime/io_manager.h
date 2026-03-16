#pragma once

// 文件用途：
// 定义 Epoll 事件循环与 FdContext 管理接口，用于 IO 事件与协程调度器对接。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/scheduler.h"
#include "rpc/runtime/timer.h"

namespace rpc::runtime {

// IO 事件类型（按位组合）。
enum class IOEvent : std::uint8_t {
    None = 0,
    Read = 1,
    Write = 2,
};
// IOEvent 位运算支持，方便组合和检查事件。
inline IOEvent operator|(IOEvent lhs, IOEvent rhs) {
    return static_cast<IOEvent>(
        static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs)
    );
}
// IOEvent 位运算支持，方便组合和检查事件。
inline IOEvent operator&(IOEvent lhs, IOEvent rhs) {
    return static_cast<IOEvent>(
        static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs)
    );
}

inline IOEvent operator~(IOEvent value) {
    return static_cast<IOEvent>(~static_cast<std::uint8_t>(value));
}

inline IOEvent& operator|=(IOEvent& lhs, IOEvent rhs) {
    lhs = lhs | rhs;
    return lhs;
}

inline IOEvent& operator&=(IOEvent& lhs, IOEvent rhs) {
    lhs = lhs & rhs;
    return lhs;
}

inline bool has_event(IOEvent value, IOEvent flag) {
    return (value & flag) == flag;
}

class IOManager {
public:
    // 创建 IO 管理器并绑定调度器。
    explicit IOManager(CoroutineScheduler& scheduler, std::size_t max_events = 256);
    ~IOManager();

    IOManager(const IOManager&) = delete;
    IOManager& operator=(const IOManager&) = delete;
    IOManager(IOManager&&) = delete;
    IOManager& operator=(IOManager&&) = delete;

    // 启动 Epoll 事件循环线程。
    void start();

    // 停止事件循环并等待线程退出。
    void stop();

    // 注册 IO 事件，绑定到回调函数（回调会通过调度器执行）。
    bool add_event(int fd, IOEvent event, CoroutineCallback callback);

    // 删除 IO 事件。
    bool del_event(int fd, IOEvent event);

    // 添加定时器：delay_ms 后执行回调（回调通过调度器执行）。
    TimerId add_timer(std::chrono::milliseconds delay_ms, CoroutineCallback callback);

    // 取消定时器。
    bool cancel_timer(TimerId timer_id);

    // 恢复 WAITING 协程（供 Hook 事件回调使用）。
    bool resume_coroutine(CoroutineId coroutine_id);

private:
    struct EventContext {
        CoroutineCallback callback{};
        bool active{false};
    };

    class FdContext {
    public:
        explicit FdContext(int fd);

        int fd() const noexcept;
        IOEvent events() const noexcept;
        std::mutex& mutex();
        // 事件上下文快照接口，用于在修改事件绑定前保存当前状态，以便在修改失败时恢复。
        EventContext snapshot_event(IOEvent event) const;
        void restore_event(IOEvent event, const EventContext& snapshot, IOEvent events_snapshot);

        bool bind_event(IOEvent event, CoroutineCallback callback);
        bool unbind_event(IOEvent event);
        void trigger_events(IOEvent events, CoroutineScheduler& scheduler, int epoll_fd);

    private:
    // 事件上下文访问接口，简化事件绑定/解绑/触发逻辑。
        EventContext& event_context(IOEvent event);
        // const 版本事件上下文访问接口，简化事件绑定/解绑/触发逻辑。
        const EventContext& event_context(IOEvent event) const;

        int fd_{-1};
        // 当前注册的事件类型（Read/Write），用于快速判断和更新 Epoll 监听状态。
        IOEvent registered_events_{IOEvent::None};
        // 读写事件上下文，分别保存对应事件的回调函数。
        EventContext read_;
        // 写事件上下文，分别保存对应事件的回调函数。
        EventContext write_;
        // 保护 FdContext 内部状态的互斥锁，避免多线程访问同一 fd 时发生竞态条件。
        mutable std::mutex mutex_;
    };

    bool add_event_internal(int fd, IOEvent event, CoroutineCallback callback);
    FdContext* get_fd_context(int fd);
    void wakeup_event_loop();
    void drain_wakeup_fd();
    void event_loop();

    int epoll_fd_{-1};
    int wakeup_fd_{-1};
    std::size_t max_events_{256};
    CoroutineScheduler& scheduler_;
    TimerManager timer_manager_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::mutex fd_mutex_;
    std::vector<std::unique_ptr<FdContext>> fd_contexts_; // fd -> FdContext 映射表，按 fd 索引，动态扩容。
};

}  // namespace rpc::runtime
