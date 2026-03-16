#pragma once

// 文件用途：
// 定义最小堆定时器管理器，提供添加、取消、到期收集和最近超时时间计算能力。

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

#include "rpc/runtime/coroutine.h"

namespace rpc::runtime {

using TimerId = std::uint64_t;

class TimerManager {
public:
    using Clock = std::chrono::steady_clock;// 定时器到期时间点类型。
    using TimePoint = Clock::time_point;// 定时器回调函数类型。
    using TimerCallback = CoroutineCallback;// 定时器回调函数类型，直接使用 CoroutineCallback 以便定时器回调中调度协程。

    TimerManager() = default;
    ~TimerManager() = default;

    TimerManager(const TimerManager&) = delete;
    TimerManager& operator=(const TimerManager&) = delete;
    TimerManager(TimerManager&&) = delete;
    TimerManager& operator=(TimerManager&&) = delete;

    // 添加定时器：delay_ms 后触发 callback。
    // earliest_changed 为 true 表示本次插入改变了堆顶（需要唤醒等待线程）。
    TimerId add_timer(
        std::chrono::milliseconds delay_ms, // 定时器延迟时间。
        TimerCallback callback,
        bool* earliest_changed = nullptr
    );

    // 取消定时器：通过哈希表 O(1) 定位并标记取消，不直接删除堆节点。
    bool cancel_timer(TimerId id);

    // 收集所有已到期回调（不在锁内执行）。
    std::vector<TimerCallback> collect_expired_callbacks();

    // 返回最近到期时间（毫秒）：
    // -1 表示无定时器；0 表示已有到期定时器；>0 表示最近等待时间。
    int next_timeout_ms();

    // 清空所有定时器。
    void clear();

private:
    struct TimerNode {
        TimePoint expires_at{}; // 定时器到期时间点。
        TimerCallback callback{}; // 定时器回调函数。
        bool cancelled{false}; // 是否已取消。
    };

    struct HeapNode {
        TimePoint expires_at{}; // 定时器到期时间点。
        TimerId id{0}; // 定时器 ID。
    };

    struct HeapNodeGreater {
        // 最小堆：到期更早优先；同到期时间按 ID 升序，保证插入顺序稳定。
        bool operator()(const HeapNode& lhs, const HeapNode& rhs) const;
    };

    void prune_heap_top_locked();// 锁内调用：清理堆顶所有已取消定时器。

    std::atomic<TimerId> next_id_{1};
    std::unordered_map<TimerId, TimerNode> timers_;
    std::priority_queue<HeapNode, std::vector<HeapNode>, HeapNodeGreater> heap_;
    mutable std::mutex mutex_;
};

}  // namespace rpc::runtime
