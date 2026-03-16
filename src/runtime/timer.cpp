#include "rpc/runtime/timer.h"

// 文件用途：
// 实现最小堆定时器管理：添加、取消、到期收集、最近超时计算。

#include <limits>
#include <stdexcept>

namespace rpc::runtime {
// 最小堆比较器：到期时间更早优先；同到期时间按 ID 升序，保证插入顺序稳定。
bool TimerManager::HeapNodeGreater::operator()(const HeapNode& lhs, const HeapNode& rhs) const {
    if (lhs.expires_at == rhs.expires_at) {
        return lhs.id > rhs.id;
    }
    return lhs.expires_at > rhs.expires_at;
}
// 添加定时器：delay_ms 后触发 callback。
TimerId TimerManager::add_timer(
    std::chrono::milliseconds delay_ms,
    TimerCallback callback,
    bool* earliest_changed
) {
    if (!callback) {
        throw std::invalid_argument("Timer callback cannot be empty");
    }

    if (delay_ms.count() < 0) {
        delay_ms = std::chrono::milliseconds(0);
    }
    // 生成唯一定时器 ID 和计算到期时间点。
    const TimerId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    // 计算定时器到期时间点，使用 steady_clock 避免系统时间调整影响。
    const TimePoint expires_at = Clock::now() + delay_ms;

    std::lock_guard<std::mutex> lock(mutex_);
    prune_heap_top_locked();
    const TimerId old_top_id = heap_.empty() ? 0 : heap_.top().id;

    timers_.emplace(
        id,
        TimerNode{
            expires_at,
            std::move(callback),
            false,
        }
    );
    heap_.push(HeapNode{expires_at, id});

    if (earliest_changed != nullptr) {
        *earliest_changed = old_top_id == 0 || heap_.top().id != old_top_id;
    }

    return id;
}

bool TimerManager::cancel_timer(TimerId id) {
    if (id == 0) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = timers_.find(id);
    if (it == timers_.end()) {
        return false;
    }

    it->second.cancelled = true;
    it->second.callback = nullptr;
    return true;
}

std::vector<TimerManager::TimerCallback> TimerManager::collect_expired_callbacks() {
    std::vector<TimerCallback> callbacks;
    const TimePoint now = Clock::now();

    std::lock_guard<std::mutex> lock(mutex_);
    prune_heap_top_locked();

    while (!heap_.empty()) {
        const HeapNode top = heap_.top();
        const auto it = timers_.find(top.id);
        if (it == timers_.end()) {
            heap_.pop();
            continue;
        }

        TimerNode& node = it->second;
        if (node.cancelled) {
            heap_.pop();
            timers_.erase(it);
            continue;
        }

        if (node.expires_at > now) {
            break;
        }

        heap_.pop();
        if (node.callback) {
            callbacks.push_back(std::move(node.callback));
        }
        timers_.erase(it);
    }

    return callbacks;
}

int TimerManager::next_timeout_ms() {
    std::lock_guard<std::mutex> lock(mutex_);
    prune_heap_top_locked();

    if (heap_.empty()) {
        return -1;
    }

    const auto it = timers_.find(heap_.top().id);
    if (it == timers_.end()) {
        return -1;
    }

    const TimePoint now = Clock::now();
    const TimePoint expires_at = it->second.expires_at;
    if (expires_at <= now) {
        return 0;
    }

    const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(expires_at - now);
    const auto diff_count = diff.count();
    const auto kIntMax = static_cast<std::chrono::milliseconds::rep>(std::numeric_limits<int>::max());
    if (diff_count > kIntMax) {
        return std::numeric_limits<int>::max();
    }

    // 向上取整到至少 1ms，避免“未到期却传 0”导致忙轮询。
    return diff_count > 0 ? static_cast<int>(diff_count) : 1;
}

void TimerManager::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    timers_.clear();
    heap_ = {};
}

void TimerManager::prune_heap_top_locked() {
    while (!heap_.empty()) {
        const HeapNode top = heap_.top();
        const auto it = timers_.find(top.id);
        if (it == timers_.end()) {
            heap_.pop();
            continue;
        }

        if (it->second.cancelled) {
            heap_.pop();
            timers_.erase(it);
            continue;
        }

        break;
    }
}

}  // namespace rpc::runtime
