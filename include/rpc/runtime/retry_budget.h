#pragma once

// 文件用途：
// 定义重试预算基础模块：基于滑动时间窗口统计请求量/重试量，
// 通过预算阈值限制重试，避免故障场景无限重试。

#include <chrono>
#include <cstddef>
#include <deque>
#include <mutex>

namespace rpc::runtime {

struct RetryBudgetOptions {
    std::chrono::milliseconds window{std::chrono::milliseconds(1000)};
    double retry_ratio{0.2};
    std::size_t min_retry_tokens{1};
};

struct RetryBudgetSnapshot {
    std::size_t request_count{0};
    std::size_t retry_count{0};
    std::size_t max_retry_tokens{0};
    std::size_t available_retry_tokens{0};
};

class RetryBudget {
public:
    explicit RetryBudget(RetryBudgetOptions options = {});

    // 更新预算参数（线程安全）。
    void update_options(RetryBudgetOptions options);

    // 记录一次请求（非重试）进入当前窗口。
    void record_request();

    // 申请一次重试预算。
    // 返回 true 表示允许执行本次重试。
    bool try_acquire_retry_token();

    // 观测接口。
    std::size_t request_count() const;
    std::size_t retry_count() const;
    RetryBudgetSnapshot snapshot() const;

private:
    using Clock = std::chrono::steady_clock;

    static RetryBudgetOptions normalize_options(RetryBudgetOptions options);

    void evict_expired_locked(Clock::time_point now) const;
    std::size_t max_retry_tokens_locked() const;

    RetryBudgetOptions options_;
    mutable std::mutex mutex_;
    mutable std::deque<Clock::time_point> request_timestamps_;
    mutable std::deque<Clock::time_point> retry_timestamps_;
};

}  // namespace rpc::runtime
