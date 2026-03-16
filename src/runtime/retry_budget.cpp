#include "rpc/runtime/retry_budget.h"

// 文件用途：
// 实现重试预算滑动窗口控制逻辑。

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace rpc::runtime {

RetryBudget::RetryBudget(RetryBudgetOptions options)
    : options_(options) {
    if (options_.window.count() <= 0) {
        throw std::invalid_argument("RetryBudget window must be positive");
    }
    if (options_.retry_ratio < 0.0) {
        throw std::invalid_argument("RetryBudget retry_ratio must be non-negative");
    }
}

void RetryBudget::record_request() {
    const Clock::time_point now = Clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    evict_expired_locked(now);
    request_timestamps_.push_back(now);
}

bool RetryBudget::try_acquire_retry_token() {
    const Clock::time_point now = Clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    evict_expired_locked(now);

    const std::size_t max_tokens = max_retry_tokens_locked();
    if (retry_timestamps_.size() >= max_tokens) {
        return false;
    }

    retry_timestamps_.push_back(now);
    return true;
}

std::size_t RetryBudget::request_count() const {
    const Clock::time_point now = Clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    evict_expired_locked(now);
    return request_timestamps_.size();
}

std::size_t RetryBudget::retry_count() const {
    const Clock::time_point now = Clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    evict_expired_locked(now);
    return retry_timestamps_.size();
}

void RetryBudget::evict_expired_locked(Clock::time_point now) const {
    const Clock::time_point threshold = now - options_.window;
    while (!request_timestamps_.empty() && request_timestamps_.front() < threshold) {
        request_timestamps_.pop_front();
    }
    while (!retry_timestamps_.empty() && retry_timestamps_.front() < threshold) {
        retry_timestamps_.pop_front();
    }
}

std::size_t RetryBudget::max_retry_tokens_locked() const {
    const std::size_t requests = request_timestamps_.size();
    const std::size_t ratio_tokens = static_cast<std::size_t>(
        std::floor(static_cast<double>(requests) * options_.retry_ratio)
    );
    return std::max(options_.min_retry_tokens, ratio_tokens);
}

}  // namespace rpc::runtime
