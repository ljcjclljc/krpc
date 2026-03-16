#include "rpc/runtime/request_context.h"

// 文件用途：
// 实现请求 deadline 上下文：取消标记、TLS 传递、超时定时器绑定。

#include <stdexcept>

#include "rpc/runtime/io_manager.h"

namespace rpc::runtime {

namespace {

thread_local RequestContextPtr g_current_request_context;

}  // namespace

std::shared_ptr<RequestContext> RequestContext::create(std::string request_id, TimePoint deadline) {
    return std::shared_ptr<RequestContext>(new RequestContext(std::move(request_id), deadline));
}

std::shared_ptr<RequestContext> RequestContext::create_with_timeout(
    std::string request_id,
    std::chrono::milliseconds timeout
) {
    if (timeout.count() < 0) {
        timeout = std::chrono::milliseconds(0);
    }
    return create(std::move(request_id), Clock::now() + timeout);
}

RequestContext::RequestContext(std::string request_id, TimePoint deadline)
    : request_id_(std::move(request_id)), deadline_(deadline) {
    if (request_id_.empty()) {
        throw std::invalid_argument("RequestContext request_id cannot be empty");
    }
}

const std::string& RequestContext::request_id() const noexcept {
    return request_id_;
}

RequestContext::TimePoint RequestContext::deadline() const noexcept {
    return deadline_;
}

bool RequestContext::cancelled() const noexcept {
    return cancelled_.load(std::memory_order_acquire);
}

bool RequestContext::cancel(std::string reason) {
    bool expected = false;
    if (!cancelled_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    cancel_reason_ = reason.empty() ? "cancelled" : std::move(reason);
    return true;
}

std::string RequestContext::cancel_reason() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cancel_reason_;
}

bool RequestContext::check_deadline_and_cancel() {
    if (cancelled()) {
        return true;
    }

    if (deadline_ == TimePoint::max()) {
        return false;
    }

    if (Clock::now() >= deadline_) {
        cancel("deadline_exceeded");
        return true;
    }

    return false;
}

void RequestContext::set_value(std::string key, std::string value) {
    std::lock_guard<std::mutex> lock(mutex_);
    values_[std::move(key)] = std::move(value);
}

std::optional<std::string> RequestContext::value(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = values_.find(key);
    if (it == values_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool RequestContext::bind_deadline_timer(IOManager& io_manager) {
    if (deadline_ == TimePoint::max()) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (deadline_timer_id_ != 0) {
            return true;
        }
    }

    const TimePoint now = Clock::now();
    if (deadline_ <= now) {
        cancel("deadline_exceeded");
        return true;
    }

    const auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(deadline_ - now);
    std::weak_ptr<RequestContext> weak_self = weak_from_this();
    const TimerId timer_id = io_manager.add_timer(delay, [weak_self]() {
        if (const auto self = weak_self.lock()) {
            self->cancel("deadline_exceeded");
        }
    });

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (deadline_timer_id_ == 0) {
            deadline_timer_id_ = timer_id;
            return true;
        }
    }

    io_manager.cancel_timer(timer_id);
    return true;
}

void RequestContext::clear_deadline_timer(IOManager& io_manager) {
    TimerId timer_id = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        timer_id = deadline_timer_id_;
        deadline_timer_id_ = 0;
    }

    if (timer_id != 0) {
        io_manager.cancel_timer(timer_id);
    }
}

void set_current_request_context(RequestContextPtr context) {
    g_current_request_context = std::move(context);
}

RequestContextPtr current_request_context() {
    return g_current_request_context;
}

ScopedRequestContext::ScopedRequestContext(RequestContextPtr context)
    : previous_(current_request_context()) {
    set_current_request_context(std::move(context));
}

ScopedRequestContext::~ScopedRequestContext() {
    set_current_request_context(std::move(previous_));
}

}  // namespace rpc::runtime
