#include "rpc/net/net.h"

// 文件用途：
// 提供 W7 阶段最小 net 能力：
// 1) 计算上游 deadline 与下游超时配置的合并预算
// 2) 提供可控失败的调用桩，用于重试预算与超时分层验证

#include <algorithm>
#include <atomic>
#include <chrono>
#include <utility>

#include "rpc/runtime/request_context.h"

namespace rpc::net {

namespace {

std::atomic<std::uint64_t> g_last_effective_timeout_ms{0};

}  // namespace

void init_network() {
    // 当前阶段无实际初始化逻辑，保留空实现用于打通启动链路。
}

EffectiveTimeout derive_effective_timeout(std::uint64_t downstream_timeout_ms) {
    EffectiveTimeout result;
    result.effective_timeout_ms = downstream_timeout_ms;

    const auto context = rpc::runtime::current_request_context();
    if (!context || context->deadline() == rpc::runtime::RequestContext::TimePoint::max()) {
        return result;
    }

    const auto now = rpc::runtime::RequestContext::Clock::now();
    if (context->deadline() <= now) {
        context->cancel("deadline_exceeded");
        result.deadline_exceeded = true;
        result.effective_timeout_ms = 0;
        return result;
    }

    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        context->deadline() - now
    );
    const std::uint64_t remaining_ms = static_cast<std::uint64_t>(remaining.count());

    if (result.effective_timeout_ms == 0) {
        result.effective_timeout_ms = remaining_ms;
    } else {
        result.effective_timeout_ms = std::min(result.effective_timeout_ms, remaining_ms);
    }

    return result;
}

NetCallResponse invoke_stub(const NetCallRequest& request) {
    const EffectiveTimeout timeout = derive_effective_timeout(request.downstream_timeout_ms);
    g_last_effective_timeout_ms.store(timeout.effective_timeout_ms, std::memory_order_release);

    if (timeout.deadline_exceeded) {
        return NetCallResponse{
            504,
            "deadline_exceeded",
            {},
            false,
            timeout.effective_timeout_ms,
        };
    }

    if (request.attempt <= request.fail_before_success) {
        return NetCallResponse{
            503,
            "transient_upstream_error",
            {},
            true,
            timeout.effective_timeout_ms,
        };
    }

    return NetCallResponse{
        0,
        "ok",
        request.endpoint + ":" + request.payload,
        false,
        timeout.effective_timeout_ms,
    };
}

std::uint64_t last_effective_timeout_ms() noexcept {
    return g_last_effective_timeout_ms.load(std::memory_order_acquire);
}

}  // namespace rpc::net
