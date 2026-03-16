#include "rpc/runtime/coroutine.h"

// 文件用途：
// 实现协程对象的状态机和上下文切换能力（基于 Boost.Context fiber）。
// 保持 READY/RUNNING/WAITING/TERM 语义，便于调度器无侵入复用。

#include <stdexcept>
#include <utility>

namespace rpc::runtime {

namespace {

// 线程级运行时上下文：每个线程独立维护，避免跨线程协程状态污染。
struct ThreadRuntimeContext {
    // 当前线程正在运行的协程。
    Coroutine* current{nullptr};
};

ThreadRuntimeContext& thread_context() {
    // 每个线程一份上下文状态。
    thread_local ThreadRuntimeContext ctx;
    return ctx;
}

}  // namespace

Coroutine::Coroutine(CoroutineId id, CoroutineCallback callback, std::size_t stack_size)
    : id_(id),
      callback_(std::move(callback)),
      state_(CoroutineState::READY),
      stack_size_((stack_size == 0) ? kDefaultStackSize : stack_size) {
    if (!callback_) {
        // 协程必须绑定可执行回调。
        throw std::invalid_argument("Coroutine callback cannot be empty");
    }
}

Coroutine::~Coroutine() = default;

void Coroutine::resume() {
    // 已结束协程不可再次执行，直接返回。
    if (state_.load(std::memory_order_acquire) == CoroutineState::TERM) {
        return;
    }
    // 状态约束：仅允许 READY -> RUNNING。
    if (state_.load(std::memory_order_acquire) != CoroutineState::READY) {
        throw std::logic_error("Coroutine can only resume from READY state");
    }

    auto& ctx = thread_context();
    // 当前实现不支持嵌套 resume（协程内直接 resume 其他协程）。
    if (ctx.current != nullptr) {
        throw std::logic_error("Nested coroutine resume is not supported");
    }

    // 惰性创建 fiber：确保创建和首次执行发生在同一线程。
    ensure_fiber_initialized();

    state_.store(CoroutineState::RUNNING, std::memory_order_release);
    ctx.current = this;

    try {
        // 切入协程执行。resume() 返回后，fiber_ 已变为“下一次可恢复句柄”。
        fiber_ = std::move(fiber_).resume();
    } catch (...) {
        ctx.current = nullptr;
        state_.store(CoroutineState::TERM, std::memory_order_release);
        throw;
    }

    // 协程 yield 或结束后会回到这里。
    ctx.current = nullptr;
}

void Coroutine::ensure_fiber_initialized() {
    if (fiber_) {
        return;
    }

    boost::context::fixedsize_stack stack_allocator{stack_size_};
    fiber_ = boost::context::fiber(
        std::allocator_arg,
        std::move(stack_allocator),
        [this](boost::context::fiber&& caller) {
            return Coroutine::fiber_entry(std::move(caller), this);
        }
    );
}

void Coroutine::yield() {
    // 状态约束：仅允许 RUNNING -> READY。
    if (state_.load(std::memory_order_acquire) != CoroutineState::RUNNING) {
        throw std::logic_error("Coroutine can only yield from RUNNING state");
    }

    auto& ctx = thread_context();
    if (ctx.current != this) {
        throw std::logic_error("Only current running coroutine can yield");
    }

    state_.store(CoroutineState::READY, std::memory_order_release);
    ctx.current = nullptr;

    if (!caller_fiber_) {
        throw std::runtime_error("Caller fiber is not initialized");
    }

    try {
        // 切回调度方。返回时说明调度方再次 resume 了当前协程。
        caller_fiber_ = std::move(caller_fiber_).resume();
    } catch (...) {
        state_.store(CoroutineState::TERM, std::memory_order_release);
        throw;
    }

    // 从 caller 恢复回来后，当前运行协程应重新标记为 this。
    ctx.current = this;
}

void Coroutine::yield_waiting() {
    // 状态约束：仅允许 RUNNING -> WAITING。
    if (state_.load(std::memory_order_acquire) != CoroutineState::RUNNING) {
        throw std::logic_error("Coroutine can only yield_waiting from RUNNING state");
    }

    auto& ctx = thread_context();
    if (ctx.current != this) {
        throw std::logic_error("Only current running coroutine can yield_waiting");
    }

    state_.store(CoroutineState::WAITING, std::memory_order_release);
    ctx.current = nullptr;

    if (!caller_fiber_) {
        throw std::runtime_error("Caller fiber is not initialized");
    }

    try {
        // 切回调度方。返回时说明调度方再次 resume 了当前协程。
        caller_fiber_ = std::move(caller_fiber_).resume();
    } catch (...) {
        state_.store(CoroutineState::TERM, std::memory_order_release);
        throw;
    }

    ctx.current = this;
}

bool Coroutine::try_mark_ready_from_waiting() noexcept {
    CoroutineState expected = CoroutineState::WAITING;
    return state_.compare_exchange_strong(
        expected,
        CoroutineState::READY,
        std::memory_order_acq_rel,
        std::memory_order_acquire
    );
}

CoroutineId Coroutine::id() const noexcept {
    return id_;
}

CoroutineState Coroutine::state() const noexcept {
    return state_.load(std::memory_order_acquire);
}

Coroutine* Coroutine::current() noexcept {
    return thread_context().current;
}

void Coroutine::yield_current() {
    // 便捷静态接口：让当前协程主动让出 CPU。
    Coroutine* current_coroutine = Coroutine::current();
    if (current_coroutine == nullptr) {
        throw std::logic_error("No current coroutine to yield");
    }
    current_coroutine->yield();
}

void Coroutine::yield_current_waiting() {
    Coroutine* current_coroutine = Coroutine::current();
    if (current_coroutine == nullptr) {
        throw std::logic_error("No current coroutine to yield_waiting");
    }
    current_coroutine->yield_waiting();
}

boost::context::fiber Coroutine::fiber_entry(boost::context::fiber&& caller, Coroutine* self) {
    // 首次切入 fiber 时接收 caller 句柄，后续 yield() 依赖它切回调度方。
    self->caller_fiber_ = std::move(caller);
    self->run();
    return std::move(self->caller_fiber_);
}

void Coroutine::run() {
    auto& ctx = thread_context();
    try {
        // 执行用户协程函数体。
        callback_();
    } catch (...) {
        // 异常不向外层调度器传播，统一收口为 TERM 以便安全回收。
        state_.store(CoroutineState::TERM, std::memory_order_release);
        callback_ = nullptr;
        ctx.current = nullptr;
        return;
    }

    // 正常执行结束也走同样的 TERM 收口逻辑。
    state_.store(CoroutineState::TERM, std::memory_order_release);
    callback_ = nullptr;
    ctx.current = nullptr;
}

}  // namespace rpc::runtime
