#include "rpc/runtime/coroutine.h"

// 文件用途：
// 实现协程对象的状态机和上下文切换能力。
// - Windows: 使用 Fiber 机制
// - POSIX  : 使用 ucontext 机制
// 同时保证正常/异常执行路径都能统一收口到 TERM，便于调度器回收。

#include <stdexcept>
#include <string>
#include <utility>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <ucontext.h>
#endif

namespace rpc::runtime {

namespace {

// 线程级运行时上下文：每个线程独立维护，避免跨线程协程状态污染。
struct ThreadRuntimeContext {
#if defined(_WIN32)
    // 主 Fiber，协程 yield 或结束时切回该上下文。
    void* main_fiber{nullptr};
#else
    // 主上下文，协程 yield 或结束时切回该上下文。
    ucontext_t main_context{};
#endif
    // 当前线程正在运行的协程。
    Coroutine* current{nullptr};
};

ThreadRuntimeContext& thread_context() {
    // 每个线程一份上下文状态。
    thread_local ThreadRuntimeContext ctx;
    return ctx;
}

#if defined(_WIN32)
void ensure_main_fiber() {
    auto& ctx = thread_context();
    if (ctx.main_fiber != nullptr) {
        return;
    }

    // 首次进入协程系统时，将线程转换为 Fiber。
    void* converted = ConvertThreadToFiberEx(nullptr, FIBER_FLAG_FLOAT_SWITCH);
    if (converted != nullptr) {
        ctx.main_fiber = converted;
        return;
    }

    const DWORD err = GetLastError();
    if (err == ERROR_ALREADY_FIBER) {
        // 若线程本身已是 Fiber，则直接使用当前 Fiber。
        ctx.main_fiber = GetCurrentFiber();
        return;
    }

    // 到这里说明创建主 Fiber 失败，抛出异常由上层处理。
    throw std::runtime_error("ConvertThreadToFiberEx failed, error=" + std::to_string(err));
}
#endif

}  // namespace

Coroutine::Coroutine(CoroutineId id, CoroutineCallback callback, std::size_t stack_size)
    : id_(id), callback_(std::move(callback)), state_(CoroutineState::READY)
#if defined(_WIN32)
      ,
      fiber_(nullptr)
#else
      ,
      context_(),
      caller_context_(nullptr),
      stack_size_(stack_size),
      stack_(nullptr)
#endif
{
    if (!callback_) {
        // 协程必须绑定可执行回调。
        throw std::invalid_argument("Coroutine callback cannot be empty");
    }

#if defined(_WIN32)
    // Windows 下创建 Fiber 作为协程执行上下文。
    fiber_ = CreateFiberEx(
        0,
        stack_size == 0 ? kDefaultStackSize : stack_size,
        FIBER_FLAG_FLOAT_SWITCH,
        reinterpret_cast<LPFIBER_START_ROUTINE>(&Coroutine::entry),
        this
    );
    if (fiber_ == nullptr) {
        throw std::runtime_error("CreateFiberEx failed, error=" + std::to_string(GetLastError()));
    }
#else
    // POSIX 下分配协程栈并初始化 ucontext。
    if (stack_size_ == 0) {
        stack_size_ = kDefaultStackSize;
    }
    stack_ = new char[stack_size_];

    if (getcontext(&context_) != 0) {
        delete[] stack_;
        stack_ = nullptr;
        throw std::runtime_error("getcontext failed when creating coroutine");
    }

    context_.uc_stack.ss_sp = stack_;
    context_.uc_stack.ss_size = stack_size_;
    context_.uc_link = nullptr;
    makecontext(&context_, reinterpret_cast<void (*)()>(&Coroutine::entry), 1, this);
#endif
}

Coroutine::~Coroutine() {
#if defined(_WIN32)
    // 释放 Fiber 资源。
    if (fiber_ != nullptr) {
        DeleteFiber(fiber_);
        fiber_ = nullptr;
    }
#else
    // 释放协程私有栈空间。
    delete[] stack_;
    stack_ = nullptr;
#endif
}

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

    state_.store(CoroutineState::RUNNING, std::memory_order_release);
    ctx.current = this;

#if defined(_WIN32)
    ensure_main_fiber();
    // 从主 Fiber 切换到协程 Fiber。
    SwitchToFiber(fiber_);
#else
    caller_context_ = &ctx.main_context;
    // 从主上下文切换到协程上下文。
    if (swapcontext(caller_context_, &context_) != 0) {
        // 切换失败时终止协程，防止状态悬挂。
        ctx.current = nullptr;
        state_.store(CoroutineState::TERM, std::memory_order_release);
        throw std::runtime_error("swapcontext failed on resume");
    }
#endif

    // 协程 yield 或结束后会回到这里。
    ctx.current = nullptr;
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

#if defined(_WIN32)
    if (ctx.main_fiber == nullptr) {
        throw std::runtime_error("Main fiber is not initialized");
    }
    // 让出执行权，切回主 Fiber。
    SwitchToFiber(ctx.main_fiber);
#else
    if (caller_context_ == nullptr) {
        throw std::runtime_error("Caller context is not initialized");
    }
    // 让出执行权，切回调用方上下文。
    if (swapcontext(&context_, caller_context_) != 0) {
        // 切换失败时终止协程，避免状态不一致。
        state_.store(CoroutineState::TERM, std::memory_order_release);
        throw std::runtime_error("swapcontext failed on yield");
    }
#endif
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

void Coroutine::entry(void* raw_ptr) {
    // 底层上下文入口函数，转发到实例方法执行。
    auto* self = reinterpret_cast<Coroutine*>(raw_ptr);
    self->run();
}

void Coroutine::run() {
    auto& ctx = thread_context();
    try {
        // 执行用户协程函数体。
        callback_();
    } catch (...) {
        // 异常路径统一收口：
        // 1) 状态标记 TERM
        // 2) 释放回调
        // 3) 清理 current 指针
        // 4) 回切主上下文
        state_.store(CoroutineState::TERM, std::memory_order_release);
        callback_ = nullptr;
        ctx.current = nullptr;
#if defined(_WIN32)
        SwitchToFiber(ctx.main_fiber);
#else
        if (caller_context_ != nullptr) {
            setcontext(caller_context_);
        }
#endif
        return;
    }

    // 正常执行结束也走同样的 TERM 收口逻辑。
    state_.store(CoroutineState::TERM, std::memory_order_release);
    callback_ = nullptr;
    ctx.current = nullptr;

#if defined(_WIN32)
    SwitchToFiber(ctx.main_fiber);
#else
    if (caller_context_ != nullptr) {
        setcontext(caller_context_);
    }
#endif
}

}  // namespace rpc::runtime
