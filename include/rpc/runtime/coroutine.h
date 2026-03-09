#pragma once

// 文件用途：
// 定义协程对象的核心接口与状态机，提供 resume/yield 上下文切换能力。
// 该头文件是 W2 协程运行时与调度器对接的基础契约。

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>

#if !defined(_WIN32)
#include <ucontext.h>
#endif

namespace rpc::runtime {

// 协程生命周期状态：
// READY   : 可被调度执行
// RUNNING : 正在执行
// TERM    : 执行结束（正常结束或异常结束）
enum class CoroutineState : std::uint8_t {
    READY = 0,
    RUNNING = 1,
    TERM = 2,
};

// 协程唯一标识类型。
using CoroutineId = std::uint64_t;

// 协程回调类型。业务逻辑以该回调形式运行在协程上下文中。
using CoroutineCallback = std::function<void()>;

class Coroutine {
public:
    // 默认协程栈大小：128KB。
    // 该值在大多数轻量逻辑场景下可平衡栈空间与并发数量。
    static constexpr std::size_t kDefaultStackSize = 128 * 1024;

    // 创建协程对象并初始化执行上下文。
    // id         : 协程 ID（由调度器分配）
    // callback   : 协程体
    // stack_size : 协程栈大小（0 时内部会退回默认值）
    Coroutine(CoroutineId id, CoroutineCallback callback, std::size_t stack_size = kDefaultStackSize);
    ~Coroutine();

    Coroutine(const Coroutine&) = delete;
    Coroutine& operator=(const Coroutine&) = delete;
    Coroutine(Coroutine&&) = delete;
    Coroutine& operator=(Coroutine&&) = delete;

    // 恢复协程执行：
    // READY -> RUNNING，并切换到协程上下文。
    void resume();

    // 协程主动让出执行权：
    // RUNNING -> READY，并切回调用方上下文。
    void yield();

    // 获取协程 ID。
    CoroutineId id() const noexcept;

    // 获取当前状态。
    CoroutineState state() const noexcept;

    // 返回当前线程正在运行的协程指针；如果当前不在协程上下文则返回 nullptr。
    static Coroutine* current() noexcept;

    // 便捷接口：让当前协程主动 yield。
    // 若当前没有运行中的协程会抛出逻辑异常。
    static void yield_current();

private:
    // 协程入口函数（用于底层上下文 API 回调）。
    static void entry(void* raw_ptr);

    // 执行用户回调并统一收口生命周期。
    void run();

    // 协程元信息与状态。
    CoroutineId id_;
    CoroutineCallback callback_;
    // W3 起状态字段改为原子，避免多线程调度下读取/写入数据竞争。
    std::atomic<CoroutineState> state_;
#if defined(_WIN32)
    // Windows 使用 Fiber 保存协程上下文。
    void* fiber_{nullptr};
#else
    // POSIX 使用 ucontext 保存协程上下文与栈信息。
    ucontext_t context_{};
    ucontext_t* caller_context_{nullptr};
    std::size_t stack_size_{0};
    char* stack_{nullptr};
#endif
};

}  // namespace rpc::runtime
