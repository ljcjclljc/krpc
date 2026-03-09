#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rpc/runtime/coroutine.h"

namespace rpc::runtime {

class CoroutineScheduler {
public:
    CoroutineScheduler() = default;
    ~CoroutineScheduler();

    CoroutineScheduler(const CoroutineScheduler&) = delete;
    CoroutineScheduler& operator=(const CoroutineScheduler&) = delete;
    CoroutineScheduler(CoroutineScheduler&&) = delete;
    CoroutineScheduler& operator=(CoroutineScheduler&&) = delete;

    // 创建协程并入队（仅在线程池启动后可调用）。
    CoroutineId schedule(CoroutineCallback callback, std::size_t stack_size = Coroutine::kDefaultStackSize);

    // 启动 M:N 调度线程池。
    // worker_threads == 0 时自动使用 hardware_concurrency（至少 1）。
    void start(std::size_t worker_threads = 0);

    // 等待在途任务全部完成，然后停止调度并回收 worker 线程。
    void stop();

    // 阻塞直到没有 pending/active 任务。
    void wait_idle();

    // 扫描并回收所有处于 TERM 的协程对象。
    void recycle_terminated();

    // 当前由调度器持有的协程对象数量。
    std::size_t alive_count() const noexcept;

    // 累计回收完成的协程数量。
    std::size_t completed_count() const noexcept;

    // 当前就绪队列（所有 worker 队列合计）中的待执行任务数量。
    std::size_t pending_count() const noexcept;

    // 当前 worker 线程数量（仅 M:N 模式有意义）。
    std::size_t worker_count() const noexcept;

    // idle 协程循环次数（用于观测空闲调度是否生效）。
    std::size_t idle_switch_count() const noexcept;

    // 当前是否处于 worker 调度模式。
    bool running() const noexcept;

private:
    struct WorkerContext {
        // 每个 worker 自己的就绪队列与同步原语，降低跨线程抢锁冲突。
        std::deque<CoroutineId> queue;
        std::mutex mutex;
        std::condition_variable cv;
        std::thread thread;
    };

    CoroutineId allocate_id();
    void recycle(CoroutineId id);

    void worker_loop(std::size_t worker_index);
    void execute_coroutine(CoroutineId id, std::size_t worker_index);
    void enqueue_ready(CoroutineId id, std::size_t worker_index);
    void notify_idle_waiters();

    mutable std::mutex state_mutex_;
    // 调度器持有的协程对象表，生命周期由调度器统一管理。
    std::unordered_map<CoroutineId, std::shared_ptr<Coroutine>> coroutines_;
    // 已回收可复用 ID 池。
    std::vector<CoroutineId> recycled_ids_;
    // worker 上下文集合。
    std::vector<std::unique_ptr<WorkerContext>> workers_;

    CoroutineId next_id_{1};
    std::size_t completed_count_{0};
    std::size_t dispatch_cursor_{0};

    std::atomic<bool> started_{false};
    std::atomic<bool> stop_requested_{false};
    // pending: 已入队未执行；active: 正在执行。
    std::atomic<std::size_t> pending_tasks_{0};
    std::atomic<std::size_t> active_tasks_{0};
    std::atomic<std::size_t> idle_switches_{0};

    // 用于 wait_idle 的条件变量通知。
    mutable std::mutex idle_mutex_;
    std::condition_variable idle_cv_;
};

}  // namespace rpc::runtime
