#pragma once

#include <atomic>
#include <cstdint>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "rpc/runtime/coroutine.h"

namespace rpc::runtime {

struct SchedulerProfileSnapshot {
    std::size_t enqueue_local{0};
    std::size_t enqueue_global{0};
    std::size_t enqueue_fast{0};

    std::size_t dequeue_local_fast{0};
    std::size_t dequeue_local{0};
    std::size_t dequeue_global{0};
    std::size_t dequeue_steal{0};

    std::uint64_t state_lock_wait_ns_total{0};
    std::size_t state_lock_wait_samples{0};
    std::uint64_t state_lock_wait_ns_avg{0};
};

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

    // 恢复 WAITING 协程：
    // - WAITING 协程会被转为 READY 并重新入队；
    // - RUNNING 协程会记录“待恢复”，在其进入 WAITING 后立即补发恢复。
    bool resume(CoroutineId id);

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

    // 工作窃取次数（用于观测负载均衡效果）。
    std::size_t steal_count() const noexcept;

    // 调度剖面快照（队列出入分布 + 锁等待）。
    SchedulerProfileSnapshot profile_snapshot() const noexcept;

    // 当前是否处于 worker 调度模式。
    bool running() const noexcept;

private:
    struct WorkerContext {
        // 协程本地快队列：协程 yield 或 IO 恢复后优先进入，降低调度往返开销。
        std::deque<CoroutineId> local_fast_queue;
        // 线程本地队列：worker 常规可执行队列。
        std::deque<CoroutineId> local_queue;
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
    bool try_dequeue_local_fast(WorkerContext& worker, CoroutineId& id);
    bool try_dequeue_local(WorkerContext& worker, CoroutineId& id);
    bool try_dequeue_global(CoroutineId& id);
    bool try_steal_from_other_worker(std::size_t thief_index, CoroutineId& id);
    bool try_dequeue_next(std::size_t worker_index, WorkerContext& worker, CoroutineId& id);

    mutable std::mutex state_mutex_;
    // 调度器持有的协程对象表，生命周期由调度器统一管理。
    std::unordered_map<CoroutineId, std::shared_ptr<Coroutine>> coroutines_;
    // 已回收可复用 ID 池。
    std::vector<CoroutineId> recycled_ids_;
    // worker 上下文集合。
    std::vector<std::unique_ptr<WorkerContext>> workers_;
    // 全局队列：外部线程提交任务先进入该队列，再由 worker 分发执行。
    std::deque<CoroutineId> global_queue_;
    mutable std::mutex global_queue_mutex_;
    // 轮询分发游标：用于 schedule/enqueue_ready 在多 worker 间均匀投递任务。
    std::size_t dispatch_cursor_{0};
    // 记录每个 worker 当前“正在执行”的协程数量。
    // key: worker 下标，value: 正在执行数。
    // 记录协程最近一次运行的 worker，用于恢复时优先投递回原线程。
    std::unordered_map<CoroutineId, std::size_t> coroutine_last_worker_;
    // 当前正在 worker 中执行的协程集合（用于处理“恢复先于挂起”的竞态）。
    std::unordered_set<CoroutineId> running_coroutines_;
    // 标记是否已进入过执行路径（用于限制可窃取任务范围）。
    std::unordered_set<CoroutineId> started_coroutines_;
    // 记录对 RUNNING 协程的提前恢复请求，等其进入 WAITING 后补发。
    std::unordered_set<CoroutineId> pending_resumes_;

    CoroutineId next_id_{1};
    std::size_t completed_count_{0};

    std::atomic<bool> started_{false};
    std::atomic<bool> stop_requested_{false};
    // pending: 已入队未执行；active: 正在执行。
    std::atomic<std::size_t> pending_tasks_{0};
    std::atomic<std::size_t> active_tasks_{0};
    std::atomic<std::size_t> idle_switches_{0};
    std::atomic<std::size_t> steal_count_{0};
    std::atomic<std::size_t> enqueue_local_count_{0};
    std::atomic<std::size_t> enqueue_global_count_{0};
    std::atomic<std::size_t> enqueue_fast_count_{0};
    std::atomic<std::size_t> dequeue_local_fast_count_{0};
    std::atomic<std::size_t> dequeue_local_count_{0};
    std::atomic<std::size_t> dequeue_global_count_{0};
    std::atomic<std::size_t> dequeue_steal_count_{0};
    std::atomic<std::uint64_t> state_lock_wait_ns_total_{0};
    std::atomic<std::size_t> state_lock_wait_samples_{0};

    // 用于 wait_idle 的条件变量通知。
    mutable std::mutex idle_mutex_;
    std::condition_variable idle_cv_;
};

}  // namespace rpc::runtime
