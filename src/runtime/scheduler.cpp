#include "rpc/runtime/scheduler.h"

#include <chrono>
#include <stdexcept>

namespace rpc::runtime {

namespace {

std::size_t normalize_worker_threads(std::size_t worker_threads) {
    // 0 表示自动探测，至少返回 1，避免创建 0 个 worker。
    if (worker_threads != 0) {
        return worker_threads;
    }

    const std::size_t hardware = static_cast<std::size_t>(std::thread::hardware_concurrency());
    return hardware == 0 ? 1 : hardware;
}

constexpr std::size_t kInvalidWorkerIndex = static_cast<std::size_t>(-1);
thread_local CoroutineScheduler* t_current_scheduler = nullptr;
thread_local std::size_t t_current_worker_index = kInvalidWorkerIndex;

template <typename Mutex, typename WaitAtomic, typename SampleAtomic>
std::unique_lock<Mutex> timed_lock(
    Mutex& mutex,
    WaitAtomic& wait_ns_total,
    SampleAtomic& wait_samples
) {
    const auto wait_started = std::chrono::steady_clock::now();
    std::unique_lock<Mutex> lock(mutex);
    const auto waited_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - wait_started
    ).count();
    wait_ns_total.fetch_add(static_cast<std::uint64_t>(waited_ns), std::memory_order_relaxed);
    wait_samples.fetch_add(1, std::memory_order_relaxed);
    return lock;
}

}  // namespace

CoroutineScheduler::~CoroutineScheduler() {
    stop();
}

CoroutineId CoroutineScheduler::schedule(CoroutineCallback callback, std::size_t stack_size) {
    // 两阶段提交：
    // 1) 在锁内完成可调度性检查与 ID 分配
    // 2) 在锁外构造协程对象，缩短全局锁持有时长
    // 3) 再次加锁写入对象表并入全局/本地队列
    CoroutineId id = 0;
    bool enqueue_to_local = false;
    std::size_t local_worker_index = 0;

    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        if (!started_.load(std::memory_order_acquire) || stop_requested_.load(std::memory_order_acquire) || workers_.empty()) {
            throw std::logic_error("CoroutineScheduler::schedule requires started worker pool");
        }

        id = allocate_id();
        // 若调用方就在当前调度器的 worker 线程内，则优先走线程本地队列。
        enqueue_to_local = (t_current_scheduler == this && t_current_worker_index < workers_.size());
        if (enqueue_to_local) {
            local_worker_index = t_current_worker_index;
        }
    }

    // 协程对象构造放在大锁外，减少全局状态锁持有时间。
    auto coroutine = std::make_shared<Coroutine>(id, std::move(callback), stack_size);

    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        // 二次检查：防止第一阶段通过后，调度器在并发 stop() 中发生状态变化。
        if (!started_.load(std::memory_order_acquire) || stop_requested_.load(std::memory_order_acquire) || workers_.empty()) {
            recycled_ids_.push_back(id);
            throw std::logic_error("CoroutineScheduler is stopping, schedule rejected");
        }

        coroutines_.emplace(id, coroutine);
        if (enqueue_to_local && local_worker_index < workers_.size()) {
            coroutine_last_worker_[id] = local_worker_index;
        } else {
            const std::size_t hinted_worker = dispatch_cursor_ % workers_.size();
            ++dispatch_cursor_;
            coroutine_last_worker_[id] = hinted_worker;
        }
        // pending 代表“已经入队但尚未被 worker 取走执行”的任务数。
        pending_tasks_.fetch_add(1, std::memory_order_relaxed);

        if (enqueue_to_local && local_worker_index < workers_.size()) {
            WorkerContext& worker = *workers_[local_worker_index];
            {
                std::lock_guard<std::mutex> worker_lock(worker.mutex);
                worker.local_queue.push_back(id);
            }
            enqueue_local_count_.fetch_add(1, std::memory_order_relaxed);
            worker.cv.notify_one();
            // 本地队列增长时唤醒其它 worker，便于触发工作窃取。
            for (std::size_t i = 0; i < workers_.size(); ++i) {
                if (i == local_worker_index) {
                    continue;
                }
                workers_[i]->cv.notify_one();
            }
        } else {
            {
                std::lock_guard<std::mutex> global_lock(global_queue_mutex_);
                global_queue_.push_back(id);
            }
            enqueue_global_count_.fetch_add(1, std::memory_order_relaxed);
            for (auto& worker : workers_) {
                worker->cv.notify_one();
            }
        }
    }

    return id;
}

bool CoroutineScheduler::resume(CoroutineId id) {
    WorkerContext* target_worker = nullptr;
    bool need_notify = false;

    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        if (!started_.load(std::memory_order_acquire)
            || stop_requested_.load(std::memory_order_acquire)
            || workers_.empty()) {
            return false;
        }

        const auto it = coroutines_.find(id);
        if (it == coroutines_.end()) {
            return false;
        }

        const std::shared_ptr<Coroutine>& coroutine = it->second;
        const CoroutineState state = coroutine->state();

        if (state == CoroutineState::RUNNING) {
            // 恢复请求早于协程挂起：先记录，待 execute_coroutine 看到 WAITING 后补发。
            if (running_coroutines_.find(id) != running_coroutines_.end()) {
                pending_resumes_.insert(id);
                return true;
            }
            return false;
        }

        if (!coroutine->try_mark_ready_from_waiting()) {
            return false;
        }

        // 若协程尚在当前 worker 执行栈回退中，则让 execute_coroutine 统一入队，避免重复投递。
        if (running_coroutines_.find(id) != running_coroutines_.end()) {
            return true;
        }

        std::size_t worker_index = 0;
        const auto worker_it = coroutine_last_worker_.find(id);
        if (worker_it != coroutine_last_worker_.end() && worker_it->second < workers_.size()) {
            worker_index = worker_it->second;
        } else {
            worker_index = dispatch_cursor_ % workers_.size();
            ++dispatch_cursor_;
            coroutine_last_worker_[id] = worker_index;
        }

        WorkerContext& worker = *workers_[worker_index];
        {
            std::lock_guard<std::mutex> worker_lock(worker.mutex);
            // IO/WAITING 恢复路径进入本地快队列，优先于常规任务执行。
            worker.local_fast_queue.push_back(id);
        }
        pending_tasks_.fetch_add(1, std::memory_order_relaxed);
        enqueue_fast_count_.fetch_add(1, std::memory_order_relaxed);
        target_worker = &worker;
        need_notify = true;
    }

    if (need_notify && target_worker != nullptr) {
        target_worker->cv.notify_one();
    }
    return true;
}

void CoroutineScheduler::start(std::size_t worker_threads) {
    // 统一归一化 worker 数，保证至少启动 1 个线程。
    worker_threads = normalize_worker_threads(worker_threads);

    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        // 已启动时直接返回，保持 start() 幂等。
        if (started_.load(std::memory_order_acquire)) {
            return;
        }
        // 初始化状态，准备启动 worker 线程。
        stop_requested_.store(false, std::memory_order_release); // 确保 stop 标志重置，允许 start/stop 循环调用。
        workers_.clear();
        {
            std::lock_guard<std::mutex> global_lock(global_queue_mutex_);
            global_queue_.clear();
        }
        pending_tasks_.store(0, std::memory_order_release);
        active_tasks_.store(0, std::memory_order_release);
        idle_switches_.store(0, std::memory_order_release);
        steal_count_.store(0, std::memory_order_release);
        enqueue_local_count_.store(0, std::memory_order_release);
        enqueue_global_count_.store(0, std::memory_order_release);
        enqueue_fast_count_.store(0, std::memory_order_release);
        dequeue_local_fast_count_.store(0, std::memory_order_release);
        dequeue_local_count_.store(0, std::memory_order_release);
        dequeue_global_count_.store(0, std::memory_order_release);
        dequeue_steal_count_.store(0, std::memory_order_release);
        state_lock_wait_ns_total_.store(0, std::memory_order_release);
        state_lock_wait_samples_.store(0, std::memory_order_release);
        // 预分配 WorkerContext，减少后续调度时的动态分配开销。
        workers_.reserve(worker_threads);
        for (std::size_t i = 0; i < worker_threads; ++i) {
            workers_.push_back(std::make_unique<WorkerContext>());
        }

        started_.store(true, std::memory_order_release);
    }

    // 线程创建放在锁外，避免长时间占用 state_mutex_。
    for (std::size_t index = 0; index < worker_threads; ++index) {
        workers_[index]->thread = std::thread(&CoroutineScheduler::worker_loop, this, index);
    }

    // 主动唤醒一次，确保初始等待态 worker 尽快进入循环。
    for (auto& worker : workers_) {
        worker->cv.notify_all();
    }
}

void CoroutineScheduler::stop() {
    if (!started_.load(std::memory_order_acquire)) {
        return;
    }

    // 先排空任务，再进入停止流程，避免中途截断任务。
    wait_idle();
    stop_requested_.store(true, std::memory_order_release);

    std::vector<std::thread> threads;
    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        for (auto& worker : workers_) {
            worker->cv.notify_all();
            if (worker->thread.joinable()) {
                threads.emplace_back(std::move(worker->thread));
            }
        }
    }

    // 在锁外 join，避免阻塞其它需要 state_mutex_ 的路径。
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        workers_.clear();
        dispatch_cursor_ = 0;
    }
    {
        std::lock_guard<std::mutex> global_lock(global_queue_mutex_);
        global_queue_.clear();
    }

    started_.store(false, std::memory_order_release);
    stop_requested_.store(false, std::memory_order_release);
    notify_idle_waiters();
}

void CoroutineScheduler::wait_idle() {
    // 以“pending==0 且 active==0”为调度空闲判定条件。
    std::unique_lock<std::mutex> lock(idle_mutex_);
    idle_cv_.wait(lock, [this]() {
        return pending_tasks_.load(std::memory_order_acquire) == 0
            && active_tasks_.load(std::memory_order_acquire) == 0;
    });
}

void CoroutineScheduler::recycle_terminated() {
    // 先收集待回收 ID，避免在持锁遍历时做 erase 导致迭代器失效。
    std::vector<CoroutineId> pending_remove;

    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        pending_remove.reserve(coroutines_.size());
        for (const auto& [id, coroutine] : coroutines_) {
            if (coroutine->state() == CoroutineState::TERM) {
                pending_remove.push_back(id);
            }
        }
    }

    for (const CoroutineId id : pending_remove) {
        recycle(id);
    }

    notify_idle_waiters();
}

std::size_t CoroutineScheduler::alive_count() const noexcept {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return coroutines_.size();
}

std::size_t CoroutineScheduler::completed_count() const noexcept {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return completed_count_;
}

std::size_t CoroutineScheduler::pending_count() const noexcept {
    return pending_tasks_.load(std::memory_order_acquire);
}

std::size_t CoroutineScheduler::worker_count() const noexcept {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return workers_.size();
}

std::size_t CoroutineScheduler::idle_switch_count() const noexcept {
    return idle_switches_.load(std::memory_order_acquire);
}

std::size_t CoroutineScheduler::steal_count() const noexcept {
    return steal_count_.load(std::memory_order_acquire);
}

SchedulerProfileSnapshot CoroutineScheduler::profile_snapshot() const noexcept {
    SchedulerProfileSnapshot snapshot;
    snapshot.enqueue_local = enqueue_local_count_.load(std::memory_order_acquire);
    snapshot.enqueue_global = enqueue_global_count_.load(std::memory_order_acquire);
    snapshot.enqueue_fast = enqueue_fast_count_.load(std::memory_order_acquire);

    snapshot.dequeue_local_fast = dequeue_local_fast_count_.load(std::memory_order_acquire);
    snapshot.dequeue_local = dequeue_local_count_.load(std::memory_order_acquire);
    snapshot.dequeue_global = dequeue_global_count_.load(std::memory_order_acquire);
    snapshot.dequeue_steal = dequeue_steal_count_.load(std::memory_order_acquire);

    snapshot.state_lock_wait_ns_total = state_lock_wait_ns_total_.load(std::memory_order_acquire);
    snapshot.state_lock_wait_samples = state_lock_wait_samples_.load(std::memory_order_acquire);
    snapshot.state_lock_wait_ns_avg = snapshot.state_lock_wait_samples == 0
        ? 0
        : (snapshot.state_lock_wait_ns_total / snapshot.state_lock_wait_samples);
    return snapshot;
}

bool CoroutineScheduler::running() const noexcept {
    return started_.load(std::memory_order_acquire);
}
// 协程 ID 分配器：优先复用回收 ID，避免 ID 无界增长。
CoroutineId CoroutineScheduler::allocate_id() {
    // 优先复用已回收 ID，避免 ID 无界增长。
    if (!recycled_ids_.empty()) {
        const CoroutineId id = recycled_ids_.back();
        recycled_ids_.pop_back();
        return id;
    }

    const CoroutineId id = next_id_;
    ++next_id_;
    return id;
}
// 协程回收：将 ID 放回复用池，并从对象表中移除。
void CoroutineScheduler::recycle(CoroutineId id) {
    // 协程对象生命周期由调度器统一收口。
    auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
    const auto it = coroutines_.find(id);
    if (it == coroutines_.end()) {
        return;
    }

    coroutines_.erase(it);
    recycled_ids_.push_back(id);
    coroutine_last_worker_.erase(id);
    running_coroutines_.erase(id);
    started_coroutines_.erase(id);
    pending_resumes_.erase(id);
    ++completed_count_;
}

void CoroutineScheduler::worker_loop(std::size_t worker_index) {
    if (worker_index >= workers_.size() || workers_[worker_index] == nullptr) {
        return;
    }
    // workers_ 在 start 后到 stop join 前不变更，可直接缓存本地引用，减少全局锁竞争。
    WorkerContext& worker = *workers_[worker_index];

    t_current_scheduler = this;
    t_current_worker_index = worker_index;

    while (true) {
        CoroutineId id = 0;
        if (!try_dequeue_next(worker_index, worker, id)) {
            if (stop_requested_.load(std::memory_order_acquire)) {
                t_current_scheduler = nullptr;
                t_current_worker_index = kInvalidWorkerIndex;
                return;
            }

            // 空闲时执行小步等待，避免 busy spin；同时累加 idle 观测计数。
            idle_switches_.fetch_add(1, std::memory_order_relaxed);
            std::unique_lock<std::mutex> lock(worker.mutex);
            worker.cv.wait_for(lock, std::chrono::milliseconds(1), [this, &worker]() {
                return stop_requested_.load(std::memory_order_acquire)
                    || !worker.local_fast_queue.empty()
                    || !worker.local_queue.empty();
            });
            continue;
        }

        // 任务从队列出队，pending 对应减少。
        pending_tasks_.fetch_sub(1, std::memory_order_relaxed);
        execute_coroutine(id, worker_index);
    }
}

bool CoroutineScheduler::try_dequeue_local_fast(WorkerContext& worker, CoroutineId& id) {
    std::lock_guard<std::mutex> lock(worker.mutex);
    if (!worker.local_fast_queue.empty()) {
        id = worker.local_fast_queue.front();
        worker.local_fast_queue.pop_front();
        dequeue_local_fast_count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

bool CoroutineScheduler::try_dequeue_local(WorkerContext& worker, CoroutineId& id) {
    std::lock_guard<std::mutex> lock(worker.mutex);
    if (!worker.local_queue.empty()) {
        id = worker.local_queue.front();
        worker.local_queue.pop_front();
        dequeue_local_count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

bool CoroutineScheduler::try_dequeue_global(CoroutineId& id) {
    std::lock_guard<std::mutex> lock(global_queue_mutex_);
    if (global_queue_.empty()) {
        return false;
    }
    id = global_queue_.front();
    global_queue_.pop_front();
    dequeue_global_count_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool CoroutineScheduler::try_steal_from_other_worker(std::size_t thief_index, CoroutineId& id) {
    if (workers_.size() <= 1 || thief_index >= workers_.size()) {
        return false;
    }

    const auto try_steal_once = [&](bool require_rich_victim) -> bool {
        for (std::size_t offset = 1; offset < workers_.size(); ++offset) {
            const std::size_t victim_index = (thief_index + offset) % workers_.size();
            WorkerContext* victim = workers_[victim_index].get();
            if (victim == nullptr || victim == workers_[thief_index].get()) {
                continue;
            }

            CoroutineId candidate = 0;
            {
                std::lock_guard<std::mutex> lock(victim->mutex);
                if (victim->local_queue.empty()) {
                    continue;
                }
                if (require_rich_victim && victim->local_queue.size() <= 1) {
                    continue;
                }

                candidate = victim->local_queue.back();
                victim->local_queue.pop_back();
            }

            bool started = false;
            {
                auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
                started = started_coroutines_.find(candidate) != started_coroutines_.end();
            }

            if (started) {
                // 已执行过的协程保持线程亲和，不参与窃取，避免跨线程恢复风险。
                std::lock_guard<std::mutex> lock(victim->mutex);
                victim->local_queue.push_back(candidate);
                continue;
            }

            id = candidate;
            steal_count_.fetch_add(1, std::memory_order_relaxed);
            dequeue_steal_count_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    };

    // 第一轮：优先从任务量较多的受害者偷取，避免把单任务 worker 直接掏空。
    if (try_steal_once(true)) {
        return true;
    }

    // 第二轮：若没有“富余受害者”，允许偷取单任务避免空转。
    if (try_steal_once(false)) {
        return true;
    }

    return false;
}

bool CoroutineScheduler::try_dequeue_next(std::size_t worker_index, WorkerContext& worker, CoroutineId& id) {
    if (try_dequeue_local_fast(worker, id)) {
        return true;
    }
    if (try_dequeue_global(id)) {
        return true;
    }
    if (try_dequeue_local(worker, id)) {
        return true;
    }
    if (try_steal_from_other_worker(worker_index, id)) {
        return true;
    }
    return try_dequeue_global(id);
}

void CoroutineScheduler::execute_coroutine(CoroutineId id, std::size_t worker_index) {
    // 先复制 shared_ptr，确保执行期间协程对象生命周期稳定。
    std::shared_ptr<Coroutine> coroutine;
    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        const auto it = coroutines_.find(id);
        if (it == coroutines_.end()) {
            notify_idle_waiters();
            return;
        }
        coroutine = it->second;
        coroutine_last_worker_[id] = worker_index;
        running_coroutines_.insert(id);
        started_coroutines_.insert(id);
    }

    if (coroutine->state() == CoroutineState::TERM) {
        // 兼容外部提前将协程置 TERM 的场景。
        recycle(id);
        notify_idle_waiters();
        return;
    }

    active_tasks_.fetch_add(1, std::memory_order_relaxed);
    bool should_requeue_ready = false;
    bool should_recycle = false;
    try {
        coroutine->resume();

        if (coroutine->state() == CoroutineState::READY) {
            // 协程主动 yield，重新入队等待下次调度。
            should_requeue_ready = true;
        } else if (coroutine->state() == CoroutineState::WAITING) {
            bool should_resume_after_wait = false;
            {
                auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
                const auto pending_it = pending_resumes_.find(id);
                if (pending_it != pending_resumes_.end()) {
                    pending_resumes_.erase(pending_it);
                    should_resume_after_wait = true;
                }
            }

            if (should_resume_after_wait && coroutine->try_mark_ready_from_waiting()) {
                should_requeue_ready = true;
            }
        } else if (coroutine->state() == CoroutineState::TERM) {
            // 协程执行完成，立即回收。
            should_recycle = true;
        }
    } catch (...) {
        // 协程异常视为终止并回收，避免异常逃逸导致 worker 线程退出。
        should_recycle = true;
    }

    {
        auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
        running_coroutines_.erase(id);
    }

    if (should_requeue_ready) {
        enqueue_ready(id, worker_index);
    } else if (should_recycle) {
        recycle(id);
    }

    active_tasks_.fetch_sub(1, std::memory_order_relaxed);
    notify_idle_waiters();
}
// 将 READY 状态的协程重新入队，供 worker 线程继续调度执行。
void CoroutineScheduler::enqueue_ready(CoroutineId id, std::size_t worker_index) {
    auto lock = timed_lock(state_mutex_, state_lock_wait_ns_total_, state_lock_wait_samples_);
    if (!started_.load(std::memory_order_acquire) || stop_requested_.load(std::memory_order_acquire) || workers_.empty()) {
        throw std::logic_error("CoroutineScheduler is stopping, cannot re-enqueue READY task");
    }

    if (worker_index >= workers_.size()) {
        worker_index = dispatch_cursor_ % workers_.size();
        ++dispatch_cursor_;
    }
    coroutine_last_worker_[id] = worker_index;

    // yield 续跑回投到线程本地普通队列；快队列只留给 WAITING->READY 恢复。
    WorkerContext& worker = *workers_[worker_index];
    {
        std::lock_guard<std::mutex> worker_lock(worker.mutex);
        worker.local_queue.push_back(id);
    }
    pending_tasks_.fetch_add(1, std::memory_order_relaxed);
    enqueue_local_count_.fetch_add(1, std::memory_order_relaxed);
    worker.cv.notify_one();
}

void CoroutineScheduler::notify_idle_waiters() {
    // 仅在完全空闲时广播，避免无效唤醒。
    if (pending_tasks_.load(std::memory_order_acquire) == 0
        && active_tasks_.load(std::memory_order_acquire) == 0) {
        std::lock_guard<std::mutex> lock(idle_mutex_);
        idle_cv_.notify_all();
    }
}

}  // namespace rpc::runtime
