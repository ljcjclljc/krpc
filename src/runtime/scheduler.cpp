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

}  // namespace

CoroutineScheduler::~CoroutineScheduler() {
    stop();
}

CoroutineId CoroutineScheduler::schedule(CoroutineCallback callback, std::size_t stack_size) {
    CoroutineId id = 0;
    std::size_t worker_index = 0;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (!started_.load(std::memory_order_acquire) || stop_requested_.load(std::memory_order_acquire) || workers_.empty()) {
            throw std::logic_error("CoroutineScheduler::schedule requires started worker pool");
        }

        id = allocate_id();
        worker_index = dispatch_cursor_ % workers_.size();
        ++dispatch_cursor_;
    }

    auto coroutine = std::make_shared<Coroutine>(id, std::move(callback), stack_size);

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (!started_.load(std::memory_order_acquire) || stop_requested_.load(std::memory_order_acquire) || workers_.empty()) {
            recycled_ids_.push_back(id);
            throw std::logic_error("CoroutineScheduler is stopping, schedule rejected");
        }

        coroutines_.emplace(id, coroutine);
        pending_tasks_.fetch_add(1, std::memory_order_relaxed);

        WorkerContext& worker = *workers_[worker_index % workers_.size()];
        {
            std::lock_guard<std::mutex> worker_lock(worker.mutex);
            worker.queue.push_back(id);
        }
        worker.cv.notify_one();
    }

    return id;
}

void CoroutineScheduler::start(std::size_t worker_threads) {
    worker_threads = normalize_worker_threads(worker_threads);

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (started_.load(std::memory_order_acquire)) {
            return;
        }

        stop_requested_.store(false, std::memory_order_release);
        workers_.clear();
        workers_.reserve(worker_threads);
        for (std::size_t i = 0; i < worker_threads; ++i) {
            workers_.push_back(std::make_unique<WorkerContext>());
        }

        started_.store(true, std::memory_order_release);
    }

    for (std::size_t index = 0; index < worker_threads; ++index) {
        workers_[index]->thread = std::thread(&CoroutineScheduler::worker_loop, this, index);
    }

    for (auto& worker : workers_) {
        worker->cv.notify_all();
    }
}

void CoroutineScheduler::stop() {
    if (!started_.load(std::memory_order_acquire)) {
        return;
    }

    wait_idle();
    stop_requested_.store(true, std::memory_order_release);

    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        for (auto& worker : workers_) {
            worker->cv.notify_all();
            if (worker->thread.joinable()) {
                threads.emplace_back(std::move(worker->thread));
            }
        }
    }

    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        workers_.clear();
        dispatch_cursor_ = 0;
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
    std::vector<CoroutineId> pending_remove;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
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

bool CoroutineScheduler::running() const noexcept {
    return started_.load(std::memory_order_acquire);
}

CoroutineId CoroutineScheduler::allocate_id() {
    if (!recycled_ids_.empty()) {
        const CoroutineId id = recycled_ids_.back();
        recycled_ids_.pop_back();
        return id;
    }

    const CoroutineId id = next_id_;
    ++next_id_;
    return id;
}

void CoroutineScheduler::recycle(CoroutineId id) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    const auto it = coroutines_.find(id);
    if (it == coroutines_.end()) {
        return;
    }

    coroutines_.erase(it);
    recycled_ids_.push_back(id);
    ++completed_count_;
}

void CoroutineScheduler::worker_loop(std::size_t worker_index) {
    WorkerContext* worker = nullptr;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (worker_index >= workers_.size()) {
            return;
        }
        worker = workers_[worker_index].get();
    }

    auto idle_coroutine = std::make_shared<Coroutine>(0, [this]() {
        // Idle 协程：空闲时短暂 sleep + yield，避免 worker 忙等空转。
        while (!stop_requested_.load(std::memory_order_acquire)) {
            idle_switches_.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            Coroutine::yield_current();
        }
    });

    while (true) {
        CoroutineId id = 0;

        {
            std::unique_lock<std::mutex> lock(worker->mutex);
            while (worker->queue.empty()) {
                if (stop_requested_.load(std::memory_order_acquire)) {
                    return;
                }

                // 队列为空先跑一次 idle 协程，再等待条件变量唤醒。
                lock.unlock();
                if (idle_coroutine->state() != CoroutineState::TERM) {
                    idle_coroutine->resume();
                }
                lock.lock();

                worker->cv.wait_for(lock, std::chrono::milliseconds(1));
            }

            id = worker->queue.front();
            worker->queue.pop_front();
        }

        pending_tasks_.fetch_sub(1, std::memory_order_relaxed);
        execute_coroutine(id, worker_index);
    }
}

void CoroutineScheduler::execute_coroutine(CoroutineId id, std::size_t worker_index) {
    std::shared_ptr<Coroutine> coroutine;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        const auto it = coroutines_.find(id);
        if (it == coroutines_.end()) {
            notify_idle_waiters();
            return;
        }
        coroutine = it->second;
    }

    if (coroutine->state() == CoroutineState::TERM) {
        // 兼容外部提前将协程置 TERM 的场景。
        recycle(id);
        notify_idle_waiters();
        return;
    }

    active_tasks_.fetch_add(1, std::memory_order_relaxed);
    try {
        coroutine->resume();

        if (coroutine->state() == CoroutineState::READY) {
            // 协程主动 yield，重新入队等待下次调度。
            enqueue_ready(id, worker_index);
        } else if (coroutine->state() == CoroutineState::TERM) {
            // 协程执行完成，立即回收。
            recycle(id);
        }
    } catch (...) {
        recycle(id);
    }

    active_tasks_.fetch_sub(1, std::memory_order_relaxed);
    notify_idle_waiters();
}

void CoroutineScheduler::enqueue_ready(CoroutineId id, std::size_t worker_index) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (!started_.load(std::memory_order_acquire) || stop_requested_.load(std::memory_order_acquire) || workers_.empty()) {
        throw std::logic_error("CoroutineScheduler is stopping, cannot re-enqueue READY task");
    }

    if (worker_index >= workers_.size()) {
        worker_index = dispatch_cursor_ % workers_.size();
        ++dispatch_cursor_;
    }

    WorkerContext& worker = *workers_[worker_index];
    {
        std::lock_guard<std::mutex> worker_lock(worker.mutex);
        worker.queue.push_back(id);
    }
    pending_tasks_.fetch_add(1, std::memory_order_relaxed);
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
