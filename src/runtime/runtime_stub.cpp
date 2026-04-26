#include "rpc/runtime/runtime.h"

// 文件用途：
// 实现 runtime 外观层：
// - 统一管理 scheduler + io_manager 生命周期
// - 提供 submit / io await / deadline context / hook enable 包装接口

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/hook.h"
#include "rpc/runtime/io_manager.h"
#include "rpc/runtime/request_context.h"
#include "rpc/runtime/scheduler.h"

namespace rpc::runtime {

namespace {

struct RuntimeHost {
    CoroutineScheduler scheduler;
    std::unique_ptr<IOManager> io_manager;
};

std::mutex g_runtime_mutex;
std::unique_ptr<RuntimeHost> g_runtime_host;

IOEvent to_io_event(RuntimeIoEvent event) {
    switch (event) {
        case RuntimeIoEvent::Read:
            return IOEvent::Read;
        case RuntimeIoEvent::Write:
            return IOEvent::Write;
        default:
            return IOEvent::Read;
    }
}

struct AwaitState {
    std::atomic<std::uint8_t> result{0};  // 0: pending, 1: ready, 2: timeout
};

}  // namespace

void start_runtime(RuntimeStartOptions options) {
    std::lock_guard<std::mutex> lock(g_runtime_mutex);
    if (g_runtime_host) {
        return;
    }

    auto host = std::make_unique<RuntimeHost>();
    host->scheduler.start(options.worker_threads);

    try {
        host->io_manager = std::make_unique<IOManager>(host->scheduler, options.io_max_events);
        host->io_manager->start();
        set_hook_io_manager(host->io_manager.get());
    } catch (...) {
        set_hook_io_manager(nullptr);
        if (host->io_manager) {
            host->io_manager->stop();
        }
        host->scheduler.stop();
        throw;
    }

    g_runtime_host = std::move(host);
}

void stop_runtime() {
    std::unique_ptr<RuntimeHost> host;
    {
        std::lock_guard<std::mutex> lock(g_runtime_mutex);
        host = std::move(g_runtime_host);
    }

    if (!host) {
        return;
    }

    set_hook_io_manager(nullptr);
    if (host->io_manager) {
        host->io_manager->stop();
    }
    host->scheduler.stop();
}

bool runtime_ready() noexcept {
    std::lock_guard<std::mutex> lock(g_runtime_mutex);
    return g_runtime_host != nullptr;
}

RuntimeSchedulerSnapshot runtime_scheduler_snapshot() noexcept {
    std::lock_guard<std::mutex> lock(g_runtime_mutex);
    RuntimeSchedulerSnapshot snapshot;
    if (!g_runtime_host) {
        return snapshot;
    }

    snapshot.running = g_runtime_host->scheduler.running();
    snapshot.worker_count = g_runtime_host->scheduler.worker_count();
    snapshot.pending_tasks = g_runtime_host->scheduler.pending_count();
    snapshot.alive_coroutines = g_runtime_host->scheduler.alive_count();
    snapshot.completed_coroutines = g_runtime_host->scheduler.completed_count();
    snapshot.idle_switches = g_runtime_host->scheduler.idle_switch_count();
    snapshot.steal_count = g_runtime_host->scheduler.steal_count();
    const SchedulerProfileSnapshot profile = g_runtime_host->scheduler.profile_snapshot();
    snapshot.enqueue_local = profile.enqueue_local;
    snapshot.enqueue_global = profile.enqueue_global;
    snapshot.enqueue_fast = profile.enqueue_fast;
    snapshot.dequeue_local_fast = profile.dequeue_local_fast;
    snapshot.dequeue_local = profile.dequeue_local;
    snapshot.dequeue_global = profile.dequeue_global;
    snapshot.dequeue_steal = profile.dequeue_steal;
    snapshot.scheduler_state_lock_wait_ns_total = profile.state_lock_wait_ns_total;
    snapshot.scheduler_state_lock_wait_samples = profile.state_lock_wait_samples;
    snapshot.scheduler_state_lock_wait_ns_avg = profile.state_lock_wait_ns_avg;
    return snapshot;
}

RuntimeTaskId submit(RuntimeTask task, std::size_t stack_size) {
    if (!task) {
        throw std::invalid_argument("runtime::submit task cannot be empty");
    }

    std::lock_guard<std::mutex> lock(g_runtime_mutex);
    if (!g_runtime_host) {
        throw std::logic_error("runtime is not started");
    }

    const std::size_t effective_stack = stack_size == 0
        ? Coroutine::kDefaultStackSize
        : stack_size;
    return static_cast<RuntimeTaskId>(g_runtime_host->scheduler.schedule(std::move(task), effective_stack));
}

void wait_runtime_idle() {
    std::lock_guard<std::mutex> lock(g_runtime_mutex);
    if (!g_runtime_host) {
        return;
    }
    g_runtime_host->scheduler.wait_idle();
}

RuntimeIoAwaitResult await_io(
    int fd,
    RuntimeIoEvent event,
    std::optional<std::chrono::milliseconds> timeout
) {
    if (fd < 0) {
        return RuntimeIoAwaitResult::Failed;
    }

    IOManager* io_manager = nullptr;
    {
        std::lock_guard<std::mutex> lock(g_runtime_mutex);
        if (!g_runtime_host || !g_runtime_host->io_manager) {
            return RuntimeIoAwaitResult::Failed;
        }
        io_manager = g_runtime_host->io_manager.get();
    }

    Coroutine* current = Coroutine::current();
    if (current == nullptr) {
        return RuntimeIoAwaitResult::Failed;
    }
    const CoroutineId waiting_coroutine_id = current->id();

    auto state = std::make_shared<AwaitState>();
    const IOEvent io_event = to_io_event(event);
    if (!io_manager->add_event(fd, io_event, [io_manager, waiting_coroutine_id, state]() {
            std::uint8_t expected = 0;
            if (state->result.compare_exchange_strong(
                    expected,
                    1,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                io_manager->resume_coroutine(waiting_coroutine_id);
            }
        })) {
        return RuntimeIoAwaitResult::Failed;
    }

    TimerId timer_id = 0;
    if (timeout.has_value()) {
        timer_id = io_manager->add_timer(*timeout, [io_manager, waiting_coroutine_id, state]() {
            std::uint8_t expected = 0;
            if (state->result.compare_exchange_strong(
                    expected,
                    2,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                io_manager->resume_coroutine(waiting_coroutine_id);
            }
        });
    }

    Coroutine::yield_current_waiting();

    if (timer_id != 0) {
        io_manager->cancel_timer(timer_id);
    }

    const std::uint8_t result = state->result.load(std::memory_order_acquire);
    if (result == 1) {
        return RuntimeIoAwaitResult::Ready;
    }
    if (result == 2) {
        io_manager->del_event(fd, io_event);
        return RuntimeIoAwaitResult::Timeout;
    }
    return RuntimeIoAwaitResult::Failed;
}

DeadlineContext create_deadline_context(std::string request_id, std::chrono::milliseconds timeout) {
    return RequestContext::create_with_timeout(std::move(request_id), timeout);
}

DeadlineContext current_deadline_context() {
    return current_request_context();
}

void set_deadline_context_value(const DeadlineContext& context, std::string key, std::string value) {
    if (!context) {
        return;
    }
    context->set_value(std::move(key), std::move(value));
}

std::optional<std::string> deadline_context_value(const DeadlineContext& context, const std::string& key) {
    if (!context) {
        return std::nullopt;
    }
    return context->value(key);
}

ScopedDeadlineContext::ScopedDeadlineContext(DeadlineContext context)
    : previous_(current_request_context()) {
    set_current_request_context(std::move(context));
}

ScopedDeadlineContext::~ScopedDeadlineContext() {
    set_current_request_context(std::move(previous_));
}

void set_runtime_hook_enabled(bool enabled) noexcept {
    set_hook_enabled(enabled);
}

bool runtime_hook_enabled() noexcept {
    return hook_enabled();
}

ScopedRuntimeHookEnable::ScopedRuntimeHookEnable(bool enabled) noexcept
    : previous_(hook_enabled()) {
    set_hook_enabled(enabled);
}

ScopedRuntimeHookEnable::~ScopedRuntimeHookEnable() {
    set_hook_enabled(previous_);
}

void init_runtime() {
    start_runtime();
}

void shutdown_runtime() {
    stop_runtime();
}

}  // namespace rpc::runtime
