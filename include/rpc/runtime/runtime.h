#pragma once

// 文件用途：
// 声明 runtime 外观层接口，统一对外暴露：
// - start/stop
// - submit
// - io await
// - deadline context
// - hook enable

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace rpc::runtime {

class RequestContext;

struct RuntimeStartOptions {
    // 调度 worker 数量；0 表示自动探测（至少 1）。
    std::size_t worker_threads{0};
    // IOManager 每轮 epoll_wait 的最大事件数量；0 时内部回退为 1。
    std::size_t io_max_events{256};
};

using RuntimeTask = std::function<void()>;
using RuntimeTaskId = std::uint64_t;
using DeadlineContext = std::shared_ptr<RequestContext>;

enum class RuntimeIoEvent : std::uint8_t {
    Read = 1,
    Write = 2,
};

enum class RuntimeIoAwaitResult : std::uint8_t {
    Ready = 0,
    Timeout = 1,
    Failed = 2,
};

struct RuntimeSchedulerSnapshot {
    bool running{false};
    std::size_t worker_count{0};
    std::size_t pending_tasks{0};
    std::size_t alive_coroutines{0};
    std::size_t completed_coroutines{0};
    std::size_t idle_switches{0};
    std::size_t steal_count{0};
    std::size_t enqueue_local{0};
    std::size_t enqueue_global{0};
    std::size_t enqueue_fast{0};
    std::size_t dequeue_local_fast{0};
    std::size_t dequeue_local{0};
    std::size_t dequeue_global{0};
    std::size_t dequeue_steal{0};
    std::uint64_t scheduler_state_lock_wait_ns_total{0};
    std::size_t scheduler_state_lock_wait_samples{0};
    std::uint64_t scheduler_state_lock_wait_ns_avg{0};
};

// 启动 runtime（幂等）。
void start_runtime(RuntimeStartOptions options = {});
// 停止 runtime（幂等）。
void stop_runtime();
// runtime 是否已启动。
bool runtime_ready() noexcept;
// 读取调度器运行态快照（可观测性）。
RuntimeSchedulerSnapshot runtime_scheduler_snapshot() noexcept;

// 提交任务到协程调度器执行，返回任务 ID。
RuntimeTaskId submit(RuntimeTask task, std::size_t stack_size = 0);
// 等待 runtime 调度器空闲（pending=0 且 active=0）。
void wait_runtime_idle();

// 在当前协程上下文等待 fd 事件（可选超时）。
RuntimeIoAwaitResult await_io(
    int fd,
    RuntimeIoEvent event,
    std::optional<std::chrono::milliseconds> timeout = std::nullopt
);

// deadline context 外观接口。
DeadlineContext create_deadline_context(std::string request_id, std::chrono::milliseconds timeout);
DeadlineContext current_deadline_context();
void set_deadline_context_value(const DeadlineContext& context, std::string key, std::string value);
std::optional<std::string> deadline_context_value(const DeadlineContext& context, const std::string& key);

// 作用域 deadline context 绑定。
class ScopedDeadlineContext {
public:
    explicit ScopedDeadlineContext(DeadlineContext context);
    ~ScopedDeadlineContext();

    ScopedDeadlineContext(const ScopedDeadlineContext&) = delete;
    ScopedDeadlineContext& operator=(const ScopedDeadlineContext&) = delete;
    ScopedDeadlineContext(ScopedDeadlineContext&&) = delete;
    ScopedDeadlineContext& operator=(ScopedDeadlineContext&&) = delete;

private:
    DeadlineContext previous_;
};

// hook enable 外观接口（线程本地）。
void set_runtime_hook_enabled(bool enabled) noexcept;
bool runtime_hook_enabled() noexcept;

// 作用域 hook 开关。
class ScopedRuntimeHookEnable {
public:
    explicit ScopedRuntimeHookEnable(bool enabled) noexcept;
    ~ScopedRuntimeHookEnable();

    ScopedRuntimeHookEnable(const ScopedRuntimeHookEnable&) = delete;
    ScopedRuntimeHookEnable& operator=(const ScopedRuntimeHookEnable&) = delete;
    ScopedRuntimeHookEnable(ScopedRuntimeHookEnable&&) = delete;
    ScopedRuntimeHookEnable& operator=(ScopedRuntimeHookEnable&&) = delete;

private:
    bool previous_{false};
};

// 兼容入口：等价于 start_runtime()。
void init_runtime();
// 兼容入口：等价于 stop_runtime()。
void shutdown_runtime();

}  // namespace rpc::runtime
