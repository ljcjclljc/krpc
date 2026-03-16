#include "rpc/runtime/io_manager.h"

// 文件用途：
// 实现 Epoll 事件循环、FD 事件注册/删除，以及 IO 事件触发后的回调调度。

#include <cerrno>
#include <cstdint>
#include <stdexcept>

#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <unistd.h>

namespace rpc::runtime {

namespace {

// 判断是否仅包含单一事件（Read 或 Write），用于接口参数校验。
bool is_single_event(IOEvent event) {
    return event == IOEvent::Read || event == IOEvent::Write;
}

void* wakeup_event_tag() {
    // 用固定非空 tag 区分 eventfd 唤醒事件与普通 fd 上下文。
    return reinterpret_cast<void*>(static_cast<std::uintptr_t>(1));
}

// 将 IOEvent 转换为 Epoll 事件掩码。
std::uint32_t to_epoll_events(IOEvent events) {
    std::uint32_t result = 0;
    if (has_event(events, IOEvent::Read)) {
        result |= EPOLLIN;
    }
    if (has_event(events, IOEvent::Write)) {
        result |= EPOLLOUT;
    }
    // 错误/挂起事件不需要显式注册，但这里附加以便显式感知。
    result |= EPOLLERR | EPOLLHUP;
    return result;
}

}  // namespace

IOManager::IOManager(CoroutineScheduler& scheduler, std::size_t max_events)
    : max_events_(max_events == 0 ? 1 : max_events),
      scheduler_(scheduler) {
    // 创建 Epoll 实例，使用 CLOEXEC 标志确保子进程不会继承该 fd。
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        throw std::runtime_error("epoll_create1 failed");
    }

    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        throw std::runtime_error("eventfd failed");
    }

    epoll_event wakeup_event{};
    wakeup_event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    wakeup_event.data.ptr = wakeup_event_tag();
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &wakeup_event) != 0) {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        throw std::runtime_error("epoll_ctl add wakeup fd failed");
    }
}

IOManager::~IOManager() {
    stop();
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

void IOManager::start() {
    if (running_.exchange(true)) {
        return;
    }
    worker_ = std::thread(&IOManager::event_loop, this);
}

void IOManager::stop() {
    if (!running_.exchange(false)) {
        timer_manager_.clear();
        return;
    }

    wakeup_event_loop();
    if (worker_.joinable()) {
        worker_.join();
    }

    timer_manager_.clear();
}

bool IOManager::add_event(int fd, IOEvent event, CoroutineCallback callback) {
    return add_event_internal(fd, event, std::move(callback));
}

bool IOManager::del_event(int fd, IOEvent event) {
    if (fd < 0 || !is_single_event(event)) {
        return false;
    }
    // 获取 FdContext 并删除事件，如果删除后没有剩余事件则从 Epoll 中移除该 fd。
    FdContext* ctx = get_fd_context(fd);
    if (ctx == nullptr) {
        return false;
    }

    std::lock_guard<std::mutex> lock(ctx->mutex());
    IOEvent old_events = ctx->events();
    EventContext snapshot = ctx->snapshot_event(event);

    if (!ctx->unbind_event(event)) {
        return false;
    }

    IOEvent new_events = ctx->events();
    int op = (new_events == IOEvent::None) ? EPOLL_CTL_DEL : EPOLL_CTL_MOD;
    epoll_event ev{};
    ev.events = to_epoll_events(new_events);
    ev.data.ptr = ctx;

    int result = 0;
    if (op == EPOLL_CTL_DEL) {
        result = ::epoll_ctl(epoll_fd_, op, fd, nullptr);
    } else {
        result = ::epoll_ctl(epoll_fd_, op, fd, &ev);
    }

    if (result != 0) {
        ctx->restore_event(event, snapshot, old_events);
        return false;
    }

    return true;
}

TimerId IOManager::add_timer(std::chrono::milliseconds delay_ms, CoroutineCallback callback) {
    bool earliest_changed = false;
    const TimerId timer_id = timer_manager_.add_timer(delay_ms, std::move(callback), &earliest_changed);
    if (earliest_changed) {
        wakeup_event_loop();
    }
    return timer_id;
}

bool IOManager::cancel_timer(TimerId timer_id) {
    const bool cancelled = timer_manager_.cancel_timer(timer_id);
    if (cancelled) {
        wakeup_event_loop();
    }
    return cancelled;
}

bool IOManager::resume_coroutine(CoroutineId coroutine_id) {
    return scheduler_.resume(coroutine_id);
}

bool IOManager::add_event_internal(
    int fd,
    IOEvent event,
    CoroutineCallback callback
) {
    if (fd < 0 || !is_single_event(event)) {
        return false;
    }

    if (!callback) {
        return false;
    }
    // 事件绑定必须提供回调函数，触发后由调度器拉起新协程执行。
    FdContext* ctx = get_fd_context(fd);
    if (ctx == nullptr) {
        return false;
    }

    std::lock_guard<std::mutex> lock(ctx->mutex());
    IOEvent old_events = ctx->events();
    EventContext snapshot = ctx->snapshot_event(event);

    if (!ctx->bind_event(event, std::move(callback))) {
        return false;
    }

    IOEvent new_events = ctx->events();
    int op = (old_events == IOEvent::None) ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    epoll_event ev{};
    ev.events = to_epoll_events(new_events);
    ev.data.ptr = ctx;

    if (::epoll_ctl(epoll_fd_, op, fd, &ev) != 0) {
        ctx->restore_event(event, snapshot, old_events);
        return false;
    }

    return true;
}
// 获取或创建 FdContext，确保 fd_contexts_ 容量足够，并返回对应的 FdContext 指针。
IOManager::FdContext* IOManager::get_fd_context(int fd) {
    if (fd < 0) {
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(fd_mutex_);
    const std::size_t index = static_cast<std::size_t>(fd);
    if (index >= fd_contexts_.size()) {
        fd_contexts_.resize(index + 1);
    }

    if (!fd_contexts_[index]) {
        fd_contexts_[index] = std::make_unique<FdContext>(fd);
    }

    return fd_contexts_[index].get();
}

void IOManager::wakeup_event_loop() {
    if (wakeup_fd_ < 0) {
        return;
    }

    const std::uint64_t one = 1;
    const ssize_t written = ::write(wakeup_fd_, &one, sizeof(one));
    if (written < 0 && errno != EAGAIN) {
        // 忽略唤醒失败；下一次 IO/超时也会推进事件循环。
    }
}

void IOManager::drain_wakeup_fd() {
    if (wakeup_fd_ < 0) {
        return;
    }

    std::uint64_t value = 0;
    while (::read(wakeup_fd_, &value, sizeof(value)) > 0) {
    }
}

void IOManager::event_loop() {
    std::vector<epoll_event> events(max_events_);

    while (running_.load(std::memory_order_acquire)) {
        const int timeout_ms = timer_manager_.next_timeout_ms();
        const int ready = ::epoll_wait(
            epoll_fd_,
            events.data(),
            static_cast<int>(events.size()),
            timeout_ms
        );

        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        if (!running_.load(std::memory_order_acquire)) {
            break;
        }

        if (ready > 0) {
            for (int i = 0; i < ready; ++i) {
                epoll_event& ev = events[static_cast<std::size_t>(i)];
                if (ev.data.ptr == wakeup_event_tag()) {
                    drain_wakeup_fd();
                    continue;
                }

                auto* ctx = static_cast<FdContext*>(ev.data.ptr);
                if (ctx == nullptr) {
                    continue;
                }

                std::uint32_t revents = ev.events;
                if (revents & (EPOLLERR | EPOLLHUP)) {
                    revents |= EPOLLIN | EPOLLOUT;
                }

                IOEvent triggered = IOEvent::None;
                if (revents & EPOLLIN) {
                    triggered |= IOEvent::Read;
                }
                if (revents & EPOLLOUT) {
                    triggered |= IOEvent::Write;
                }

                if (triggered != IOEvent::None) {
                    ctx->trigger_events(triggered, scheduler_, epoll_fd_);
                }
            }
        }

        std::vector<TimerManager::TimerCallback> expired_callbacks = timer_manager_.collect_expired_callbacks();
        for (auto& callback : expired_callbacks) {
            if (!callback) {
                continue;
            }

            try {
                scheduler_.schedule(std::move(callback));
            } catch (...) {
                // 调度器可能已停止，忽略异常避免事件线程异常退出。
            }
        }
    }
}

IOManager::FdContext::FdContext(int fd) : fd_(fd) {}

int IOManager::FdContext::fd() const noexcept {
    return fd_;
}

IOEvent IOManager::FdContext::events() const noexcept {
    return registered_events_;
}

std::mutex& IOManager::FdContext::mutex() {
    return mutex_;
}
// 事件上下文快照接口，用于在修改事件绑定前保存当前状态，以便在修改失败时恢复。
IOManager::EventContext IOManager::FdContext::snapshot_event(IOEvent event) const {
    return event_context(event);
}

void IOManager::FdContext::restore_event(IOEvent event, const EventContext& snapshot, IOEvent events_snapshot) {
    EventContext& ctx = event_context(event);
    ctx = snapshot;
    registered_events_ = events_snapshot;
}
// 事件绑定接口，绑定事件回调，要求事件必须未被绑定。
bool IOManager::FdContext::bind_event(IOEvent event, CoroutineCallback callback) {
    EventContext& ctx = event_context(event);
    if (ctx.active) {
        return false;
    }

    ctx.active = true;
    ctx.callback = std::move(callback);
    registered_events_ |= event;
    return true;
}

bool IOManager::FdContext::unbind_event(IOEvent event) {
    EventContext& ctx = event_context(event);
    if (!ctx.active) {
        return false;
    }

    ctx.active = false;
    ctx.callback = nullptr;
    registered_events_ &= ~event;
    return true;
}
// 事件触发接口，触发事件回调，并更新 Epoll 监听状态。
void IOManager::FdContext::trigger_events(IOEvent events, CoroutineScheduler& scheduler, int epoll_fd) {
    if (events == IOEvent::None) {
        return;
    }

    EventContext read_snapshot;
    EventContext write_snapshot;
    bool has_read = false;
    bool has_write = false;
    IOEvent old_events = IOEvent::None;
    IOEvent new_events = IOEvent::None;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        old_events = registered_events_;
        IOEvent fired = events & registered_events_;

        if (has_event(fired, IOEvent::Read)) {
            EventContext& ctx = read_;
            if (ctx.active) {
                read_snapshot = ctx;
                has_read = true;
            }
            ctx.active = false;
            ctx.callback = nullptr;
            registered_events_ &= ~IOEvent::Read;
        }

        if (has_event(fired, IOEvent::Write)) {
            EventContext& ctx = write_;
            if (ctx.active) {
                write_snapshot = ctx;
                has_write = true;
            }
            ctx.active = false;
            ctx.callback = nullptr;
            registered_events_ &= ~IOEvent::Write;
        }

        new_events = registered_events_;

        if (old_events != new_events) {
            int op = (new_events == IOEvent::None) ? EPOLL_CTL_DEL : EPOLL_CTL_MOD;
            if (op == EPOLL_CTL_DEL) {
                ::epoll_ctl(epoll_fd, op, fd_, nullptr);
            } else {
                epoll_event ev{};
                ev.events = to_epoll_events(new_events);
                ev.data.ptr = this;
                ::epoll_ctl(epoll_fd, op, fd_, &ev);
            }
        }
    }

    if (has_read) {
        if (read_snapshot.callback) {
            try {
                scheduler.schedule(std::move(read_snapshot.callback));
            } catch (...) {
                // 调度器可能已停止，忽略异常以避免事件线程退出。
            }
        }
    }

    if (has_write) {
        if (write_snapshot.callback) {
            try {
                scheduler.schedule(std::move(write_snapshot.callback));
            } catch (...) {
                // 调度器可能已停止，忽略异常以避免事件线程退出。
            }
        }
    }
}

IOManager::EventContext& IOManager::FdContext::event_context(IOEvent event) {
    return event == IOEvent::Read ? read_ : write_;
}

const IOManager::EventContext& IOManager::FdContext::event_context(IOEvent event) const {
    return event == IOEvent::Read ? read_ : write_;
}

}  // namespace rpc::runtime
