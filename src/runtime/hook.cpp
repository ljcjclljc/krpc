#include "rpc/runtime/hook.h"

// 文件用途：
// 实现 sleep/read/write/recv/send 的首批 Hook，
// 在协程上下文中把阻塞等待转化为 IOManager 事件 + 协程 yield。

#include <atomic>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/io_manager.h"
#include "rpc/runtime/request_context.h"

namespace rpc::runtime {

namespace {

thread_local bool g_hook_enabled = false;
thread_local std::uint64_t g_hook_connect_timeout_ms = 0;
std::atomic<IOManager*> g_hook_io_manager{nullptr};

}  // namespace

void set_hook_enabled(bool enabled) noexcept {
    g_hook_enabled = enabled;
}

bool hook_enabled() noexcept {
    return g_hook_enabled;
}

ScopedHookEnable::ScopedHookEnable(bool enabled) noexcept
    : previous_(hook_enabled()) {
    set_hook_enabled(enabled);
}

ScopedHookEnable::~ScopedHookEnable() {
    set_hook_enabled(previous_);
}

void set_hook_io_manager(IOManager* io_manager) noexcept {
    g_hook_io_manager.store(io_manager, std::memory_order_release);
}

IOManager* hook_io_manager() noexcept {
    return g_hook_io_manager.load(std::memory_order_acquire);
}

void set_hook_connect_timeout_ms(std::uint64_t timeout_ms) noexcept {
    g_hook_connect_timeout_ms = timeout_ms;
}

std::uint64_t hook_connect_timeout_ms() noexcept {
    return g_hook_connect_timeout_ms;
}

}  // namespace rpc::runtime

namespace {

using SleepFn = unsigned int (*)(unsigned int);
using ReadFn = ssize_t (*)(int, void*, std::size_t);
using WriteFn = ssize_t (*)(int, const void*, std::size_t);
using RecvFn = ssize_t (*)(int, void*, std::size_t, int);
using SendFn = ssize_t (*)(int, const void*, std::size_t, int);
using ConnectFn = int (*)(int, const struct sockaddr*, socklen_t);
using AcceptFn = int (*)(int, struct sockaddr*, socklen_t*);

template <typename Fn>
Fn load_symbol(const char* name) noexcept {
    void* symbol = ::dlsym(RTLD_NEXT, name);
    return reinterpret_cast<Fn>(symbol);
}

SleepFn real_sleep() noexcept {
    static SleepFn fn = load_symbol<SleepFn>("sleep");
    return fn;
}

ReadFn real_read() noexcept {
    static ReadFn fn = load_symbol<ReadFn>("read");
    return fn;
}

WriteFn real_write() noexcept {
    static WriteFn fn = load_symbol<WriteFn>("write");
    return fn;
}

RecvFn real_recv() noexcept {
    static RecvFn fn = load_symbol<RecvFn>("recv");
    return fn;
}

SendFn real_send() noexcept {
    static SendFn fn = load_symbol<SendFn>("send");
    return fn;
}

ConnectFn real_connect() noexcept {
    static ConnectFn fn = load_symbol<ConnectFn>("connect");
    return fn;
}

AcceptFn real_accept() noexcept {
    static AcceptFn fn = load_symbol<AcceptFn>("accept");
    return fn;
}

bool hook_path_enabled() noexcept {
    return rpc::runtime::hook_enabled()
        && rpc::runtime::Coroutine::current() != nullptr
        && rpc::runtime::hook_io_manager() != nullptr;
}

class ScopedNonBlocking {
public:
    explicit ScopedNonBlocking(int fd) : fd_(fd) {
        old_flags_ = ::fcntl(fd_, F_GETFL, 0);
        if (old_flags_ < 0) {
            return;
        }

        if ((old_flags_ & O_NONBLOCK) != 0) {
            ok_ = true;
            return;
        }

        if (::fcntl(fd_, F_SETFL, old_flags_ | O_NONBLOCK) == 0) {
            changed_ = true;
            ok_ = true;
        }
    }

    ~ScopedNonBlocking() {
        if (!changed_) {
            return;
        }

        const int saved_errno = errno;
        ::fcntl(fd_, F_SETFL, old_flags_);
        errno = saved_errno;
    }

    bool ok() const noexcept {
        return ok_;
    }

private:
    int fd_{-1};
    int old_flags_{0};
    bool changed_{false};
    bool ok_{false};
};

bool wait_fd_event_once(int fd, rpc::runtime::IOEvent event) {
    rpc::runtime::IOManager* io_manager = rpc::runtime::hook_io_manager();
    if (io_manager == nullptr) {
        return false;
    }

    rpc::runtime::Coroutine* current = rpc::runtime::Coroutine::current();
    if (current == nullptr) {
        return false;
    }
    const rpc::runtime::CoroutineId waiting_coroutine_id = current->id();

    if (!io_manager->add_event(fd, event, [io_manager, waiting_coroutine_id]() {
            // 事件回调在调度器的新协程中执行，负责恢复原等待协程。
            io_manager->resume_coroutine(waiting_coroutine_id);
        })) {
        return false;
    }

    rpc::runtime::Coroutine::yield_current_waiting();
    return true;
}

enum class WaitEventResult : std::uint8_t {
    Ready = 0,
    Timeout = 1,
    Failed = 2,
};

struct WaitEventState {
    std::atomic<std::uint8_t> result{0}; // 0: pending, 1: ready, 2: timeout
};

WaitEventResult wait_fd_event_with_timeout_once(
    int fd,
    rpc::runtime::IOEvent event,
    std::optional<std::chrono::milliseconds> timeout
) {
    rpc::runtime::IOManager* io_manager = rpc::runtime::hook_io_manager();
    if (io_manager == nullptr) {
        return WaitEventResult::Failed;
    }

    rpc::runtime::Coroutine* current = rpc::runtime::Coroutine::current();
    if (current == nullptr) {
        return WaitEventResult::Failed;
    }
    const rpc::runtime::CoroutineId waiting_coroutine_id = current->id();

    auto state = std::make_shared<WaitEventState>();
    if (!io_manager->add_event(fd, event, [io_manager, waiting_coroutine_id, state]() {
            std::uint8_t expected = 0;
            if (state->result.compare_exchange_strong(
                    expected,
                    1,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                io_manager->resume_coroutine(waiting_coroutine_id);
            }
        })) {
        return WaitEventResult::Failed;
    }

    rpc::runtime::TimerId timer_id = 0;
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

    rpc::runtime::Coroutine::yield_current_waiting();

    if (timer_id != 0) {
        io_manager->cancel_timer(timer_id);
    }

    const std::uint8_t result = state->result.load(std::memory_order_acquire);
    if (result == 1) {
        return WaitEventResult::Ready;
    }
    if (result == 2) {
        io_manager->del_event(fd, event);
        return WaitEventResult::Timeout;
    }
    return WaitEventResult::Failed;
}

std::optional<std::chrono::milliseconds> resolve_connect_wait_timeout() {
    std::optional<std::chrono::milliseconds> configured_timeout;
    const std::uint64_t hook_timeout_ms = rpc::runtime::hook_connect_timeout_ms();
    if (hook_timeout_ms > 0) {
        configured_timeout = std::chrono::milliseconds(hook_timeout_ms);
    }

    auto request_context = rpc::runtime::current_request_context();
    if (!request_context || request_context->deadline() == rpc::runtime::RequestContext::TimePoint::max()) {
        return configured_timeout;
    }

    const auto now = rpc::runtime::RequestContext::Clock::now();
    if (request_context->deadline() <= now) {
        return std::chrono::milliseconds(0);
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        request_context->deadline() - now
    );

    if (!configured_timeout.has_value()) {
        return remaining;
    }
    return std::min(*configured_timeout, remaining);
}

template <typename Syscall>
ssize_t run_io_hooked(int fd, rpc::runtime::IOEvent wait_event, Syscall&& syscall_once) {
    if (!hook_path_enabled()) {
        return syscall_once();
    }

    ScopedNonBlocking non_blocking(fd);
    if (!non_blocking.ok()) {
        return -1;
    }

    while (true) {
        const ssize_t result = syscall_once();
        if (result >= 0) {
            return result;
        }

        const int saved_errno = errno;
        if (saved_errno == EINTR) {
            continue;
        }

        if (saved_errno != EAGAIN && saved_errno != EWOULDBLOCK) {
            errno = saved_errno;
            return -1;
        }

        if (!wait_fd_event_once(fd, wait_event)) {
            errno = EBUSY;
            return -1;
        }
    }
}

}  // namespace

extern "C" unsigned int sleep(unsigned int seconds) {
    SleepFn fn = real_sleep();
    if (!hook_path_enabled()) {
        if (fn == nullptr) {
            errno = ENOSYS;
            return seconds;
        }
        return fn(seconds);
    }

    rpc::runtime::IOManager* io_manager = rpc::runtime::hook_io_manager();
    if (io_manager == nullptr) {
        if (fn == nullptr) {
            errno = ENOSYS;
            return seconds;
        }
        return fn(seconds);
    }

    std::atomic<bool> done{false};
    io_manager->add_timer(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::seconds(seconds)),
        [&done]() {
            done.store(true, std::memory_order_release);
        }
    );

    while (!done.load(std::memory_order_acquire)) {
        rpc::runtime::Coroutine::yield_current();
    }

    return 0;
}

extern "C" ssize_t read(int fd, void* buf, std::size_t count) {
    ReadFn fn = real_read();
    if (fn == nullptr) {
        errno = ENOSYS;
        return -1;
    }

    return run_io_hooked(fd, rpc::runtime::IOEvent::Read, [&]() {
        return fn(fd, buf, count);
    });
}

extern "C" ssize_t write(int fd, const void* buf, std::size_t count) {
    WriteFn fn = real_write();
    if (fn == nullptr) {
        errno = ENOSYS;
        return -1;
    }

    return run_io_hooked(fd, rpc::runtime::IOEvent::Write, [&]() {
        return fn(fd, buf, count);
    });
}

extern "C" ssize_t recv(int sockfd, void* buf, std::size_t len, int flags) {
    RecvFn fn = real_recv();
    if (fn == nullptr) {
        errno = ENOSYS;
        return -1;
    }

    return run_io_hooked(sockfd, rpc::runtime::IOEvent::Read, [&]() {
        return fn(sockfd, buf, len, flags);
    });
}

extern "C" ssize_t send(int sockfd, const void* buf, std::size_t len, int flags) {
    SendFn fn = real_send();
    WriteFn write_fn = real_write();
    if (fn == nullptr) {
        errno = ENOSYS;
        return -1;
    }

    return run_io_hooked(sockfd, rpc::runtime::IOEvent::Write, [&]() {
        const ssize_t result = fn(sockfd, buf, len, flags);
        if (result >= 0 || errno != EPERM || flags != 0 || write_fn == nullptr) {
            return result;
        }

        return write_fn(sockfd, buf, len);
    });
}

extern "C" int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen) {
    ConnectFn fn = real_connect();
    if (fn == nullptr) {
        errno = ENOSYS;
        return -1;
    }

    if (!hook_path_enabled()) {
        return fn(sockfd, addr, addrlen);
    }

    ScopedNonBlocking non_blocking(sockfd);
    if (!non_blocking.ok()) {
        return -1;
    }

    while (true) {
        const int result = fn(sockfd, addr, addrlen);
        if (result == 0) {
            return 0;
        }

        const int saved_errno = errno;
        if (saved_errno == EINTR) {
            continue;
        }

        if (saved_errno != EINPROGRESS
            && saved_errno != EALREADY
            && saved_errno != EAGAIN
            && saved_errno != EWOULDBLOCK) {
            errno = saved_errno;
            return -1;
        }

        const std::optional<std::chrono::milliseconds> timeout = resolve_connect_wait_timeout();
        if (timeout.has_value() && timeout->count() <= 0) {
            errno = ETIMEDOUT;
            return -1;
        }

        const WaitEventResult wait_result =
            wait_fd_event_with_timeout_once(sockfd, rpc::runtime::IOEvent::Write, timeout);
        if (wait_result == WaitEventResult::Timeout) {
            errno = ETIMEDOUT;
            return -1;
        }
        if (wait_result == WaitEventResult::Failed) {
            errno = EBUSY;
            return -1;
        }

        int connect_error = 0;
        socklen_t connect_error_len = static_cast<socklen_t>(sizeof(connect_error));
        if (::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &connect_error, &connect_error_len) != 0) {
            return -1;
        }
        if (connect_error == 0) {
            return 0;
        }
        if (connect_error == EINPROGRESS
            || connect_error == EALREADY
            || connect_error == EAGAIN
            || connect_error == EWOULDBLOCK) {
            continue;
        }

        errno = connect_error;
        return -1;
    }
}

extern "C" int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen) {
    AcceptFn fn = real_accept();
    if (fn == nullptr) {
        errno = ENOSYS;
        return -1;
    }

    const ssize_t result = run_io_hooked(sockfd, rpc::runtime::IOEvent::Read, [&]() {
        return static_cast<ssize_t>(fn(sockfd, addr, addrlen));
    });
    if (result < 0) {
        return -1;
    }
    return static_cast<int>(result);
}
