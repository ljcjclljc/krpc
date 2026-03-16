#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rpc/infra/infra.h"
#include "rpc/net/net.h"
#include "rpc/rpc/client.h"
#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/io_manager.h"
#include "rpc/runtime/request_context.h"
#include "rpc/runtime/runtime.h"
#include "rpc/runtime/scheduler.h"

// 文件用途：
// 集成验证 W1-W5：调度器多线程 + IO 事件循环 + Timer + deadline 上下文 + RPC 抽象调用。

#include <sys/socket.h>
#include <unistd.h>

namespace {

template <typename Predicate>
bool wait_until(Predicate predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return predicate();
}

long long since_ms(const std::chrono::steady_clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

const char* bool_text(bool value) {
    return value ? "true" : "false";
}

}  // namespace

int main() {
    using namespace std::chrono_literals;
    const auto test_start = std::chrono::steady_clock::now();
    std::mutex log_mutex;
    auto log_info = [&](const std::string& message) {
        std::lock_guard<std::mutex> lock(log_mutex);
        std::cout << "[W5][integration][+" << since_ms(test_start) << "ms] " << message << '\n';
    };
    auto log_error = [&](const std::string& message) {
        std::lock_guard<std::mutex> lock(log_mutex);
        std::cerr << "[W5][integration][+" << since_ms(test_start) << "ms][ERROR] " << message << '\n';
    };

    rpc::infra::init_infra();
    log_info("infra initialized");
    rpc::runtime::init_runtime();
    log_info("runtime initialized");
    rpc::net::init_network();
    log_info("network initialized");
    rpc::client::init_client();
    log_info("rpc client initialized");

    rpc::client::RpcRequest request;
    request.service = "gateway.backend";
    request.method = "Ping";
    request.payload = "hello";
    const rpc::client::RpcResponse response = rpc::client::default_client()->invoke(request);
    if (response.code != 0) {
        std::ostringstream oss;
        oss << "rpc invoke failed, code=" << response.code << ", message=" << response.message;
        log_error(oss.str());
        return 1;
    }
    log_info("rpc invoke passed, payload=" + response.payload);

    rpc::runtime::CoroutineScheduler scheduler;
    scheduler.start(2);
    log_info("scheduler started, workers=2");

    rpc::runtime::IOManager io_manager(scheduler);
    io_manager.start();
    log_info("io_manager started");

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        log_error("socketpair failed");
        return 1;
    }
    {
        std::ostringstream oss;
        oss << "socketpair created, read_fd=" << fds[0] << ", write_fd=" << fds[1];
        log_info(oss.str());
    }

    constexpr int kBackgroundTasks = 4;
    std::atomic<int> background_done{0};
    for (int i = 0; i < kBackgroundTasks; ++i) {
        scheduler.schedule([&background_done, &log_info, i]() {
            rpc::runtime::Coroutine::yield_current();
            rpc::runtime::Coroutine::yield_current();
            const int done = background_done.fetch_add(1, std::memory_order_relaxed) + 1;
            std::ostringstream oss;
            oss << "background task done, index=" << i << ", done=" << done;
            log_info(oss.str());
        });
    }
    log_info("background tasks scheduled");

    std::atomic<bool> read_registered{false};
    std::atomic<bool> read_done{false};
    std::atomic<bool> write_callback_done{false};
    std::atomic<bool> timer_cancelled_fired{false};
    std::atomic<int> timer_callback_done{0};
    std::atomic<bool> context_tls_ok{false};
    std::atomic<bool> deadline_cancelled{false};
    std::vector<int> timer_order;
    std::mutex timer_order_mutex;

    if (!io_manager.add_event(fds[0], rpc::runtime::IOEvent::Read, [&]() {
            log_info("read callback fired");
            rpc::runtime::Coroutine::yield_current();

            char buffer[16]{};
            const ssize_t n = ::read(fds[0], buffer, sizeof(buffer));
            if (n > 0) {
                read_done.store(true, std::memory_order_relaxed);
                std::ostringstream oss;
                oss << "read callback consumed bytes=" << n << ", payload=" << std::string(buffer, buffer + n);
                log_info(oss.str());
            } else {
                std::ostringstream oss;
                oss << "read callback read returned n=" << n;
                log_error(oss.str());
            }
        })) {
        log_error("add read event failed");
        return 1;
    }
    read_registered.store(true, std::memory_order_relaxed);
    log_info("read event registered");

    if (!io_manager.add_event(fds[1], rpc::runtime::IOEvent::Write, [&write_callback_done, &log_info]() {
            write_callback_done.store(true, std::memory_order_relaxed);
            log_info("write callback fired");
        })) {
        log_error("add write event failed");
        return 1;
    }
    log_info("write event registered");

    const rpc::runtime::TimerId timer_1 = io_manager.add_timer(15ms, [&]() {
        std::lock_guard<std::mutex> lock(timer_order_mutex);
        timer_order.push_back(1);
        timer_callback_done.fetch_add(1, std::memory_order_relaxed);
        log_info("timer callback #1 fired");
    });
    const rpc::runtime::TimerId timer_2 = io_manager.add_timer(45ms, [&]() {
        std::lock_guard<std::mutex> lock(timer_order_mutex);
        timer_order.push_back(2);
        timer_callback_done.fetch_add(1, std::memory_order_relaxed);
        log_info("timer callback #2 fired");
    });
    const rpc::runtime::TimerId cancelled_timer = io_manager.add_timer(30ms, [&]() {
        timer_cancelled_fired.store(true, std::memory_order_relaxed);
        log_error("cancelled timer fired unexpectedly");
    });
    {
        std::ostringstream oss;
        oss << "timers registered, timer_1=" << timer_1
            << ", timer_2=" << timer_2
            << ", cancelled_timer=" << cancelled_timer;
        log_info(oss.str());
    }
    if (!io_manager.cancel_timer(cancelled_timer)) {
        std::ostringstream oss;
        oss << "cancel timer failed, timer_id=" << cancelled_timer;
        log_error(oss.str());
        return 1;
    }
    log_info("cancelled_timer cancelled successfully");

    auto request_ctx = rpc::runtime::RequestContext::create_with_timeout("req-w5-deadline", 80ms);
    request_ctx->set_value("route", "gateway.backend");
    log_info("request context created, request_id=req-w5-deadline, timeout=80ms");
    if (!request_ctx->bind_deadline_timer(io_manager)) {
        log_error("bind deadline timer failed");
        return 1;
    }
    log_info("request context deadline timer bound");

    scheduler.schedule([request_ctx, &context_tls_ok, &deadline_cancelled, &log_info]() {
        rpc::runtime::ScopedRequestContext scoped_ctx(request_ctx);
        log_info("deadline coroutine started, context bound to TLS");
        auto ctx = rpc::runtime::current_request_context();
        if (ctx && ctx->request_id() == "req-w5-deadline"
            && ctx->value("route").value_or("") == "gateway.backend") {
            context_tls_ok.store(true, std::memory_order_relaxed);
            log_info("deadline coroutine confirmed TLS context");
        }

        while (ctx && !ctx->check_deadline_and_cancel()) {
            rpc::runtime::Coroutine::yield_current();
        }

        if (ctx && ctx->cancelled()) {
            deadline_cancelled.store(true, std::memory_order_relaxed);
            log_info("deadline coroutine observed cancellation");
        }
    });
    log_info("deadline coroutine scheduled");

    std::thread writer([fd = fds[1], &log_info]() {
        std::this_thread::sleep_for(20ms);
        const char msg[] = "ping";
        ::write(fd, msg, sizeof(msg));
        log_info("writer thread sent payload to socketpair");
    });
    log_info("writer thread started");

    log_info("wait_until started (timeout=3000ms)");
    const bool ok = wait_until([&]() {
        return read_registered.load(std::memory_order_relaxed)
            && read_done.load(std::memory_order_relaxed)
            && write_callback_done.load(std::memory_order_relaxed)
            && timer_callback_done.load(std::memory_order_relaxed) == 2
            && !timer_cancelled_fired.load(std::memory_order_relaxed)
            && context_tls_ok.load(std::memory_order_relaxed)
            && deadline_cancelled.load(std::memory_order_relaxed)
            && background_done.load(std::memory_order_relaxed) == kBackgroundTasks;
    }, 3000ms);
    log_info(std::string("wait_until completed, ok=") + bool_text(ok));

    if (!ok) {
        std::ostringstream oss;
        oss << "timeout waiting for components"
            << ", read_registered=" << read_registered.load(std::memory_order_relaxed)
            << ", read_done=" << read_done.load(std::memory_order_relaxed)
            << ", write_done=" << write_callback_done.load(std::memory_order_relaxed)
            << ", timer_done=" << timer_callback_done.load(std::memory_order_relaxed)
            << ", timer_cancelled_fired=" << timer_cancelled_fired.load(std::memory_order_relaxed)
            << ", context_tls_ok=" << context_tls_ok.load(std::memory_order_relaxed)
            << ", deadline_cancelled=" << deadline_cancelled.load(std::memory_order_relaxed)
            << ", background_done=" << background_done.load(std::memory_order_relaxed);
        log_error(oss.str());
        writer.join();
        log_info("writer thread joined (failure path)");
        request_ctx->clear_deadline_timer(io_manager);
        log_info("request context deadline timer cleared (failure path)");
        io_manager.stop();
        log_info("io_manager stopped (failure path)");
        scheduler.stop();
        log_info("scheduler stopped (failure path)");
        ::close(fds[0]);
        ::close(fds[1]);
        log_info("fds closed (failure path)");
        return 1;
    }

    writer.join();
    log_info("writer thread joined");
    request_ctx->clear_deadline_timer(io_manager);
    log_info("request context deadline timer cleared");
    ::close(fds[0]);
    ::close(fds[1]);
    log_info("fds closed");

    {
        std::lock_guard<std::mutex> lock(timer_order_mutex);
        if (timer_order.size() != 2 || timer_order[0] != 1 || timer_order[1] != 2) {
            std::ostringstream oss;
            oss << "unexpected timer callback order";
            for (const int order : timer_order) {
                oss << ' ' << order;
            }
            log_error(oss.str());
            io_manager.stop();
            log_info("io_manager stopped (timer-order failure path)");
            scheduler.stop();
            log_info("scheduler stopped (timer-order failure path)");
            return 1;
        }
        std::ostringstream oss;
        oss << "timer callback order verified: " << timer_order[0] << " -> " << timer_order[1];
        log_info(oss.str());
    }

    io_manager.stop();
    log_info("io_manager stopped");
    scheduler.stop();
    log_info("scheduler stopped");

    if (scheduler.pending_count() != 0 || scheduler.alive_count() != 0) {
        std::ostringstream oss;
        oss << "scheduler not drained, pending=" << scheduler.pending_count()
            << ", alive=" << scheduler.alive_count();
        log_error(oss.str());
        return 1;
    }
    log_info("scheduler drained check passed");

    std::cout << "io_epoll_integration_test passed"
              << ", background=" << background_done.load(std::memory_order_relaxed)
              << ", read_done=" << read_done.load(std::memory_order_relaxed)
              << ", write_done=" << write_callback_done.load(std::memory_order_relaxed)
              << ", timer_done=" << timer_callback_done.load(std::memory_order_relaxed)
              << ", deadline_cancelled=" << deadline_cancelled.load(std::memory_order_relaxed)
              << '\n';
    return 0;
}
