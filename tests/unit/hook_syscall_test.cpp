#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <future>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/hook.h"
#include "rpc/runtime/io_manager.h"
#include "rpc/runtime/scheduler.h"

// 文件用途：
// 单元验证 W6 Hook 首批能力：sleep/read/write/recv/send、TLS 开关与 errno 兼容。

namespace {

template <typename Rep, typename Period>
bool wait_idle_with_timeout(
    rpc::runtime::CoroutineScheduler& scheduler,
    std::chrono::duration<Rep, Period> timeout
) {
    auto waiter = std::async(std::launch::async, [&scheduler]() {
        scheduler.wait_idle();
    });

    if (waiter.wait_for(timeout) != std::future_status::ready) {
        return false;
    }

    waiter.get();
    return true;
}

template <typename Rep, typename Period>
bool wait_flag_with_timeout(
    const std::atomic<bool>& flag,
    std::chrono::duration<Rep, Period> timeout
) {
    const auto deadline = std::chrono::steady_clock::now()
        + std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout);

    while (std::chrono::steady_clock::now() < deadline) {
        if (flag.load(std::memory_order_acquire)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    return flag.load(std::memory_order_acquire);
}

std::string make_unix_socket_path(const char* prefix) {
    const auto stamp = static_cast<unsigned long long>(
        std::chrono::steady_clock::now().time_since_epoch().count()
    );
    std::string compact_prefix(prefix);
    if (compact_prefix.size() > 12) {
        compact_prefix.resize(12);
    }
    return std::string("/tmp/") + compact_prefix + "_" + std::to_string(::getpid()) + "_"
        + std::to_string(stamp & 0xFFFFFull);
}

}  // namespace

int main() {
    using namespace std::chrono_literals;

    std::mutex error_mutex;
    std::string first_error;
    auto fail_once = [&](const std::string& message) {
        std::lock_guard<std::mutex> lock(error_mutex);
        if (first_error.empty()) {
            first_error = message;
        }
    };

    rpc::runtime::CoroutineScheduler scheduler;
    scheduler.start(1);

    rpc::runtime::IOManager io_manager(scheduler);
    io_manager.start();

    rpc::runtime::set_hook_io_manager(&io_manager);
    rpc::runtime::set_hook_enabled(false);

    // 1) TLS Hook 开关隔离：子线程开启不应影响主线程。
    std::atomic<bool> child_initial{true};
    std::atomic<bool> child_after_enable{false};
    std::thread hook_thread([&]() {
        child_initial.store(rpc::runtime::hook_enabled(), std::memory_order_relaxed);
        rpc::runtime::set_hook_enabled(true);
        child_after_enable.store(rpc::runtime::hook_enabled(), std::memory_order_relaxed);
    });
    hook_thread.join();

    if (rpc::runtime::hook_enabled()) {
        fail_once("main thread hook switch should remain disabled");
    }
    if (child_initial.load(std::memory_order_relaxed)) {
        fail_once("new thread hook default should be disabled");
    }
    if (!child_after_enable.load(std::memory_order_relaxed)) {
        fail_once("child thread hook enable failed");
    }

    // 2) sleep Hook 协程化：同 worker 下另一个协程应持续推进。
    std::atomic<bool> sleep_done{false};
    std::atomic<bool> sleep_ok{true};
    std::atomic<int> sleep_spin{0};
    const auto sleep_start = std::chrono::steady_clock::now();

    scheduler.schedule([&]() {
        rpc::runtime::ScopedHookEnable scoped_hook(true);
        const unsigned int remain = ::sleep(1);
        if (remain != 0) {
            sleep_ok.store(false, std::memory_order_relaxed);
        }
        sleep_done.store(true, std::memory_order_release);
    });

    scheduler.schedule([&]() {
        rpc::runtime::ScopedHookEnable scoped_hook(true);
        while (!sleep_done.load(std::memory_order_acquire)) {
            sleep_spin.fetch_add(1, std::memory_order_relaxed);
            rpc::runtime::Coroutine::yield_current();
        }
    });

    if (!wait_idle_with_timeout(scheduler, 5s)) {
        fail_once("sleep hook stage wait_idle timeout");
    }

    const auto sleep_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - sleep_start
    ).count();

    if (!sleep_ok.load(std::memory_order_relaxed)) {
        fail_once("hooked sleep returned non-zero remain");
    }
    if (sleep_spin.load(std::memory_order_relaxed) <= 0) {
        fail_once("sleep hook did not release worker for peer coroutine");
    }
    if (sleep_elapsed_ms < 850 || sleep_elapsed_ms > 4000) {
        std::ostringstream oss;
        oss << "hooked sleep elapsed out of range, elapsed_ms=" << sleep_elapsed_ms;
        fail_once(oss.str());
    }

    // 3) read Hook 协程化 + write 基本可用。
    int rw_fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, rw_fds) != 0) {
        fail_once("socketpair for read/write stage failed");
    } else {
        std::atomic<bool> read_done{false};
        std::atomic<bool> read_ok{false};
        std::atomic<int> read_spin{0};

        scheduler.schedule([&]() {
            rpc::runtime::ScopedHookEnable scoped_hook(true);
            char buffer[16]{};
            const ssize_t n = ::read(rw_fds[0], buffer, sizeof(buffer));
            if (n == 4 && std::string(buffer, buffer + n) == "ping") {
                read_ok.store(true, std::memory_order_relaxed);
            }
            read_done.store(true, std::memory_order_release);
        });

        scheduler.schedule([&]() {
            rpc::runtime::ScopedHookEnable scoped_hook(true);
            while (!read_done.load(std::memory_order_acquire)) {
                read_spin.fetch_add(1, std::memory_order_relaxed);
                rpc::runtime::Coroutine::yield_current();
            }
        });

        std::thread writer([&]() {
            std::this_thread::sleep_for(80ms);
            const char payload[] = "ping";
            ::write(rw_fds[1], payload, sizeof(payload) - 1);
        });

        if (!wait_flag_with_timeout(read_done, 4s)) {
            fail_once("read hook stage timeout waiting completion");
        }
        writer.join();

        if (!read_ok.load(std::memory_order_relaxed)) {
            fail_once("hooked read failed to receive expected payload");
        }
        if (read_spin.load(std::memory_order_relaxed) <= 0) {
            fail_once("hooked read did not yield while waiting");
        }

        ::close(rw_fds[0]);
        ::close(rw_fds[1]);
    }

    // 4) send Hook 基本可用。
    int send_fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, send_fds) != 0) {
        fail_once("socketpair for send stage failed");
    } else {
        std::atomic<ssize_t> send_ret{0};
        scheduler.schedule([&]() {
            rpc::runtime::ScopedHookEnable scoped_hook(true);
            const char payload[] = "xy";
            send_ret.store(::send(send_fds[0], payload, 2, 0), std::memory_order_relaxed);
        });

        if (!wait_idle_with_timeout(scheduler, 3s)) {
            fail_once("send hook stage wait_idle timeout");
        } else {
            char buffer[8]{};
            errno = 0;
            const ssize_t recvd = ::recv(send_fds[1], buffer, sizeof(buffer), MSG_DONTWAIT);
            if (send_ret.load(std::memory_order_relaxed) != 2
                || recvd != 2
                || buffer[0] != 'x'
                || buffer[1] != 'y') {
                fail_once("hooked send stage failed");
            }
        }

        ::close(send_fds[0]);
        ::close(send_fds[1]);
    }

    // 5) recv Hook 协程化。
    int recv_fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, recv_fds) != 0) {
        fail_once("socketpair for recv stage failed");
    } else {
        std::atomic<bool> recv_done{false};
        std::atomic<bool> recv_ok{false};
        std::atomic<int> recv_spin{0};

        scheduler.schedule([&]() {
            rpc::runtime::ScopedHookEnable scoped_hook(true);
            char buffer[16]{};
            const ssize_t n = ::recv(recv_fds[0], buffer, sizeof(buffer), 0);
            if (n == 4 && std::string(buffer, buffer + n) == "pong") {
                recv_ok.store(true, std::memory_order_relaxed);
            }
            recv_done.store(true, std::memory_order_release);
        });

        scheduler.schedule([&]() {
            rpc::runtime::ScopedHookEnable scoped_hook(true);
            while (!recv_done.load(std::memory_order_acquire)) {
                recv_spin.fetch_add(1, std::memory_order_relaxed);
                rpc::runtime::Coroutine::yield_current();
            }
        });

        std::thread recv_writer([&]() {
            std::this_thread::sleep_for(80ms);
            const char payload[] = "pong";
            ::write(recv_fds[1], payload, sizeof(payload) - 1);
        });

        if (!wait_flag_with_timeout(recv_done, 4s)) {
            fail_once("recv hook stage timeout waiting completion");
        }
        recv_writer.join();

        if (!recv_ok.load(std::memory_order_relaxed)) {
            fail_once("hooked recv failed to receive expected payload");
        }
        if (recv_spin.load(std::memory_order_relaxed) <= 0) {
            fail_once("hooked recv did not yield while waiting");
        }

        ::close(recv_fds[0]);
        ::close(recv_fds[1]);
    }

    // 6) accept Hook 协程化（UNIX 域套接字）。
    const std::string accept_socket_path = make_unix_socket_path("rpc_hook_accept");
    int accept_listen_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (accept_listen_fd < 0) {
        fail_once("create listen fd for accept stage failed");
    } else {
        ::unlink(accept_socket_path.c_str());
        sockaddr_un listen_addr{};
        listen_addr.sun_family = AF_UNIX;
        std::snprintf(listen_addr.sun_path, sizeof(listen_addr.sun_path), "%s", accept_socket_path.c_str());
        const socklen_t listen_len = static_cast<socklen_t>(sizeof(sockaddr_un));

        if (::bind(accept_listen_fd, reinterpret_cast<sockaddr*>(&listen_addr), listen_len) != 0
            || ::listen(accept_listen_fd, 8) != 0) {
            std::ostringstream oss;
            oss << "bind/listen for accept stage failed, errno=" << errno
                << ", path=" << accept_socket_path;
            fail_once(oss.str());
            ::close(accept_listen_fd);
            ::unlink(accept_socket_path.c_str());
        } else {
            std::atomic<bool> accept_done{false};
            std::atomic<bool> accept_ok{false};
            std::atomic<int> accepted_fd{-1};

            scheduler.schedule([&]() {
                rpc::runtime::ScopedHookEnable scoped_hook(true);
                sockaddr_un peer_addr{};
                socklen_t peer_len = static_cast<socklen_t>(sizeof(peer_addr));
                const int fd = ::accept(
                    accept_listen_fd,
                    reinterpret_cast<sockaddr*>(&peer_addr),
                    &peer_len
                );
                if (fd >= 0) {
                    accept_ok.store(true, std::memory_order_relaxed);
                    accepted_fd.store(fd, std::memory_order_relaxed);
                }
                accept_done.store(true, std::memory_order_release);
            });

            std::thread accept_peer([&]() {
                std::this_thread::sleep_for(80ms);
                const int client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
                if (client_fd < 0) {
                    fail_once("accept stage peer socket failed");
                    return;
                }

                if (::connect(client_fd, reinterpret_cast<const sockaddr*>(&listen_addr), listen_len) != 0) {
                    fail_once("accept stage peer connect failed");
                    ::close(client_fd);
                    return;
                }

                const char payload[] = "ok";
                ::write(client_fd, payload, sizeof(payload) - 1);
                ::close(client_fd);
            });

            if (!wait_flag_with_timeout(accept_done, 4s)) {
                fail_once("accept hook stage timeout waiting completion");
            }
            accept_peer.join();

            if (!accept_ok.load(std::memory_order_relaxed)) {
                fail_once("hooked accept did not return valid fd");
            } else {
                const int fd = accepted_fd.load(std::memory_order_relaxed);
                char buffer[8]{};
                const ssize_t n = ::read(fd, buffer, sizeof(buffer));
                if (n != 2 || buffer[0] != 'o' || buffer[1] != 'k') {
                    fail_once("hooked accept stage payload mismatch");
                }
                ::close(fd);
            }

            ::close(accept_listen_fd);
            ::unlink(accept_socket_path.c_str());
        }
    }

    // 7) connect Hook 基本可用（协程内调用 connect，UNIX 域套接字）。
    const std::string connect_socket_path = make_unix_socket_path("rpc_hook_connect");
    int connect_listen_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (connect_listen_fd < 0) {
        fail_once("create listen fd for connect stage failed");
    } else {
        ::unlink(connect_socket_path.c_str());
        sockaddr_un listen_addr{};
        listen_addr.sun_family = AF_UNIX;
        std::snprintf(listen_addr.sun_path, sizeof(listen_addr.sun_path), "%s", connect_socket_path.c_str());
        const socklen_t listen_len = static_cast<socklen_t>(sizeof(sockaddr_un));

        if (::bind(connect_listen_fd, reinterpret_cast<sockaddr*>(&listen_addr), listen_len) != 0
            || ::listen(connect_listen_fd, 8) != 0) {
            std::ostringstream oss;
            oss << "bind/listen for connect stage failed, errno=" << errno
                << ", path=" << connect_socket_path;
            fail_once(oss.str());
            ::close(connect_listen_fd);
            ::unlink(connect_socket_path.c_str());
        } else {
            std::atomic<bool> connect_done{false};
            std::atomic<bool> connect_ok{false};
            std::atomic<bool> server_read_ok{false};

            std::thread connect_peer([&]() {
                sockaddr_un peer_addr{};
                socklen_t peer_len = static_cast<socklen_t>(sizeof(peer_addr));
                const int accepted = ::accept(
                    connect_listen_fd,
                    reinterpret_cast<sockaddr*>(&peer_addr),
                    &peer_len
                );
                if (accepted < 0) {
                    fail_once("connect stage server accept failed");
                    return;
                }

                char buffer[8]{};
                const ssize_t n = ::read(accepted, buffer, sizeof(buffer));
                if (n == 2 && buffer[0] == 'c' && buffer[1] == 'n') {
                    server_read_ok.store(true, std::memory_order_relaxed);
                }
                ::close(accepted);
            });

            scheduler.schedule([&]() {
                rpc::runtime::ScopedHookEnable scoped_hook(true);
                rpc::runtime::set_hook_connect_timeout_ms(500);
                const int client_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
                if (client_fd < 0) {
                    connect_done.store(true, std::memory_order_release);
                    return;
                }

                if (::connect(client_fd, reinterpret_cast<const sockaddr*>(&listen_addr), listen_len) == 0) {
                    const char payload[] = "cn";
                    if (::write(client_fd, payload, sizeof(payload) - 1) == 2) {
                        connect_ok.store(true, std::memory_order_relaxed);
                    }
                }

                ::close(client_fd);
                rpc::runtime::set_hook_connect_timeout_ms(0);
                connect_done.store(true, std::memory_order_release);
            });

            if (!wait_flag_with_timeout(connect_done, 4s)) {
                fail_once("connect hook stage timeout waiting completion");
            }
            connect_peer.join();

            if (!connect_ok.load(std::memory_order_relaxed)) {
                fail_once("hooked connect stage failed");
            }
            if (!server_read_ok.load(std::memory_order_relaxed)) {
                fail_once("hooked connect stage server read mismatch");
            }

            ::close(connect_listen_fd);
            ::unlink(connect_socket_path.c_str());
        }
    }

    // 8) connect 超时控制：网络黑洞地址应在超时窗口内退出（或环境路由直接报错）。
    std::atomic<bool> connect_timeout_done{false};
    std::atomic<int> connect_timeout_ret{0};
    std::atomic<int> connect_timeout_errno{0};
    std::atomic<long long> connect_timeout_elapsed_ms{0};
    std::atomic<bool> connect_timeout_skipped{false};
    scheduler.schedule([&]() {
        rpc::runtime::ScopedHookEnable scoped_hook(true);
        rpc::runtime::set_hook_connect_timeout_ms(120);
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            const int err = errno;
            if (err == EAFNOSUPPORT || err == EPROTONOSUPPORT || err == ENOSYS) {
                connect_timeout_skipped.store(true, std::memory_order_relaxed);
            } else {
                connect_timeout_ret.store(-1, std::memory_order_relaxed);
                connect_timeout_errno.store(err, std::memory_order_relaxed);
            }
            connect_timeout_done.store(true, std::memory_order_release);
            return;
        }

        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(65000);
        ::inet_pton(AF_INET, "203.0.113.1", &target.sin_addr);

        const auto start = std::chrono::steady_clock::now();
        errno = 0;
        const int rc = ::connect(fd, reinterpret_cast<const sockaddr*>(&target), sizeof(target));
        const int err = errno;
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start
        ).count();

        connect_timeout_ret.store(rc, std::memory_order_relaxed);
        connect_timeout_errno.store(err, std::memory_order_relaxed);
        connect_timeout_elapsed_ms.store(elapsed, std::memory_order_relaxed);

        ::close(fd);
        rpc::runtime::set_hook_connect_timeout_ms(0);
        connect_timeout_done.store(true, std::memory_order_release);
    });

    if (!wait_flag_with_timeout(connect_timeout_done, 6s)) {
        fail_once("connect timeout stage wait timeout");
    } else if (connect_timeout_skipped.load(std::memory_order_relaxed)) {
        // 当前环境不支持 AF_INET connect 测试，跳过该子场景。
    } else {
        const int rc = connect_timeout_ret.load(std::memory_order_relaxed);
        const int err = connect_timeout_errno.load(std::memory_order_relaxed);
        const long long elapsed_ms = connect_timeout_elapsed_ms.load(std::memory_order_relaxed);
        if (rc == -1 && err == ETIMEDOUT) {
            if (elapsed_ms < 60 || elapsed_ms > 2000) {
                std::ostringstream oss;
                oss << "connect timeout elapsed out of range, elapsed_ms=" << elapsed_ms;
                fail_once(oss.str());
            }
        } else if (rc == -1
            && (err == ENETUNREACH || err == EHOSTUNREACH || err == ECONNREFUSED
                || err == EADDRNOTAVAIL || err == ENETDOWN)) {
            // 部分环境会在路由阶段快速失败，不强制要求走 ETIMEDOUT。
        } else {
            std::ostringstream oss;
            oss << "unexpected connect timeout stage result, ret=" << rc << ", errno=" << err;
            fail_once(oss.str());
        }
    }

    // 9) errno 兼容：非法 fd 在 Hook 开启时仍返回 EBADF。
    std::atomic<ssize_t> invalid_ret{0};
    std::atomic<int> invalid_errno{0};
    scheduler.schedule([&]() {
        rpc::runtime::ScopedHookEnable scoped_hook(true);
        errno = 0;
        char c = 0;
        invalid_ret.store(::read(-1, &c, 1), std::memory_order_relaxed);
        invalid_errno.store(errno, std::memory_order_relaxed);
    });

    if (!wait_idle_with_timeout(scheduler, 3s)) {
        fail_once("errno stage wait_idle timeout");
    }

    if (invalid_ret.load(std::memory_order_relaxed) != -1
        || invalid_errno.load(std::memory_order_relaxed) != EBADF) {
        std::ostringstream oss;
        oss << "errno compatibility failed, ret="
            << invalid_ret.load(std::memory_order_relaxed)
            << ", errno=" << invalid_errno.load(std::memory_order_relaxed);
        fail_once(oss.str());
    }

    rpc::runtime::set_hook_io_manager(nullptr);
    io_manager.stop();
    scheduler.stop();

    if (!first_error.empty()) {
        std::cerr << "hook_syscall_test failed: " << first_error << '\n';
        return 1;
    }

    std::cout << "hook_syscall_test passed"
              << ", sleep_elapsed_ms=" << sleep_elapsed_ms
              << ", sleep_spin=" << sleep_spin.load(std::memory_order_relaxed)
              << '\n';
    return 0;
}
