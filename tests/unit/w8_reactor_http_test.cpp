#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/system/error_code.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rpc/net/reactor_http_server.h"

namespace {

namespace http = boost::beast::http;

using namespace std::chrono_literals;

bool wait_until(std::function<bool()> predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(5ms);
    }
    return predicate();
}

bool set_recv_timeout(int fd, std::chrono::milliseconds timeout) {
    timeval tv{};
    tv.tv_sec = static_cast<long>(timeout.count() / 1000);
    tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);
    return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, static_cast<socklen_t>(sizeof(tv))) == 0;
}

int connect_to_server(std::uint16_t port) {
    for (int attempt = 0; attempt < 60; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) == 0) {
            return fd;
        }

        ::close(fd);
        std::this_thread::sleep_for(10ms);
    }
    return -1;
}

bool send_all(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + static_cast<std::ptrdiff_t>(sent), data.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

std::string build_request(
    const std::string& method,
    const std::string& target,
    const std::string& body,
    const std::string& connection,
    std::size_t override_content_length = 0,
    bool use_override_content_length = false
) {
    http::request<http::string_body> request;
    request.version(11);
    request.target(target);
    request.set(http::field::host, "127.0.0.1");
    request.set(http::field::connection, connection);
    request.body() = body;

    const http::verb verb = http::string_to_verb(method);
    if (verb != http::verb::unknown) {
        request.method(verb);
    } else {
        request.method_string(method);
    }

    if (use_override_content_length) {
        request.set(http::field::content_length, std::to_string(override_content_length));
    } else {
        request.prepare_payload();
    }

    std::ostringstream stream;
    stream << request;
    return stream.str();
}

std::string lower_ascii(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

struct ParsedResponse {
    int status{0};
    std::string body;
    std::map<std::string, std::string> headers;
};

class HttpResponseReader {
public:
    explicit HttpResponseReader(int fd) : fd_(fd) {}

    bool read_one(ParsedResponse* response) {
        if (response == nullptr) {
            return false;
        }

        http::response_parser<http::string_body> parser;
        parser.body_limit(4 * 1024 * 1024);

        while (true) {
            boost::system::error_code ec;
            const std::size_t consumed = parser.put(boost::asio::buffer(buffer_.data(), buffer_.size()), ec);
            if (consumed > 0) {
                buffer_.erase(0, consumed);
            }

            if (ec == http::error::need_more) {
                if (!recv_more()) {
                    return false;
                }
                continue;
            }
            if (ec) {
                return false;
            }
            if (!parser.is_done()) {
                if (!recv_more()) {
                    return false;
                }
                continue;
            }

            http::response<http::string_body> parsed = parser.release();
            response->status = parsed.result_int();
            response->body = std::move(parsed.body());
            response->headers.clear();
            for (const auto& field : parsed.base()) {
                response->headers[lower_ascii(std::string(field.name_string()))] = std::string(field.value());
            }
            return true;
        }
    }

private:
    bool recv_more() {
        char temp[4096];
        const ssize_t n = ::recv(fd_, temp, sizeof(temp), 0);
        if (n > 0) {
            buffer_.append(temp, static_cast<std::size_t>(n));
            return true;
        }
        if (n < 0 && errno == EINTR) {
            return recv_more();
        }
        return false;
    }

    int fd_{-1};
    std::string buffer_;
};

}  // namespace

int main() {
    // In restricted sandboxes, socket syscalls may be blocked (EPERM/EACCES).
    // Skip this integration-style test in that environment.
    int probe_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (probe_fd < 0 && (errno == EPERM || errno == EACCES)) {
        std::cout << "w8_reactor_http_test skipped: socket operations are not permitted\n";
        return 0;
    }
    if (probe_fd >= 0) {
        ::close(probe_fd);
    }

    rpc::net::ReactorHttpServerOptions options;
    options.bind_address = "127.0.0.1";
    options.port = 0;
    options.backlog = 256;
    options.max_connections = 256;
    options.ingress_queue_capacity = 4;
    options.worker_threads = 1;
    options.max_header_bytes = 256;
    options.max_body_bytes = 64;
    options.max_connection_requests_inflight = 64;
    options.max_connection_write_buffer_bytes = 16 * 1024;

    rpc::net::ReactorHttpServer server(options);
    if (!server.start()) {
        std::cerr << "failed to start server\n";
        return 1;
    }

    const std::uint16_t port = server.listen_port();
    if (port == 0) {
        std::cerr << "invalid listen port\n";
        return 1;
    }

    // 1) long connection keep-alive.
    {
        const int fd = connect_to_server(port);
        if (fd < 0) {
            std::cerr << "failed to connect keep-alive client\n";
            return 1;
        }
        if (!set_recv_timeout(fd, 4s)) {
            std::cerr << "set recv timeout failed\n";
            ::close(fd);
            return 1;
        }

        HttpResponseReader reader(fd);

        if (!send_all(fd, build_request("POST", "/keep-1", "a", "keep-alive"))) {
            std::cerr << "send keep request #1 failed\n";
            ::close(fd);
            return 1;
        }

        ParsedResponse response;
        if (!reader.read_one(&response) || response.status != 200) {
            std::cerr << "unexpected keep response #1, status=" << response.status << '\n';
            ::close(fd);
            return 1;
        }

        if (!send_all(fd, build_request("POST", "/keep-2", "bb", "close"))) {
            std::cerr << "send keep request #2 failed\n";
            ::close(fd);
            return 1;
        }

        if (!reader.read_one(&response) || response.status != 200) {
            std::cerr << "unexpected keep response #2, status=" << response.status << '\n';
            ::close(fd);
            return 1;
        }

        ::close(fd);
    }

    // 2) concurrent short connections.
    {
        constexpr int kShortClients = 24;
        std::atomic<int> failed{0};
        std::vector<std::thread> clients;
        clients.reserve(kShortClients);

        for (int i = 0; i < kShortClients; ++i) {
            clients.emplace_back([port, &failed, i]() {
                const int fd = connect_to_server(port);
                if (fd < 0) {
                    failed.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
                (void)set_recv_timeout(fd, 4s);

                HttpResponseReader reader(fd);
                const std::string body = "short-" + std::to_string(i);
                if (!send_all(fd, build_request("POST", "/short", body, "close"))) {
                    failed.fetch_add(1, std::memory_order_relaxed);
                    ::close(fd);
                    return;
                }

                ParsedResponse response;
                if (!reader.read_one(&response) || response.status != 200) {
                    failed.fetch_add(1, std::memory_order_relaxed);
                }

                ::close(fd);
            });
        }

        for (std::thread& client : clients) {
            client.join();
        }

        if (failed.load(std::memory_order_relaxed) != 0) {
            std::cerr << "short connection concurrency failed, failed="
                      << failed.load(std::memory_order_relaxed) << '\n';
            return 1;
        }
    }

    // 3) header limit -> 431.
    {
        const int fd = connect_to_server(port);
        if (fd < 0) {
            std::cerr << "failed to connect for header limit case\n";
            return 1;
        }
        (void)set_recv_timeout(fd, 4s);

        http::request<http::empty_body> request_message;
        request_message.version(11);
        request_message.method(http::verb::get);
        request_message.target("/header-limit");
        request_message.set(http::field::host, "127.0.0.1");
        request_message.set(http::field::connection, "close");
        request_message.set("X-Big", std::string(options.max_header_bytes + 32, 'x'));

        std::ostringstream request_stream;
        request_stream << request_message;
        const std::string request = request_stream.str();

        if (!send_all(fd, request)) {
            std::cerr << "send header limit request failed\n";
            ::close(fd);
            return 1;
        }

        HttpResponseReader reader(fd);
        ParsedResponse response;
        if (!reader.read_one(&response) || response.status != 431) {
            std::cerr << "expected 431, got " << response.status << '\n';
            ::close(fd);
            return 1;
        }

        ::close(fd);
    }

    // 4) body limit -> 413.
    {
        const int fd = connect_to_server(port);
        if (fd < 0) {
            std::cerr << "failed to connect for body limit case\n";
            return 1;
        }
        (void)set_recv_timeout(fd, 4s);

        const std::string request = build_request(
            "POST",
            "/body-limit",
            "",
            "close",
            options.max_body_bytes + 1,
            true
        );

        if (!send_all(fd, request)) {
            std::cerr << "send body limit request failed\n";
            ::close(fd);
            return 1;
        }

        HttpResponseReader reader(fd);
        ParsedResponse response;
        if (!reader.read_one(&response) || response.status != 413) {
            std::cerr << "expected 413, got " << response.status << '\n';
            ::close(fd);
            return 1;
        }

        ::close(fd);
    }

    // 5) queue overload -> 429 fast-fail.
    std::size_t overload_200 = 0;
    std::size_t overload_429 = 0;
    std::size_t overload_503 = 0;
    {
        constexpr int kOverloadClients = 48;
        std::mutex result_mutex;
        std::vector<std::thread> clients;
        clients.reserve(kOverloadClients);

        for (int i = 0; i < kOverloadClients; ++i) {
            clients.emplace_back([port, &result_mutex, &overload_200, &overload_429, &overload_503]() {
                const int fd = connect_to_server(port);
                if (fd < 0) {
                    return;
                }
                (void)set_recv_timeout(fd, 5s);

                HttpResponseReader reader(fd);
                if (!send_all(fd, build_request("POST", "/slow", "x", "close"))) {
                    ::close(fd);
                    return;
                }

                ParsedResponse response;
                if (reader.read_one(&response)) {
                    std::lock_guard<std::mutex> lock(result_mutex);
                    if (response.status == 200) {
                        ++overload_200;
                    } else if (response.status == 429) {
                        ++overload_429;
                    } else if (response.status == 503) {
                        ++overload_503;
                    }
                }

                ::close(fd);
            });
        }

        for (std::thread& client : clients) {
            client.join();
        }
    }

    if (overload_429 == 0) {
        std::cerr << "expected at least one 429 in overload case\n";
        return 1;
    }
    if (overload_200 == 0) {
        std::cerr << "expected at least one successful request in overload case\n";
        return 1;
    }

    if (!wait_until([&server]() {
            const rpc::net::ReactorHttpServerStats stats = server.stats();
            return stats.ingress_queue_size == 0 && stats.active_requests == 0;
        },
        6s)) {
        std::cerr << "server did not drain queue after overload\n";
        return 1;
    }

    const rpc::net::ReactorHttpServerStats stats = server.stats();
    if (stats.ingress_queue_high_watermark > options.ingress_queue_capacity) {
        std::cerr << "ingress queue high watermark exceeded capacity, high="
                  << stats.ingress_queue_high_watermark
                  << ", capacity=" << options.ingress_queue_capacity << '\n';
        return 1;
    }

    if (stats.read_buffer_high_watermark > 2 * 1024 * 1024
        || stats.write_buffer_high_watermark > 2 * 1024 * 1024) {
        std::cerr << "buffer high watermark too large, read_high="
                  << stats.read_buffer_high_watermark
                  << ", write_high=" << stats.write_buffer_high_watermark << '\n';
        return 1;
    }

    if (!stats.runtime_running
        || stats.runtime_worker_count == 0
        || stats.runtime_completed_coroutines == 0) {
        std::cerr << "runtime stats not exposed from server.stats(), running=" << stats.runtime_running
                  << ", workers=" << stats.runtime_worker_count
                  << ", completed=" << stats.runtime_completed_coroutines << '\n';
        return 1;
    }

    server.stop();

    // 6) connection limit -> 503 fast-fail.
    rpc::net::ReactorHttpServerOptions limit_options = options;
    limit_options.max_connections = 2;
    limit_options.ingress_queue_capacity = 8;
    limit_options.worker_threads = 1;

    rpc::net::ReactorHttpServer limit_server(limit_options);
    if (!limit_server.start()) {
        std::cerr << "failed to start limit server\n";
        return 1;
    }

    const std::uint16_t limit_port = limit_server.listen_port();
    if (limit_port == 0) {
        std::cerr << "invalid limit server port\n";
        return 1;
    }

    const int hold_fd_1 = connect_to_server(limit_port);
    const int hold_fd_2 = connect_to_server(limit_port);
    if (hold_fd_1 < 0 || hold_fd_2 < 0) {
        std::cerr << "failed to establish hold connections for limit test\n";
        if (hold_fd_1 >= 0) {
            ::close(hold_fd_1);
        }
        if (hold_fd_2 >= 0) {
            ::close(hold_fd_2);
        }
        return 1;
    }

    std::this_thread::sleep_for(80ms);

    const int rejected_fd = connect_to_server(limit_port);
    if (rejected_fd < 0) {
        std::cerr << "failed to establish overflow connection\n";
        ::close(hold_fd_1);
        ::close(hold_fd_2);
        return 1;
    }

    (void)set_recv_timeout(rejected_fd, 4s);
    if (!send_all(rejected_fd, build_request("GET", "/limit", "", "close"))) {
        std::cerr << "failed to send overflow request\n";
        ::close(rejected_fd);
        ::close(hold_fd_1);
        ::close(hold_fd_2);
        return 1;
    }

    HttpResponseReader rejected_reader(rejected_fd);
    ParsedResponse rejected_response;
    if (!rejected_reader.read_one(&rejected_response) || rejected_response.status != 503) {
        std::cerr << "expected 503 on connection limit, got " << rejected_response.status << '\n';
        ::close(rejected_fd);
        ::close(hold_fd_1);
        ::close(hold_fd_2);
        return 1;
    }

    ::close(rejected_fd);
    ::close(hold_fd_1);
    ::close(hold_fd_2);

    limit_server.stop();

    std::cout << "w8_reactor_http_test passed"
              << ", overload_200=" << overload_200
              << ", overload_429=" << overload_429
              << ", overload_503=" << overload_503
              << ", queue_high=" << stats.ingress_queue_high_watermark
              << ", read_high=" << stats.read_buffer_high_watermark
              << ", write_high=" << stats.write_buffer_high_watermark
              << '\n';

    return 0;
}
