#include "rpc/net/reactor_http_server.h"

// File purpose:
// Implement a bounded reactor HTTP server with epoll accept/read/write loop,
// bounded ingress queue, and overload fast-fail behavior.

#include <boost/beast/http.hpp>
#include <boost/system/error_code.hpp>

#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <future>
#include <limits>
#include <sstream>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "rpc/runtime/runtime.h"
#include "rpc/infra/infra.h"

namespace rpc::net {

namespace {

namespace http = boost::beast::http;
namespace asio = boost::asio;
using tcp = boost::asio::ip::tcp;

std::size_t normalize_workers(std::size_t worker_threads) {
    if (worker_threads != 0) {
        return worker_threads;
    }
    const std::size_t detected = static_cast<std::size_t>(std::thread::hardware_concurrency());
    return detected == 0 ? 1 : detected;
}

std::string lower_ascii(std::string_view value) {
    std::string lower;
    lower.reserve(value.size());
    for (char ch : value) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return lower;
}

std::size_t parse_config_size_from_snapshot(
    const rpc::infra::ConfigSnapshot& snapshot,
    const char* key,
    std::size_t fallback
) {
    if (key == nullptr) {
        return fallback;
    }
    const auto it = snapshot.values.find(key);
    if (it == snapshot.values.end() || it->second.empty()) {
        return fallback;
    }

    try {
        const unsigned long long parsed = std::stoull(it->second);
        if (parsed > static_cast<unsigned long long>(std::numeric_limits<std::size_t>::max())) {
            return fallback;
        }
        return static_cast<std::size_t>(parsed);
    } catch (...) {
        return fallback;
    }
}

}  // namespace

ReactorHttpServer::ReactorHttpServer(ReactorHttpServerOptions options)
    : options_(std::move(options)),
      http_gateway_(
          rpc::gateway::HttpGatewayOptions{
              std::chrono::milliseconds(options_.gateway_upstream_timeout_ms),
              rpc::gateway::AuthInterceptorOptions{
                  options_.enable_auth,
                  options_.auth_api_key,
                  options_.auth_jwt_issuer,
                  options_.auth_jwt_audience,
                  std::chrono::seconds(0),
              },
          }) {}

ReactorHttpServer::~ReactorHttpServer() {
    stop();
}

bool ReactorHttpServer::set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool ReactorHttpServer::set_close_on_exec(int fd) {
    const int flags = ::fcntl(fd, F_GETFD, 0);
    if (flags < 0) {
        return false;
    }
    return ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
}

bool ReactorHttpServer::setup_listener() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return false;
    }

    const int reuse = 1;
    (void)::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, static_cast<socklen_t>(sizeof(reuse)));

    if (!set_close_on_exec(listen_fd_) || !set_nonblocking(listen_fd_)) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options_.port);
    if (::inet_pton(AF_INET, options_.bind_address.c_str(), &addr.sin_addr) != 1) {
        return false;
    }

    if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
        return false;
    }

    if (::listen(listen_fd_, options_.backlog) != 0) {
        return false;
    }

    sockaddr_in bound{};
    socklen_t bound_len = static_cast<socklen_t>(sizeof(bound));
    if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &bound_len) != 0) {
        return false;
    }

    listen_port_ = ntohs(bound.sin_port);
    return true;
}

bool ReactorHttpServer::setup_epoll() {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0 && errno == EINVAL) {
        epoll_fd_ = ::epoll_create(256);
        if (epoll_fd_ >= 0) {
            (void)set_close_on_exec(epoll_fd_);
        }
    }
    if (epoll_fd_ < 0) {
        return false;
    }

    notify_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify_fd_ < 0 && errno == EINVAL) {
        notify_fd_ = ::eventfd(0, 0);
        if (notify_fd_ >= 0) {
            (void)set_close_on_exec(notify_fd_);
            (void)set_nonblocking(notify_fd_);
        }
    }
    if (notify_fd_ < 0) {
        return false;
    }

    epoll_event listen_event{};
    listen_event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    listen_event.data.fd = listen_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &listen_event) != 0) {
        return false;
    }

    epoll_event notify_event{};
    notify_event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    notify_event.data.fd = notify_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, notify_fd_, &notify_event) != 0) {
        return false;
    }

    return true;
}

void ReactorHttpServer::close_fds() {
    if (notify_fd_ >= 0) {
        ::close(notify_fd_);
        notify_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

bool ReactorHttpServer::start() {
    if (running_.load(std::memory_order_acquire)) {
        return true;
    }

    if (options_.ingress_queue_capacity == 0
        || options_.max_connections == 0
        || options_.max_connection_requests_inflight == 0
        || options_.max_connection_write_buffer_bytes == 0) {
        return false;
    }

    epoll_mod_calls_.store(0, std::memory_order_release);
    epoll_mod_skipped_.store(0, std::memory_order_release);
    send_syscalls_.store(0, std::memory_order_release);
    recv_syscalls_.store(0, std::memory_order_release);
    rejected_rate_limited_429_.store(0, std::memory_order_release);
    rate_limit_window_sec_.store(0, std::memory_order_release);
    rate_limit_window_count_.store(0, std::memory_order_release);
    rate_limit_config_version_ = 0;
    refresh_rate_limit_from_config_if_needed();

    if (!rpc::runtime::runtime_ready()) {
        try {
            rpc::runtime::start_runtime();
            owns_runtime_ = true;
        } catch (...) {
            owns_runtime_ = false;
            return false;
        }
    } else {
        owns_runtime_ = false;
    }

    if (!setup_listener()) {
        close_fds();
        if (owns_runtime_) {
            rpc::runtime::stop_runtime();
            owns_runtime_ = false;
        }
        return false;
    }

    if (!setup_epoll()) {
        close_fds();
        if (owns_runtime_) {
            rpc::runtime::stop_runtime();
            owns_runtime_ = false;
        }
        return false;
    }

    running_.store(true, std::memory_order_release);

    const std::size_t workers = normalize_workers(options_.worker_threads);
    worker_threads_.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
        worker_threads_.emplace_back(&ReactorHttpServer::worker_loop, this);
    }

    reactor_thread_ = std::thread(&ReactorHttpServer::reactor_loop, this);
    return true;
}

void ReactorHttpServer::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        close_fds();
        if (owns_runtime_) {
            rpc::runtime::stop_runtime();
            owns_runtime_ = false;
        }
        return;
    }

    ingress_cv_.notify_all();
    notify_reactor();

    if (reactor_thread_.joinable()) {
        reactor_thread_.join();
    }

    for (std::thread& worker : worker_threads_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    worker_threads_.clear();

    for (auto& [fd, connection] : connections_) {
        (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        if (connection.stream) {
            boost::system::error_code ec;
            connection.stream->socket().close(ec);
        } else {
            ::close(fd);
        }
        connection.read_buffer.consume(connection.read_buffer.size());
        connection.request_parser.reset();
        connection.write_buffer.clear();
        connection.write_offset = 0;
    }
    connections_.clear();

    {
        std::lock_guard<std::mutex> lock(ingress_mutex_);
        ingress_queue_.clear();
        ingress_queue_size_.store(0, std::memory_order_release);
    }
    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        completed_queue_.clear();
    }

    active_connections_.store(0, std::memory_order_release);
    active_requests_.store(0, std::memory_order_release);
    read_buffer_bytes_.store(0, std::memory_order_release);
    write_buffer_bytes_.store(0, std::memory_order_release);

    close_fds();

    if (owns_runtime_) {
        rpc::runtime::stop_runtime();
        owns_runtime_ = false;
    }
}

bool ReactorHttpServer::running() const noexcept {
    return running_.load(std::memory_order_acquire);
}

std::uint16_t ReactorHttpServer::listen_port() const noexcept {
    return listen_port_;
}

ReactorHttpServerStats ReactorHttpServer::stats() const noexcept {
    ReactorHttpServerStats snapshot;
    snapshot.active_connections = active_connections_.load(std::memory_order_acquire);
    snapshot.active_requests = active_requests_.load(std::memory_order_acquire);
    snapshot.ingress_queue_size = ingress_queue_size_.load(std::memory_order_acquire);
    snapshot.ingress_queue_high_watermark = ingress_queue_high_water_.load(std::memory_order_acquire);

    snapshot.rejected_429 = rejected_429_.load(std::memory_order_acquire);
    snapshot.rejected_rate_limited_429 = rejected_rate_limited_429_.load(std::memory_order_acquire);
    snapshot.rejected_503 = rejected_503_.load(std::memory_order_acquire);

    snapshot.parser_400 = parser_400_.load(std::memory_order_acquire);
    snapshot.parser_413 = parser_413_.load(std::memory_order_acquire);
    snapshot.parser_431 = parser_431_.load(std::memory_order_acquire);
    snapshot.epoll_mod_calls = epoll_mod_calls_.load(std::memory_order_acquire);
    snapshot.epoll_mod_skipped = epoll_mod_skipped_.load(std::memory_order_acquire);
    snapshot.send_syscalls = send_syscalls_.load(std::memory_order_acquire);
    snapshot.recv_syscalls = recv_syscalls_.load(std::memory_order_acquire);

    snapshot.read_buffer_bytes = read_buffer_bytes_.load(std::memory_order_acquire);
    snapshot.write_buffer_bytes = write_buffer_bytes_.load(std::memory_order_acquire);
    snapshot.read_buffer_high_watermark = read_buffer_high_water_.load(std::memory_order_acquire);
    snapshot.write_buffer_high_watermark = write_buffer_high_water_.load(std::memory_order_acquire);

    const rpc::runtime::RuntimeSchedulerSnapshot runtime_snapshot = rpc::runtime::runtime_scheduler_snapshot();
    snapshot.runtime_running = runtime_snapshot.running;
    snapshot.runtime_worker_count = runtime_snapshot.worker_count;
    snapshot.runtime_pending_tasks = runtime_snapshot.pending_tasks;
    snapshot.runtime_alive_coroutines = runtime_snapshot.alive_coroutines;
    snapshot.runtime_completed_coroutines = runtime_snapshot.completed_coroutines;
    snapshot.runtime_idle_switches = runtime_snapshot.idle_switches;
    snapshot.runtime_steal_count = runtime_snapshot.steal_count;
    return snapshot;
}

std::vector<rpc::gateway::RouteMetricsSnapshot> ReactorHttpServer::route_metrics_snapshot() const {
    return http_gateway_.route_metrics_snapshot();
}

rpc::gateway::GatewayTraceSnapshot ReactorHttpServer::last_trace_snapshot() const {
    return http_gateway_.last_trace_snapshot();
}

void ReactorHttpServer::reactor_loop() {
    std::vector<epoll_event> events(options_.epoll_max_events == 0 ? 1 : options_.epoll_max_events);

    while (running_.load(std::memory_order_acquire)) {
        const int ready = ::epoll_wait(
            epoll_fd_,
            events.data(),
            static_cast<int>(events.size()),
            100
        );

        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        if (ready == 0) {
            continue;
        }

        for (int i = 0; i < ready; ++i) {
            const epoll_event& event = events[static_cast<std::size_t>(i)];
            const int fd = event.data.fd;

            if (fd == listen_fd_) {
                handle_accept();
                continue;
            }

            if (fd == notify_fd_) {
                drain_notify_fd();
                handle_completed_tasks();
                continue;
            }

            if ((event.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0U) {
                handle_read(fd);
                handle_write(fd);
                auto connection_it = connections_.find(fd);
                if (connection_it != connections_.end() && pending_write_bytes(connection_it->second) == 0) {
                    close_connection(fd);
                }
                continue;
            }

            if ((event.events & EPOLLIN) != 0U) {
                handle_read(fd);
            }
            if ((event.events & EPOLLOUT) != 0U) {
                handle_write(fd);
            }
        }
    }
}

void ReactorHttpServer::worker_loop() {
    while (true) {
        IngressTask task;
        std::size_t queue_size_after_pop = 0;

        {
            std::unique_lock<std::mutex> lock(ingress_mutex_);
            ingress_cv_.wait(lock, [this]() {
                return !running_.load(std::memory_order_acquire) || !ingress_queue_.empty();
            });

            if (ingress_queue_.empty()) {
                if (!running_.load(std::memory_order_acquire)) {
                    return;
                }
                continue;
            }

            task = std::move(ingress_queue_.front());
            ingress_queue_.pop_front();
            queue_size_after_pop = ingress_queue_.size();
        }

        ingress_queue_size_.store(queue_size_after_pop, std::memory_order_release);

        active_requests_.fetch_add(1, std::memory_order_relaxed);
        bool close_after_response = task.close_after_response;
        std::string response = process_request(std::move(task), &close_after_response);
        active_requests_.fetch_sub(1, std::memory_order_relaxed);

        enqueue_completed(CompletedTask{
            task.fd,
            close_after_response,
            std::move(response),
        });
    }
}

void ReactorHttpServer::handle_accept() {
    while (true) {
        sockaddr_in peer_addr{};
        socklen_t peer_len = static_cast<socklen_t>(sizeof(peer_addr));
        const int client_fd = ::accept(
            listen_fd_,
            reinterpret_cast<sockaddr*>(&peer_addr),
            &peer_len
        );

        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            return;
        }

        if (!set_close_on_exec(client_fd) || !set_nonblocking(client_fd)) {
            ::close(client_fd);
            continue;
        }

        if (active_connections_.load(std::memory_order_acquire) >= options_.max_connections) {
            ++rejected_503_;
            const std::string overload = build_response(503, "Service Unavailable", "connection_limit", false);
            boost::system::error_code ec;
            tcp::socket socket(io_context_);
            socket.assign(tcp::v4(), client_fd, ec);
            if (!ec) {
                (void)socket.send(
                    asio::buffer(overload.data(), overload.size()),
                    static_cast<asio::socket_base::message_flags>(MSG_NOSIGNAL),
                    ec
                );
                socket.close(ec);
            } else {
                ::close(client_fd);
            }
            continue;
        }

        epoll_event event{};
        event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
        event.data.fd = client_fd;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &event) != 0) {
            ::close(client_fd);
            continue;
        }

        auto stream = std::make_unique<boost::beast::tcp_stream>(io_context_);
        boost::system::error_code ec;
        stream->socket().assign(tcp::v4(), client_fd, ec);
        if (ec) {
            ::close(client_fd);
            continue;
        }

        stream->socket().non_blocking(true, ec);
        if (ec) {
            boost::system::error_code close_ec;
            stream->socket().close(close_ec);
            continue;
        }

        Connection connection;
        connection.fd = client_fd;
        connection.stream = std::move(stream);
        reset_request_parser(connection);
        connections_.emplace(client_fd, std::move(connection));
        active_connections_.fetch_add(1, std::memory_order_relaxed);
    }
}

void ReactorHttpServer::handle_read(int fd) {
    auto connection_it = connections_.find(fd);
    if (connection_it == connections_.end()) {
        return;
    }
    Connection& connection = connection_it->second;

    if (!connection.stream) {
        close_connection(fd);
        return;
    }

    while (true) {
        auto mutable_buffer = connection.read_buffer.prepare(4096);
        boost::system::error_code ec;
        recv_syscalls_.fetch_add(1, std::memory_order_relaxed);
        const std::size_t n = connection.stream->socket().receive(
            mutable_buffer,
            static_cast<asio::socket_base::message_flags>(0),
            ec
        );
        connection.read_buffer.commit(n);

        if (!ec && n > 0) {
            const std::size_t total = read_buffer_bytes_.fetch_add(n, std::memory_order_relaxed)
                + n;
            record_high_water(read_buffer_high_water_, total);
            continue;
        }

        if (ec == boost::asio::error::eof || ec == boost::asio::error::connection_reset) {
            close_connection(fd);
            return;
        }

        if (ec == boost::asio::error::interrupted) {
            continue;
        }
        if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
            break;
        }
        if (!ec && n == 0) {
            break;
        }

        close_connection(fd);
        return;
    }

    connection_it = connections_.find(fd);
    if (connection_it == connections_.end()) {
        return;
    }

    Connection& current = connection_it->second;

    while (current.read_buffer.size() > 0) {
        if (!current.request_parser) {
            reset_request_parser(current);
        }

        boost::system::error_code parse_ec;
        const std::size_t consumed = current.request_parser->put(current.read_buffer.data(), parse_ec);
        if (consumed > 0) {
            current.read_buffer.consume(consumed);
            read_buffer_bytes_.fetch_sub(consumed, std::memory_order_relaxed);
        }

        if (parse_ec == http::error::need_more) {
            break;
        }

        if (parse_ec == http::error::header_limit) {
            ++parser_431_;
            append_response(
                current,
                build_response(431, "Request Header Fields Too Large", "header_too_large", false),
                true
            );
            reset_request_parser(current);
            read_buffer_bytes_.fetch_sub(current.read_buffer.size(), std::memory_order_relaxed);
            current.read_buffer.consume(current.read_buffer.size());
            break;
        }

        if (parse_ec == http::error::body_limit) {
            ++parser_413_;
            append_response(
                current,
                build_response(413, "Payload Too Large", "body_too_large", false),
                true
            );
            reset_request_parser(current);
            read_buffer_bytes_.fetch_sub(current.read_buffer.size(), std::memory_order_relaxed);
            current.read_buffer.consume(current.read_buffer.size());
            break;
        }

        if (parse_ec) {
            ++parser_400_;
            append_response(
                current,
                build_response(400, "Bad Request", "bad_request", false),
                true
            );
            reset_request_parser(current);
            read_buffer_bytes_.fetch_sub(current.read_buffer.size(), std::memory_order_relaxed);
            current.read_buffer.consume(current.read_buffer.size());
            break;
        }

        if (!current.request_parser->is_done()) {
            break;
        }

        http::request<http::string_body> parsed_request = current.request_parser->release();
        reset_request_parser(current);

        if (parsed_request.target().empty()
            || parsed_request.find(http::field::transfer_encoding) != parsed_request.end()) {
            ++parser_400_;
            append_response(
                current,
                build_response(400, "Bad Request", "bad_request", false),
                true
            );
            read_buffer_bytes_.fetch_sub(current.read_buffer.size(), std::memory_order_relaxed);
            current.read_buffer.consume(current.read_buffer.size());
            break;
        }

        if (!allow_request_now()) {
            ++rejected_429_;
            ++rejected_rate_limited_429_;
            append_response(
                current,
                build_response(429, "Too Many Requests", "rate_limited", parsed_request.keep_alive()),
                !parsed_request.keep_alive()
            );
            continue;
        }

        if (current.inflight_requests >= options_.max_connection_requests_inflight) {
            ++rejected_503_;
            append_response(
                current,
                build_response(503, "Service Unavailable", "connection_backpressure", false),
                true
            );
            continue;
        }

        IngressTask task;
        task.fd = fd;
        task.close_after_response = !parsed_request.keep_alive();
        task.method = std::string(parsed_request.method_string());
        task.target = std::string(parsed_request.target());
        task.body = std::move(parsed_request.body());

        for (const auto& field : parsed_request.base()) {
            const std::string key = lower_ascii(field.name_string());
            const std::string value = std::string(field.value());
            auto it = task.headers.find(key);
            if (it == task.headers.end()) {
                task.headers.emplace(key, value);
            } else {
                it->second.push_back(',');
                it->second.append(value);
            }
        }

        if (!enqueue_ingress(std::move(task))) {
            ++rejected_429_;
            append_response(
                current,
                build_response(429, "Too Many Requests", "ingress_queue_full", parsed_request.keep_alive()),
                !parsed_request.keep_alive()
            );
        } else {
            ++current.inflight_requests;
            if (!parsed_request.keep_alive()) {
                current.close_after_write = true;
            }
        }
    }

    auto final_it = connections_.find(fd);
    if (final_it == connections_.end()) {
        return;
    }

    Connection& final_connection = final_it->second;
    if (final_connection.close_after_write
        && final_connection.inflight_requests == 0
        && pending_write_bytes(final_connection) == 0) {
        close_connection(fd);
    }
}

void ReactorHttpServer::handle_write(int fd) {
    auto connection_it = connections_.find(fd);
    if (connection_it == connections_.end()) {
        return;
    }

    Connection& connection = connection_it->second;
    if (!connection.stream) {
        close_connection(fd);
        return;
    }

    while (pending_write_bytes(connection) > 0) {
        const std::size_t offset = connection.write_offset;
        const std::size_t remaining = connection.write_buffer.size() - offset;
        boost::system::error_code ec;
        send_syscalls_.fetch_add(1, std::memory_order_relaxed);
        const std::size_t n = connection.stream->socket().send(
            asio::buffer(connection.write_buffer.data() + static_cast<std::ptrdiff_t>(offset), remaining),
            static_cast<asio::socket_base::message_flags>(MSG_NOSIGNAL),
            ec
        );

        if (!ec && n > 0) {
            connection.write_offset += n;
            if (connection.write_offset >= connection.write_buffer.size()) {
                connection.write_buffer.clear();
                connection.write_offset = 0;
            }
            write_buffer_bytes_.fetch_sub(n, std::memory_order_relaxed);
            continue;
        }

        if (ec == boost::asio::error::interrupted) {
            continue;
        }
        if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
            break;
        }
        if (!ec && n == 0) {
            break;
        }

        close_connection(fd);
        return;
    }

    auto final_it = connections_.find(fd);
    if (final_it == connections_.end()) {
        return;
    }

    Connection& final_connection = final_it->second;
    if (pending_write_bytes(final_connection) == 0) {
        update_interest(fd, false);
        if (final_connection.close_after_write && final_connection.inflight_requests == 0) {
            close_connection(fd);
        }
    } else {
        update_interest(fd, true);
    }
}

void ReactorHttpServer::handle_completed_tasks() {
    std::deque<CompletedTask> completed;
    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        completed.swap(completed_queue_);
    }

    for (CompletedTask& task : completed) {
        auto connection_it = connections_.find(task.fd);
        if (connection_it == connections_.end()) {
            continue;
        }

        Connection& connection = connection_it->second;
        if (connection.inflight_requests > 0) {
            --connection.inflight_requests;
        }

        append_response(connection, std::move(task.response), task.close_after_response);

        auto final_it = connections_.find(task.fd);
        if (final_it == connections_.end()) {
            continue;
        }

        Connection& final_connection = final_it->second;
        if (final_connection.close_after_write
            && final_connection.inflight_requests == 0
            && pending_write_bytes(final_connection) == 0) {
            close_connection(task.fd);
        }
    }
}

bool ReactorHttpServer::enqueue_ingress(IngressTask task) {
    std::size_t size_after_push = 0;
    {
        std::lock_guard<std::mutex> lock(ingress_mutex_);
        if (ingress_queue_.size() >= options_.ingress_queue_capacity) {
            return false;
        }

        ingress_queue_.push_back(std::move(task));
        size_after_push = ingress_queue_.size();
    }

    ingress_queue_size_.store(size_after_push, std::memory_order_release);
    record_high_water(ingress_queue_high_water_, size_after_push);

    ingress_cv_.notify_one();
    return true;
}

void ReactorHttpServer::enqueue_completed(CompletedTask task) {
    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        completed_queue_.push_back(std::move(task));
    }
    notify_reactor();
}

void ReactorHttpServer::update_interest(int fd, bool want_write) {
    auto connection_it = connections_.find(fd);
    if (connection_it == connections_.end()) {
        return;
    }
    Connection& connection = connection_it->second;
    if (connection.write_interest_enabled == want_write) {
        epoll_mod_skipped_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    epoll_event event{};
    event.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    if (want_write) {
        event.events |= EPOLLOUT;
    }
    event.data.fd = fd;

    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event);
    connection.write_interest_enabled = want_write;
    epoll_mod_calls_.fetch_add(1, std::memory_order_relaxed);
}

void ReactorHttpServer::append_response(Connection& connection, std::string response, bool close_after_response) {
    if (close_after_response) {
        connection.close_after_write = true;
    }

    if (connection.write_offset > 0) {
        if (connection.write_offset >= connection.write_buffer.size()) {
            connection.write_buffer.clear();
            connection.write_offset = 0;
        } else if (connection.write_offset > (connection.write_buffer.size() / 2)) {
            connection.write_buffer.erase(0, connection.write_offset);
            connection.write_offset = 0;
        }
    }

    if (response.empty()) {
        update_interest(connection.fd, pending_write_bytes(connection) > 0);
        return;
    }

    if (pending_write_bytes(connection) + response.size() > options_.max_connection_write_buffer_bytes) {
        ++rejected_503_;
        connection.close_after_write = true;

        if (pending_write_bytes(connection) == 0) {
            std::string fallback = build_response(503, "Service Unavailable", "write_buffer_overflow", false);
            if (fallback.size() <= options_.max_connection_write_buffer_bytes) {
                const std::size_t total = write_buffer_bytes_.fetch_add(fallback.size(), std::memory_order_relaxed)
                    + fallback.size();
                record_high_water(write_buffer_high_water_, total);
                connection.write_buffer = std::move(fallback);
                connection.write_offset = 0;
            }
        }

        update_interest(connection.fd, pending_write_bytes(connection) > 0);
        return;
    }

    const std::size_t total = write_buffer_bytes_.fetch_add(response.size(), std::memory_order_relaxed)
        + response.size();
    record_high_water(write_buffer_high_water_, total);
    connection.write_buffer.append(response);
    update_interest(connection.fd, true);
}

void ReactorHttpServer::close_connection(int fd) {
    auto connection_it = connections_.find(fd);
    if (connection_it == connections_.end()) {
        return;
    }

    Connection& connection = connection_it->second;

    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    if (connection.stream) {
        boost::system::error_code ec;
        connection.stream->socket().close(ec);
    } else {
        ::close(fd);
    }

    if (connection.read_buffer.size() > 0) {
        read_buffer_bytes_.fetch_sub(connection.read_buffer.size(), std::memory_order_relaxed);
        connection.read_buffer.consume(connection.read_buffer.size());
    }
    if (pending_write_bytes(connection) > 0) {
        write_buffer_bytes_.fetch_sub(pending_write_bytes(connection), std::memory_order_relaxed);
    }

    connections_.erase(connection_it);
    active_connections_.fetch_sub(1, std::memory_order_relaxed);
}

void ReactorHttpServer::reset_request_parser(Connection& connection) {
    auto parser = std::make_unique<http::request_parser<http::string_body>>();
    parser->header_limit(options_.max_header_bytes);
    parser->body_limit(options_.max_body_bytes);
    connection.request_parser = std::move(parser);
}

void ReactorHttpServer::refresh_rate_limit_from_config_if_needed() {
    if (options_.rate_limit_qps != 0 && options_.rate_limit_burst != 0) {
        effective_rate_limit_qps_ = options_.rate_limit_qps;
        effective_rate_limit_burst_ = options_.rate_limit_burst;
        return;
    }

    rpc::infra::ConfigSnapshot snapshot;
    try {
        snapshot = rpc::infra::config_repository().snapshot();
    } catch (...) {
        effective_rate_limit_qps_ = options_.rate_limit_qps;
        effective_rate_limit_burst_ = options_.rate_limit_burst;
        return;
    }

    if (snapshot.version == rate_limit_config_version_) {
        return;
    }

    std::size_t parsed_qps = options_.rate_limit_qps;
    if (parsed_qps == 0) {
        parsed_qps = parse_config_size_from_snapshot(snapshot, "gateway.rate_limit.qps", 0);
    }

    std::size_t parsed_burst = options_.rate_limit_burst;
    if (parsed_burst == 0) {
        parsed_burst = parse_config_size_from_snapshot(snapshot, "gateway.rate_limit.burst", 0);
    }

    effective_rate_limit_qps_ = parsed_qps;
    effective_rate_limit_burst_ = parsed_burst;
    rate_limit_config_version_ = snapshot.version;
    rate_limit_window_sec_.store(0, std::memory_order_release);
    rate_limit_window_count_.store(0, std::memory_order_release);
}

bool ReactorHttpServer::allow_request_now() {
    refresh_rate_limit_from_config_if_needed();

    if (effective_rate_limit_qps_ == 0) {
        return true;
    }

    const std::uint64_t now_sec = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count()
    );
    std::uint64_t window_sec = rate_limit_window_sec_.load(std::memory_order_acquire);
    if (window_sec != now_sec
        && rate_limit_window_sec_.compare_exchange_strong(
            window_sec,
            now_sec,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        rate_limit_window_count_.store(0, std::memory_order_release);
    } else if (window_sec != now_sec) {
        rate_limit_window_sec_.store(now_sec, std::memory_order_release);
        rate_limit_window_count_.store(0, std::memory_order_release);
    }

    const std::size_t allowance = effective_rate_limit_qps_ + effective_rate_limit_burst_;
    const std::size_t current = rate_limit_window_count_.fetch_add(1, std::memory_order_acq_rel) + 1;
    return current <= allowance;
}

std::size_t ReactorHttpServer::pending_write_bytes(const Connection& connection) noexcept {
    if (connection.write_offset >= connection.write_buffer.size()) {
        return 0;
    }
    return connection.write_buffer.size() - connection.write_offset;
}

std::string ReactorHttpServer::process_request(IngressTask task, bool* close_after_response) {
    rpc::gateway::GatewayHttpRequest gateway_request;
    gateway_request.method = std::move(task.method);
    gateway_request.target = std::move(task.target);
    gateway_request.headers = std::move(task.headers);
    gateway_request.body = std::move(task.body);

    rpc::gateway::GatewayHttpResponse gateway_response;
    if (rpc::runtime::runtime_ready()) {
        auto response_promise = std::make_shared<std::promise<rpc::gateway::GatewayHttpResponse>>();
        std::future<rpc::gateway::GatewayHttpResponse> response_future = response_promise->get_future();
        bool submitted = false;

        try {
            (void)rpc::runtime::submit([this, gateway_request, response_promise]() mutable {
                try {
                    rpc::runtime::ScopedRuntimeHookEnable scoped_hook(options_.enable_runtime_hook);
                    response_promise->set_value(http_gateway_.handle(gateway_request));
                } catch (...) {
                    response_promise->set_exception(std::current_exception());
                }
            });
            submitted = true;
        } catch (...) {
            submitted = false;
        }

        if (submitted) {
            try {
                gateway_response = response_future.get();
            } catch (...) {
                gateway_response = rpc::gateway::GatewayHttpResponse{
                    500,
                    "Internal Server Error",
                    "runtime_gateway_dispatch_failed",
                    true,
                };
            }
        } else {
            gateway_response = http_gateway_.handle(gateway_request);
        }
    } else {
        gateway_response = http_gateway_.handle(gateway_request);
    }

    const bool keep_alive = !task.close_after_response && gateway_response.keep_alive;
    if (close_after_response != nullptr) {
        *close_after_response = !keep_alive;
    }

    return build_response(
        gateway_response.status,
        gateway_response.reason.c_str(),
        gateway_response.body,
        keep_alive
    );
}

std::string ReactorHttpServer::build_response(
    int status,
    const char* reason,
    const std::string& body,
    bool keep_alive
) {
    http::response<http::string_body> response;
    response.version(11);
    response.result(static_cast<http::status>(status));
    response.reason(reason);
    response.keep_alive(keep_alive);
    response.set(http::field::content_type, "text/plain");
    response.body() = body;
    response.prepare_payload();

    std::ostringstream stream;
    stream << response;
    return stream.str();
}

void ReactorHttpServer::record_high_water(std::atomic<std::size_t>& high, std::size_t value) {
    std::size_t current = high.load(std::memory_order_acquire);
    while (value > current
           && !high.compare_exchange_weak(
               current,
               value,
               std::memory_order_acq_rel,
               std::memory_order_acquire)) {
    }
}

void ReactorHttpServer::notify_reactor() {
    if (notify_fd_ < 0) {
        return;
    }

    const std::uint64_t one = 1;
    const ssize_t written = ::write(notify_fd_, &one, sizeof(one));
    if (written < 0 && errno != EAGAIN) {
        // Ignore wake-up write failures. Event loop will make progress on other events.
    }
}

void ReactorHttpServer::drain_notify_fd() {
    if (notify_fd_ < 0) {
        return;
    }

    std::uint64_t value = 0;
    while (::read(notify_fd_, &value, sizeof(value)) > 0) {
    }
}

}  // namespace rpc::net
