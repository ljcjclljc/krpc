#pragma once

// File purpose:
// Define a bounded HTTP reactor server with accept/read/write main loop,
// request queue backpressure, and fast-fail overload handling.

#include <boost/asio/io_context.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/string_body.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rpc/gateway/http_gateway.h"

namespace rpc::net {

struct ReactorHttpServerOptions {
    std::string bind_address{"127.0.0.1"};
    std::uint16_t port{0};
    int backlog{256};

    std::size_t max_connections{1024};
    std::size_t ingress_queue_capacity{256};
    std::size_t worker_threads{2};

    std::size_t max_header_bytes{8 * 1024};
    std::size_t max_body_bytes{1024 * 1024};

    std::size_t max_connection_requests_inflight{8};
    std::size_t max_connection_write_buffer_bytes{128 * 1024};
    // 全局入口限流：0 表示按配置中心默认值；解析后为 0 表示关闭。
    std::size_t rate_limit_qps{0};
    // 限流突发桶容量（附加额度）。
    std::size_t rate_limit_burst{0};

    std::size_t epoll_max_events{256};

    bool enable_auth{false};
    // 是否在 runtime 协程任务内开启 Hook（默认关闭，需手动开启）。
    bool enable_runtime_hook{false};
    std::string auth_api_key;
    std::string auth_jwt_issuer;
    std::string auth_jwt_audience;
    std::uint64_t gateway_upstream_timeout_ms{300};
};

struct ReactorHttpServerStats {
    std::size_t active_connections{0};
    std::size_t active_requests{0};
    std::size_t ingress_queue_size{0};
    std::size_t ingress_queue_high_watermark{0};

    std::size_t rejected_429{0};
    std::size_t rejected_rate_limited_429{0};
    std::size_t rejected_503{0};

    std::size_t parser_400{0};
    std::size_t parser_413{0};
    std::size_t parser_431{0};
    std::size_t epoll_mod_calls{0};
    std::size_t epoll_mod_skipped{0};
    std::size_t send_syscalls{0};
    std::size_t recv_syscalls{0};

    std::size_t read_buffer_bytes{0};
    std::size_t write_buffer_bytes{0};
    std::size_t read_buffer_high_watermark{0};
    std::size_t write_buffer_high_watermark{0};

    bool runtime_running{false};
    std::size_t runtime_worker_count{0};
    std::size_t runtime_pending_tasks{0};
    std::size_t runtime_alive_coroutines{0};
    std::size_t runtime_completed_coroutines{0};
    std::size_t runtime_idle_switches{0};
    std::size_t runtime_steal_count{0};
};

class ReactorHttpServer {
public:
    explicit ReactorHttpServer(ReactorHttpServerOptions options = {});
    ~ReactorHttpServer();

    ReactorHttpServer(const ReactorHttpServer&) = delete;
    ReactorHttpServer& operator=(const ReactorHttpServer&) = delete;
    ReactorHttpServer(ReactorHttpServer&&) = delete;
    ReactorHttpServer& operator=(ReactorHttpServer&&) = delete;

    bool start();
    void stop();

    bool running() const noexcept;
    std::uint16_t listen_port() const noexcept;
    ReactorHttpServerStats stats() const noexcept;
    std::vector<rpc::gateway::RouteMetricsSnapshot> route_metrics_snapshot() const;
    rpc::gateway::GatewayTraceSnapshot last_trace_snapshot() const;

private:
    struct Connection {
        int fd{-1};
        std::unique_ptr<boost::beast::tcp_stream> stream;
        boost::beast::flat_buffer read_buffer;
        std::unique_ptr<boost::beast::http::request_parser<boost::beast::http::string_body>> request_parser;
        std::string write_buffer;
        std::size_t write_offset{0};
        std::size_t inflight_requests{0};
        bool write_interest_enabled{false};
        bool close_after_write{false};
    };

    struct IngressTask {
        int fd{-1};
        bool close_after_response{false};
        std::string method;
        std::string target;
        std::unordered_map<std::string, std::string> headers;
        std::string body;
    };

    struct CompletedTask {
        int fd{-1};
        bool close_after_response{false};
        std::string response;
    };

    static bool set_nonblocking(int fd);
    static bool set_close_on_exec(int fd);

    bool setup_listener();
    bool setup_epoll();
    void close_fds();

    void reactor_loop();
    void worker_loop();

    void handle_accept();
    void handle_read(int fd);
    void handle_write(int fd);
    void handle_completed_tasks();

    bool enqueue_ingress(IngressTask task);
    void enqueue_completed(CompletedTask task);

    void update_interest(int fd, bool want_write);
    void append_response(Connection& connection, std::string response, bool close_after_response);
    void close_connection(int fd);
    void reset_request_parser(Connection& connection);
    void refresh_rate_limit_from_config_if_needed();
    bool allow_request_now();
    static std::size_t pending_write_bytes(const Connection& connection) noexcept;

    std::string process_request(IngressTask task, bool* close_after_response);
    static std::string build_response(
        int status,
        const char* reason,
        const std::string& body,
        bool keep_alive
    );

    static void record_high_water(std::atomic<std::size_t>& high, std::size_t value);
    void notify_reactor();
    void drain_notify_fd();

    ReactorHttpServerOptions options_{};
    rpc::gateway::HttpGateway http_gateway_;
    boost::asio::io_context io_context_{1};

    std::atomic<bool> running_{false};
    bool owns_runtime_{false};
    int listen_fd_{-1};
    int epoll_fd_{-1};
    int notify_fd_{-1};
    std::uint16_t listen_port_{0};

    std::thread reactor_thread_;
    std::vector<std::thread> worker_threads_;

    std::unordered_map<int, Connection> connections_;

    mutable std::mutex ingress_mutex_;
    std::condition_variable ingress_cv_;
    std::deque<IngressTask> ingress_queue_;

    mutable std::mutex completed_mutex_;
    std::deque<CompletedTask> completed_queue_;

    std::atomic<std::size_t> active_connections_{0};
    std::atomic<std::size_t> active_requests_{0};
    std::atomic<std::size_t> ingress_queue_size_{0};
    std::atomic<std::size_t> ingress_queue_high_water_{0};

    std::atomic<std::size_t> rejected_429_{0};
    std::atomic<std::size_t> rejected_rate_limited_429_{0};
    std::atomic<std::size_t> rejected_503_{0};

    std::atomic<std::size_t> parser_400_{0};
    std::atomic<std::size_t> parser_413_{0};
    std::atomic<std::size_t> parser_431_{0};
    std::atomic<std::size_t> epoll_mod_calls_{0};
    std::atomic<std::size_t> epoll_mod_skipped_{0};
    std::atomic<std::size_t> send_syscalls_{0};
    std::atomic<std::size_t> recv_syscalls_{0};

    std::atomic<std::size_t> read_buffer_bytes_{0};
    std::atomic<std::size_t> write_buffer_bytes_{0};
    std::atomic<std::size_t> read_buffer_high_water_{0};
    std::atomic<std::size_t> write_buffer_high_water_{0};

    std::size_t effective_rate_limit_qps_{0};
    std::size_t effective_rate_limit_burst_{0};
    std::uint64_t rate_limit_config_version_{0};
    std::atomic<std::uint64_t> rate_limit_window_sec_{0};
    std::atomic<std::size_t> rate_limit_window_count_{0};
};

}  // namespace rpc::net
