#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/system/error_code.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <limits>
#include <future>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "krpc.pb.h"
#include "rpc/infra/memory_pool.h"
#include "rpc/gateway/http_gateway.h"
#include "rpc/net/reactor_http_server.h"
#include "rpc/infra/structured_log.h"
#include "rpc/rpc/client.h"
#include "rpc/rpc/krpc_channel.h"
#include "rpc/runtime/runtime.h"

namespace {

namespace http = boost::beast::http;
using Clock = std::chrono::steady_clock;

bool expect_true(bool condition, const char* message) {
    if (condition) {
        return true;
    }
    std::cerr << message << '\n';
    return false;
}

class StaticDiscovery final : public rpc::client::IServiceDiscovery {
public:
    explicit StaticDiscovery(std::vector<rpc::client::ServiceNode> nodes)
        : nodes_(std::move(nodes)) {}

    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override {
        if (service_name == "perf.echo") {
            return nodes_;
        }
        return {};
    }

private:
    std::vector<rpc::client::ServiceNode> nodes_;
};

class ScopedEchoBackend final {
public:
    explicit ScopedEchoBackend(std::string tag) : tag_(std::move(tag)) {}

    ~ScopedEchoBackend() {
        stop();
    }

    bool start() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return false;
        }

        const int reuse = 1;
        (void)::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, static_cast<socklen_t>(sizeof(reuse)));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(0);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
            stop();
            return false;
        }
        if (::listen(listen_fd_, 64) != 0) {
            stop();
            return false;
        }

        sockaddr_in bound{};
        socklen_t bound_len = static_cast<socklen_t>(sizeof(bound));
        if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &bound_len) != 0) {
            stop();
            return false;
        }
        port_ = ntohs(bound.sin_port);
        if (port_ == 0) {
            stop();
            return false;
        }

        worker_ = std::thread([this]() {
            while (true) {
                sockaddr_in peer{};
                socklen_t peer_len = static_cast<socklen_t>(sizeof(peer));
                const int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
                if (client_fd < 0) {
                    return;
                }

                std::thread([this, client_fd]() {
                    handle_client(client_fd);
                }).detach();
            }
        });
        return true;
    }

    void stop() {
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    std::uint16_t port() const noexcept {
        return port_;
    }

private:
    void handle_client(int client_fd) {
        char buffer[4096];
        while (true) {
            std::string request_payload;
            while (true) {
                const ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
                if (n > 0) {
                    request_payload.append(buffer, static_cast<std::size_t>(n));
                    if (static_cast<std::size_t>(n) < sizeof(buffer)) {
                        break;
                    }
                    continue;
                }
                if (n == 0) {
                    ::close(client_fd);
                    return;
                }
                if (errno == EINTR) {
                    continue;
                }
                ::close(client_fd);
                return;
            }

            const std::string response = tag_ + ":" + request_payload;
            std::size_t sent = 0;
            while (sent < response.size()) {
                const ssize_t n = ::send(
                    client_fd,
                    response.data() + static_cast<std::ptrdiff_t>(sent),
                    response.size() - sent,
                    MSG_NOSIGNAL
                );
                if (n > 0) {
                    sent += static_cast<std::size_t>(n);
                    continue;
                }
                if (n < 0 && errno == EINTR) {
                    continue;
                }
                ::close(client_fd);
                return;
            }
        }
    }

    std::string tag_;
    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::thread worker_;
};

bool set_recv_timeout(int fd, std::chrono::milliseconds timeout) {
    timeval tv{};
    tv.tv_sec = static_cast<long>(timeout.count() / 1000);
    tv.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);
    return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, static_cast<socklen_t>(sizeof(tv))) == 0;
}

int connect_to_server(std::uint16_t port) {
    for (int attempt = 0; attempt < 80; ++attempt) {
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
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return -1;
}

bool send_all(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(
            fd,
            data.data() + static_cast<std::ptrdiff_t>(sent),
            data.size() - sent,
            MSG_NOSIGNAL
        );
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

struct ParsedResponse {
    int status{0};
    std::string body;
};

class HttpResponseReader {
public:
    explicit HttpResponseReader(int fd) : fd_(fd) {}

    bool read_one(ParsedResponse* response) {
        if (response == nullptr) {
            return false;
        }

        http::response_parser<http::string_body> parser;
        parser.body_limit(8 * 1024 * 1024);

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

std::uint64_t percentile_us_from_sorted(const std::vector<std::uint64_t>& sorted, double p) {
    if (sorted.empty()) {
        return 0;
    }
    const double clamped = std::clamp(p, 0.0, 1.0);
    const std::size_t index = static_cast<std::size_t>(clamped * static_cast<double>(sorted.size() - 1));
    return sorted[index];
}

std::string build_http_request(
    const std::string& method,
    const std::string& target,
    const std::unordered_map<std::string, std::string>& headers,
    const std::string& body
) {
    std::ostringstream oss;
    oss << method << ' ' << target << " HTTP/1.1\r\n";
    oss << "Host: 127.0.0.1\r\n";
    oss << "Connection: keep-alive\r\n";
    for (const auto& kv : headers) {
        oss << kv.first << ": " << kv.second << "\r\n";
    }
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "\r\n";
    oss << body;
    return oss.str();
}

struct ProtobufCompareResult {
    std::uint64_t legacy_encode_ns_per_op{0};
    std::uint64_t optimized_encode_ns_per_op{0};
    std::uint64_t legacy_decode_ns_per_op{0};
    std::uint64_t optimized_decode_ns_per_op{0};
};

ProtobufCompareResult run_protobuf_compare() {
    rpc::rpc::KrpcRequestFrame frame;
    frame.service = "perf.echo";
    frame.method = "Echo";
    frame.payload = std::string(256, 'x');
    frame.timeout_ms = 120;
    frame.max_retries = 2;
    for (int i = 0; i < 12; ++i) {
        frame.metadata.emplace("k" + std::to_string(i), "v" + std::to_string(i));
    }

    constexpr std::size_t kIters = 30000;

    std::string wire;

    const auto legacy_encode_started = Clock::now();
    for (std::size_t i = 0; i < kIters; ++i) {
        ::rpc::krpc::KrpcRequest message;
        message.set_service(frame.service);
        message.set_method(frame.method);
        message.set_payload(frame.payload);
        message.set_timeout_ms(frame.timeout_ms);
        message.set_max_retries(static_cast<std::uint64_t>(frame.max_retries));
        auto* metadata = message.mutable_metadata();
        for (const auto& kv : frame.metadata) {
            (*metadata)[kv.first] = kv.second;
        }
        if (!message.SerializeToString(&wire)) {
            break;
        }
    }
    const auto legacy_encode_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - legacy_encode_started).count()
    );

    const auto optimized_encode_started = Clock::now();
    for (std::size_t i = 0; i < kIters; ++i) {
        if (!rpc::rpc::KrpcChannel::encode_request_protobuf(frame, &wire)) {
            break;
        }
    }
    const auto optimized_encode_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - optimized_encode_started).count()
    );

    rpc::rpc::KrpcChannel::encode_request_protobuf(frame, &wire);

    const auto legacy_decode_started = Clock::now();
    for (std::size_t i = 0; i < kIters; ++i) {
        ::rpc::krpc::KrpcRequest message;
        if (!message.ParseFromString(wire)) {
            break;
        }
        rpc::rpc::KrpcRequestFrame decoded;
        decoded.service = message.service();
        decoded.method = message.method();
        decoded.payload = message.payload();
        decoded.timeout_ms = message.timeout_ms();
        const std::uint64_t max_retries = message.max_retries();
        if (max_retries > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
            break;
        }
        decoded.max_retries = static_cast<std::size_t>(max_retries);
        for (const auto& kv : message.metadata()) {
            if (!kv.first.empty()) {
                decoded.metadata[kv.first] = kv.second;
            }
        }
    }
    const auto legacy_decode_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - legacy_decode_started).count()
    );

    const auto optimized_decode_started = Clock::now();
    for (std::size_t i = 0; i < kIters; ++i) {
        rpc::rpc::KrpcRequestFrame decoded;
        if (!rpc::rpc::KrpcChannel::decode_request_protobuf(wire, &decoded)) {
            break;
        }
    }
    const auto optimized_decode_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - optimized_decode_started).count()
    );

    ProtobufCompareResult result;
    result.legacy_encode_ns_per_op = legacy_encode_ns / kIters;
    result.optimized_encode_ns_per_op = optimized_encode_ns / kIters;
    result.legacy_decode_ns_per_op = legacy_decode_ns / kIters;
    result.optimized_decode_ns_per_op = optimized_decode_ns / kIters;
    return result;
}

void run_memory_pool_stress() {
    rpc::infra::AdaptiveMemoryPool& pool = rpc::infra::AdaptiveMemoryPool::instance();
    std::vector<std::thread> workers;
    workers.reserve(8);
    for (std::size_t worker = 0; worker < 8; ++worker) {
        workers.emplace_back([&pool, worker]() {
            for (std::size_t i = 0; i < 2000; ++i) {
                const bool long_lived = ((i + worker) % 5) == 0;
                const rpc::infra::MemoryGeneration generation = long_lived
                    ? rpc::infra::MemoryGeneration::LongLived
                    : rpc::infra::MemoryGeneration::ShortLived;
                const std::size_t bytes = long_lived ? 1024 : 64;
                void* ptr = pool.allocate(bytes, generation);
                if (ptr == nullptr) {
                    continue;
                }
                std::memset(ptr, 0x7A, bytes);
                (void)pool.deallocate(ptr);
            }
        });
    }
    for (std::thread& worker : workers) {
        worker.join();
    }
    for (int i = 0; i < 4; ++i) {
        pool.maintenance_tick();
    }
}

}  // namespace

int main() {
    ScopedEchoBackend backend_a("A");
    ScopedEchoBackend backend_b("B");
    const bool backend_ready = backend_a.start() && backend_b.start();
    if (!backend_ready) {
        std::cout << "w15_fullchain_perf_test fallback_mode=inprocess reason=backend_bind_restricted\n";
    }

    rpc::client::ClientInitOptions client_options;
    client_options.discovery = std::make_shared<StaticDiscovery>(std::vector<rpc::client::ServiceNode>{
        rpc::client::ServiceNode{
            "perf-node-a",
            "127.0.0.1",
            backend_ready ? backend_a.port() : static_cast<std::uint16_t>(1),
            {{"lane", "stable"}, {"az", "a"}},
            0.38,
            0.35,
            180.0,
            26.0,
        },
        rpc::client::ServiceNode{
            "perf-node-b",
            "127.0.0.1",
            backend_ready ? backend_b.port() : static_cast<std::uint16_t>(2),
            {{"lane", "stable"}, {"az", "b"}},
            0.42,
            0.40,
            200.0,
            30.0,
        },
    });
    rpc::client::init_client(std::move(client_options));
    rpc::infra::configure_structured_log(rpc::infra::LogSamplingConfig{
        0.0,
        0.0,
        0.0,
        1.0,
        60'000,
    });
    rpc::infra::reset_structured_log_stats();

    constexpr std::size_t kConcurrency = 16;
    constexpr std::size_t kRequestsPerWorker = 100;

    std::mutex merge_mutex;
    std::vector<std::uint64_t> all_latencies_us;
    all_latencies_us.reserve(kConcurrency * kRequestsPerWorker);
    std::atomic<std::size_t> completed{0};
    std::atomic<std::size_t> failed{0};
    rpc::runtime::RuntimeSchedulerSnapshot runtime_snapshot;
    rpc::net::ReactorHttpServerStats reactor_stats;

    bool reactor_mode = false;
    std::unique_ptr<rpc::net::ReactorHttpServer> server;
    if (backend_ready) {
        rpc::net::ReactorHttpServerOptions options;
        options.bind_address = "127.0.0.1";
        options.port = 0;
        options.worker_threads = 4;
        options.ingress_queue_capacity = 1024;
        options.max_connections = 1024;
        options.max_connection_requests_inflight = 32;
        options.max_connection_write_buffer_bytes = 512 * 1024;
        options.gateway_upstream_timeout_ms = 300;

        server = std::make_unique<rpc::net::ReactorHttpServer>(options);
        reactor_mode = server->start() && (server->listen_port() != 0);
        if (!reactor_mode) {
            if (server) {
                server->stop();
            }
            std::cout << "w15_fullchain_perf_test fallback_mode=inprocess reason=reactor_bind_restricted\n";
        }
    }

    auto run_reactor_mode = [&](std::uint16_t port) -> std::chrono::steady_clock::duration {
        const auto load_started = Clock::now();
        std::vector<std::thread> workers;
        workers.reserve(kConcurrency);

        for (std::size_t worker = 0; worker < kConcurrency; ++worker) {
            workers.emplace_back([&, worker]() {
                const int fd = connect_to_server(port);
                if (fd < 0) {
                    failed.fetch_add(kRequestsPerWorker, std::memory_order_relaxed);
                    return;
                }

                (void)set_recv_timeout(fd, std::chrono::milliseconds(3000));
                HttpResponseReader reader(fd);
                std::vector<std::uint64_t> local_latencies;
                local_latencies.reserve(kRequestsPerWorker);

                for (std::size_t i = 0; i < kRequestsPerWorker; ++i) {
                    const std::size_t selector = (i + worker) % 10;
                    std::string target;
                    std::unordered_map<std::string, std::string> headers;
                    std::string body = "payload-w15-" + std::to_string(worker) + "-" + std::to_string(i);

                    if (selector <= 5) {
                        target = "/rpc/invoke";
                        headers.emplace("x-rpc-service", "perf.echo");
                        headers.emplace("x-rpc-method", "Echo");
                        headers.emplace("x-krpc-codec", "protobuf");
                        headers.emplace("x-rpc-timeout-ms", "220");
                        headers.emplace("x-upstream-timeout-ms", "220");
                        headers.emplace("x-rpc-max-retries", "1");
                    } else if (selector <= 7) {
                        target = "/short";
                    } else {
                        target = "/slow";
                    }

                    const std::string request = build_http_request("POST", target, headers, body);
                    const auto one_started = Clock::now();

                    if (!send_all(fd, request)) {
                        failed.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }

                    ParsedResponse response;
                    if (!reader.read_one(&response)) {
                        failed.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }

                    const auto latency_us = static_cast<std::uint64_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - one_started).count()
                    );
                    if (response.status == 200) {
                        completed.fetch_add(1, std::memory_order_relaxed);
                        local_latencies.push_back(latency_us);
                    } else {
                        failed.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                ::close(fd);

                std::lock_guard<std::mutex> lock(merge_mutex);
                all_latencies_us.insert(all_latencies_us.end(), local_latencies.begin(), local_latencies.end());
            });
        }

        for (std::thread& worker : workers) {
            worker.join();
        }
        return Clock::now() - load_started;
    };

    auto run_inprocess_mode = [&]() -> std::chrono::steady_clock::duration {
        const bool runtime_was_ready = rpc::runtime::runtime_ready();
        if (!runtime_was_ready) {
            rpc::runtime::start_runtime(rpc::runtime::RuntimeStartOptions{4, 256});
        }

        rpc::gateway::HttpGateway gateway(
            rpc::gateway::HttpGatewayOptions{std::chrono::milliseconds(300), {}},
            rpc::client::default_client()
        );

        const auto load_started = Clock::now();
        std::vector<std::thread> workers;
        workers.reserve(kConcurrency);
        for (std::size_t worker = 0; worker < kConcurrency; ++worker) {
            workers.emplace_back([&, worker]() {
                std::vector<std::uint64_t> local_latencies;
                local_latencies.reserve(kRequestsPerWorker);

                for (std::size_t i = 0; i < kRequestsPerWorker; ++i) {
                    const std::size_t selector = (i + worker) % 10;
                    rpc::gateway::GatewayHttpRequest request;
                    request.method = "POST";
                    request.body = "payload-w15-" + std::to_string(worker) + "-" + std::to_string(i);
                    if (selector <= 5) {
                        request.target = "/rpc/invoke";
                        request.headers["x-rpc-service"] = "perf.echo";
                        request.headers["x-rpc-method"] = "Echo";
                        request.headers["x-krpc-codec"] = "protobuf";
                        request.headers["x-rpc-timeout-ms"] = "220";
                        request.headers["x-upstream-timeout-ms"] = "220";
                        request.headers["x-rpc-max-retries"] = "1";
                        request.headers["x-fallback-static"] = "degraded-static-ok";
                    } else if (selector <= 7) {
                        request.target = "/short";
                    } else {
                        request.target = "/slow";
                    }

                    const auto one_started = Clock::now();
                    auto promise = std::make_shared<std::promise<rpc::gateway::GatewayHttpResponse>>();
                    std::future<rpc::gateway::GatewayHttpResponse> future = promise->get_future();
                    try {
                        (void)rpc::runtime::submit([&gateway, request, promise]() mutable {
                            promise->set_value(gateway.handle(request));
                        });
                    } catch (...) {
                        failed.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }

                    rpc::gateway::GatewayHttpResponse response;
                    try {
                        response = future.get();
                    } catch (...) {
                        failed.fetch_add(1, std::memory_order_relaxed);
                        break;
                    }

                    const auto latency_us = static_cast<std::uint64_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - one_started).count()
                    );
                    if (response.status == 200) {
                        completed.fetch_add(1, std::memory_order_relaxed);
                        local_latencies.push_back(latency_us);
                    } else {
                        failed.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                std::lock_guard<std::mutex> lock(merge_mutex);
                all_latencies_us.insert(all_latencies_us.end(), local_latencies.begin(), local_latencies.end());
            });
        }
        for (std::thread& worker : workers) {
            worker.join();
        }

        rpc::runtime::wait_runtime_idle();
        runtime_snapshot = rpc::runtime::runtime_scheduler_snapshot();
        if (!runtime_was_ready) {
            rpc::runtime::stop_runtime();
        }
        return Clock::now() - load_started;
    };

    std::chrono::steady_clock::duration load_elapsed{};
    if (reactor_mode) {
        const std::uint16_t port = server->listen_port();
        if (!expect_true(port != 0, "reactor listen port should not be zero")) {
            server->stop();
            return 1;
        }
        load_elapsed = run_reactor_mode(port);
        runtime_snapshot = rpc::runtime::runtime_scheduler_snapshot();
        reactor_stats = server->stats();
        server->stop();
    } else {
        load_elapsed = run_inprocess_mode();
    }

    std::sort(all_latencies_us.begin(), all_latencies_us.end());

    const std::size_t completed_requests = completed.load(std::memory_order_acquire);
    const std::size_t failed_requests = failed.load(std::memory_order_acquire);
    if (!expect_true(completed_requests > 0, "no request completed in fullchain perf test")) {
        if (server) {
            server->stop();
        }
        return 1;
    }

    const double elapsed_s = std::max(
        1e-6,
        std::chrono::duration_cast<std::chrono::duration<double>>(load_elapsed).count()
    );
    const double qps = static_cast<double>(completed_requests) / elapsed_s;
    const double p95_ms = static_cast<double>(percentile_us_from_sorted(all_latencies_us, 0.95)) / 1000.0;
    const double p99_ms = static_cast<double>(percentile_us_from_sorted(all_latencies_us, 0.99)) / 1000.0;

    run_memory_pool_stress();
    const rpc::infra::MemoryPoolStats memory_stats = rpc::infra::AdaptiveMemoryPool::instance().stats();
    const rpc::client::LoadBalancerRuntimeStats lb_stats = rpc::client::load_balancer_runtime_stats();
    const ProtobufCompareResult protobuf_compare = run_protobuf_compare();

    const bool target_qps_ok = qps >= 120.0;
    const bool target_p95_ok = p95_ms <= 120.0;
    const bool target_p99_ok = p99_ms <= 220.0;

    std::cout << std::fixed << std::setprecision(2)
              << "w15_fullchain_perf_test summary"
              << " qps=" << qps
              << " p95_ms=" << p95_ms
              << " p99_ms=" << p99_ms
              << " completed=" << completed_requests
              << " failed=" << failed_requests
              << " target_qps_ok=" << (target_qps_ok ? 1 : 0)
              << " target_p95_ok=" << (target_p95_ok ? 1 : 0)
              << " target_p99_ok=" << (target_p99_ok ? 1 : 0)
              << " mode=" << (reactor_mode ? "reactor" : "inprocess")
              << '\n';

    std::cout << "w15_scheduler_profile"
              << " enqueue_local=" << runtime_snapshot.enqueue_local
              << " enqueue_global=" << runtime_snapshot.enqueue_global
              << " enqueue_fast=" << runtime_snapshot.enqueue_fast
              << " dequeue_local_fast=" << runtime_snapshot.dequeue_local_fast
              << " dequeue_local=" << runtime_snapshot.dequeue_local
              << " dequeue_global=" << runtime_snapshot.dequeue_global
              << " dequeue_steal=" << runtime_snapshot.dequeue_steal
              << " state_lock_wait_ns_total=" << runtime_snapshot.scheduler_state_lock_wait_ns_total
              << " state_lock_wait_ns_avg=" << runtime_snapshot.scheduler_state_lock_wait_ns_avg
              << " idle_switches=" << runtime_snapshot.idle_switches
              << " steal_count=" << runtime_snapshot.steal_count
              << '\n';

    std::cout << "w15_lb_profile"
              << " select_calls=" << lb_stats.select_calls
              << " feedback_calls=" << lb_stats.feedback_calls
              << " blocked_by_circuit_breaker=" << lb_stats.blocked_by_circuit_breaker
              << " gray_routed=" << lb_stats.gray_routed
              << " select_lock_wait_ns_total=" << lb_stats.select_lock_wait_ns_total
              << " feedback_lock_wait_ns_total=" << lb_stats.feedback_lock_wait_ns_total
              << '\n';
    for (const auto& entry : lb_stats.selected_endpoint_counts) {
        std::cout << "w15_lb_endpoint endpoint=" << entry.first << " selected=" << entry.second << '\n';
    }

    std::cout << std::setprecision(4)
              << "w15_memory_profile"
              << " short_hit_ratio=" << memory_stats.short_lived.hit_ratio
              << " long_hit_ratio=" << memory_stats.long_lived.hit_ratio
              << " active_bytes_high_watermark=" << memory_stats.active_bytes_high_watermark
              << " fallback_allocations=" << memory_stats.fallback_allocations
              << " lock_wait_ns_total=" << memory_stats.lock_wait_ns_total
              << " lock_wait_ns_avg=" << memory_stats.lock_wait_ns_avg
              << '\n';

    std::cout << "w15_syscall_profile"
              << " epoll_mod_calls=" << reactor_stats.epoll_mod_calls
              << " epoll_mod_skipped=" << reactor_stats.epoll_mod_skipped
              << " send_syscalls=" << reactor_stats.send_syscalls
              << " recv_syscalls=" << reactor_stats.recv_syscalls
              << '\n';

    std::cout << "w15_protobuf_compare"
              << " legacy_encode_ns_per_op=" << protobuf_compare.legacy_encode_ns_per_op
              << " optimized_encode_ns_per_op=" << protobuf_compare.optimized_encode_ns_per_op
              << " legacy_decode_ns_per_op=" << protobuf_compare.legacy_decode_ns_per_op
              << " optimized_decode_ns_per_op=" << protobuf_compare.optimized_decode_ns_per_op
              << '\n';

    if (!expect_true(target_qps_ok, "qps target not reached")) {
        return 1;
    }
    if (!expect_true(target_p95_ok, "p95 target not reached")) {
        return 1;
    }
    if (!expect_true(target_p99_ok, "p99 target not reached")) {
        return 1;
    }

    return 0;
}
