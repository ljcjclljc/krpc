#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/system/error_code.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rpc/infra/infra.h"
#include "rpc/net/reactor_http_server.h"
#include "rpc/net/net.h"
#include "rpc/rpc/client.h"

namespace {

namespace asio = boost::asio;
namespace ssl = asio::ssl;
using asio::ip::tcp;
using namespace std::chrono_literals;

#ifndef RPC_SOURCE_DIR
#define RPC_SOURCE_DIR "."
#endif

bool expect_true(bool condition, const char* message) {
    if (condition) {
        return true;
    }
    std::cerr << message << '\n';
    return false;
}

std::string cert_asset_path(const char* filename) {
    return std::string(RPC_SOURCE_DIR) + "/tests/assets/w17_tls/" + filename;
}

bool send_all(int fd, const std::string& data) {
    std::size_t offset = 0;
    while (offset < data.size()) {
        const ssize_t n = ::send(
            fd,
            data.data() + static_cast<std::ptrdiff_t>(offset),
            data.size() - offset,
            MSG_NOSIGNAL
        );
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

int connect_loopback(std::uint16_t port) {
    for (int i = 0; i < 80; ++i) {
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

int parse_http_status(const std::string& response) {
    const std::size_t first_space = response.find(' ');
    if (first_space == std::string::npos) {
        return -1;
    }
    const std::size_t second_space = response.find(' ', first_space + 1);
    if (second_space == std::string::npos || second_space <= first_space + 1) {
        return -1;
    }

    try {
        return std::stoi(response.substr(first_space + 1, second_space - first_space - 1));
    } catch (...) {
        return -1;
    }
}

int post_once(std::uint16_t port, const std::string& target, const std::string& body) {
    const int fd = connect_loopback(port);
    if (fd < 0) {
        return -1;
    }

    const std::string request = "POST " + target + " HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Connection: close\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" + body;

    if (!send_all(fd, request)) {
        ::close(fd);
        return -1;
    }

    char buffer[4096];
    std::string response;
    while (true) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n > 0) {
            response.append(buffer, static_cast<std::size_t>(n));
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        ::close(fd);
        return -1;
    }

    ::close(fd);
    return parse_http_status(response);
}

class ToggleEchoBackend final {
public:
    explicit ToggleEchoBackend(std::string name) : name_(std::move(name)) {}

    ~ToggleEchoBackend() {
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
        running_.store(true, std::memory_order_release);
        worker_ = std::thread([this]() { run_loop(); });
        return true;
    }

    void stop() {
        running_.store(false, std::memory_order_release);
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
    void run_loop() {
        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in peer{};
            socklen_t peer_len = static_cast<socklen_t>(sizeof(peer));
            const int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
            if (client_fd < 0) {
                if (!running_.load(std::memory_order_acquire)) {
                    return;
                }
                if (errno == EINTR) {
                    continue;
                }
                return;
            }

            handle_client(client_fd);
            ::close(client_fd);
        }
    }

    void handle_client(int client_fd) {
        char buffer[4096];
        std::string payload;
        while (true) {
            const ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
            if (n > 0) {
                payload.append(buffer, static_cast<std::size_t>(n));
                if (static_cast<std::size_t>(n) < sizeof(buffer)) {
                    break;
                }
                continue;
            }
            if (n == 0) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            return;
        }

        const std::string response = "echo-" + name_ + ":" + payload;
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
            return;
        }
    }

    std::string name_;
    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::atomic<bool> running_{false};
    std::thread worker_;
};

class MtlsEchoServer final {
public:
    bool start(const std::string& ca_file, const std::string& cert_file, const std::string& key_file) {
        boost::system::error_code ec;

        context_.set_options(
            ssl::context::default_workarounds
            | ssl::context::no_sslv2
            | ssl::context::no_sslv3
            | ssl::context::single_dh_use
        );
        context_.use_certificate_chain_file(cert_file);
        context_.use_private_key_file(key_file, ssl::context::pem);
        context_.load_verify_file(ca_file);

        acceptor_ = std::make_unique<tcp::acceptor>(io_context_);
        acceptor_->open(tcp::v4(), ec);
        if (ec) {
            return false;
        }
        acceptor_->set_option(asio::socket_base::reuse_address(true), ec);
        if (ec) {
            return false;
        }
        acceptor_->bind(tcp::endpoint(asio::ip::address_v4::loopback(), 0), ec);
        if (ec) {
            return false;
        }
        acceptor_->listen(asio::socket_base::max_listen_connections, ec);
        if (ec) {
            return false;
        }

        port_ = acceptor_->local_endpoint(ec).port();
        if (ec || port_ == 0) {
            return false;
        }

        running_.store(true, std::memory_order_release);
        worker_ = std::thread([this]() { run_loop(); });
        return true;
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        boost::system::error_code ec;
        if (acceptor_) {
            acceptor_->close(ec);
        }
        io_context_.stop();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    ~MtlsEchoServer() {
        stop();
    }

    std::uint16_t port() const noexcept {
        return port_;
    }

private:
    void run_loop() {
        while (running_.load(std::memory_order_acquire)) {
            tcp::socket socket(io_context_);
            boost::system::error_code ec;
            acceptor_->accept(socket, ec);
            if (ec) {
                if (!running_.load(std::memory_order_acquire)) {
                    return;
                }
                continue;
            }

            ssl::stream<tcp::socket> stream(std::move(socket), context_);
            stream.set_verify_mode(ssl::verify_peer | ssl::verify_fail_if_no_peer_cert);
            stream.handshake(ssl::stream_base::server, ec);
            if (ec) {
                continue;
            }

            std::string payload;
            char buffer[4096];
            while (true) {
                const std::size_t n = stream.read_some(asio::buffer(buffer, sizeof(buffer)), ec);
                if (!ec && n > 0) {
                    payload.append(buffer, n);
                    if (n < sizeof(buffer)) {
                        break;
                    }
                    continue;
                }
                if (ec == asio::error::eof || ec == ssl::error::stream_truncated) {
                    break;
                }
                if (!ec && n == 0) {
                    break;
                }
                break;
            }

            if (!payload.empty()) {
                const std::string response = "mtls:" + payload;
                asio::write(stream, asio::buffer(response), ec);
            }

            boost::system::error_code shutdown_ec;
            stream.shutdown(shutdown_ec);
        }
    }

    asio::io_context io_context_;
    ssl::context context_{ssl::context::tls_server};
    std::unique_ptr<tcp::acceptor> acceptor_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::uint16_t port_{0};
};

class W17Discovery final : public rpc::client::IServiceDiscovery {
public:
    W17Discovery(std::uint16_t stable_port, std::uint16_t gray_port, std::uint16_t mtls_port)
        : stable_port_(stable_port),
          gray_port_(gray_port),
          mtls_port_(mtls_port) {}

    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override {
        if (service_name == "svc.w17.gray") {
            return {
                rpc::client::ServiceNode{
                    "stable-a",
                    "127.0.0.1",
                    stable_port_,
                    {{"lane", "stable"}},
                    0.20,
                    0.20,
                    80.0,
                    12.0,
                },
                rpc::client::ServiceNode{
                    "gray-b",
                    "127.0.0.1",
                    gray_port_,
                    {{"lane", "gray"}},
                    0.60,
                    0.55,
                    240.0,
                    90.0,
                },
            };
        }

        if (service_name == "svc.w17.mtls") {
            return {
                rpc::client::ServiceNode{
                    "mtls-only",
                    "127.0.0.1",
                    mtls_port_,
                    {{"lane", "stable"}},
                    0.30,
                    0.30,
                    70.0,
                    20.0,
                },
            };
        }

        return {};
    }

private:
    std::uint16_t stable_port_{0};
    std::uint16_t gray_port_{0};
    std::uint16_t mtls_port_{0};
};

rpc::client::RpcRequest make_request(std::string service, std::string payload) {
    rpc::client::RpcRequest request;
    request.service = std::move(service);
    request.method = "Echo";
    request.payload = std::move(payload);
    request.timeout_ms = 400;
    request.max_retries = 0;
    request.metadata["x-fallback-cache"] = "0";
    request.metadata["x-user-id"] = "user-w17";
    return request;
}

}  // namespace

int main() {
    int probe_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (probe_fd < 0 && (errno == EPERM || errno == EACCES)) {
        std::cout << "w17_hot_reload_tls_rollback_test skipped: socket operations are not permitted\n";
        return 0;
    }
    if (probe_fd >= 0) {
        ::close(probe_fd);
    }

    rpc::infra::init_infra();

    // 1) 热更新限流：不重启服务即可生效。
    {
        std::uint64_t version_after_limit_low = 0;
        if (!expect_true(
                rpc::infra::publish_config_patch(
                    {
                        {"gateway.rate_limit.qps", "2"},
                        {"gateway.rate_limit.burst", "0"},
                    },
                    &version_after_limit_low
                ),
                "publish low rate limit config failed"
            )) {
            return 1;
        }

        rpc::net::ReactorHttpServerOptions options;
        options.bind_address = "127.0.0.1";
        options.port = 0;
        options.worker_threads = 1;
        options.max_connections = 128;
        options.ingress_queue_capacity = 64;
        options.max_connection_requests_inflight = 16;
        options.max_connection_write_buffer_bytes = 32 * 1024;
        options.rate_limit_qps = 0;
        options.rate_limit_burst = 0;

        rpc::net::ReactorHttpServer server(options);
        if (!expect_true(server.start(), "failed to start reactor server")) {
            return 1;
        }

        const std::uint16_t port = server.listen_port();
        if (!expect_true(port != 0, "invalid reactor listen port")) {
            server.stop();
            return 1;
        }

        int low_ok = 0;
        int low_429 = 0;
        for (int i = 0; i < 24; ++i) {
            const int status = post_once(port, "/rpc/invoke", "w17-low-" + std::to_string(i));
            if (status == 200) {
                ++low_ok;
            } else if (status == 429) {
                ++low_429;
            }
        }

        if (!expect_true(low_429 > 0, "low-qps stage did not trigger rate_limit 429")) {
            server.stop();
            return 1;
        }

        std::uint64_t version_after_limit_high = 0;
        if (!expect_true(
                rpc::infra::publish_config_patch(
                    {
                        {"gateway.rate_limit.qps", "300"},
                        {"gateway.rate_limit.burst", "50"},
                    },
                    &version_after_limit_high
                ),
                "publish high rate limit config failed"
            )) {
            server.stop();
            return 1;
        }

        std::this_thread::sleep_for(120ms);

        int high_ok = 0;
        int high_429 = 0;
        for (int i = 0; i < 24; ++i) {
            const int status = post_once(port, "/rpc/invoke", "w17-high-" + std::to_string(i));
            if (status == 200) {
                ++high_ok;
            } else if (status == 429) {
                ++high_429;
            }
        }

        if (!expect_true(high_ok > low_ok, "rate limit hot update did not improve pass-through")) {
            server.stop();
            return 1;
        }
        if (!expect_true(server.running(), "server unexpectedly stopped after config hot update")) {
            server.stop();
            return 1;
        }
        if (!expect_true(server.listen_port() == port, "server restarted unexpectedly during hot update")) {
            server.stop();
            return 1;
        }

        server.stop();
    }

    // 2) 灰度发布 + 回滚演练（分钟级内完成）。
    ToggleEchoBackend stable_backend("stable");
    ToggleEchoBackend gray_backend("gray");
    if (!expect_true(stable_backend.start() && gray_backend.start(), "failed to start gray test backends")) {
        return 1;
    }

    MtlsEchoServer mtls_server;
    const std::string ca_file = cert_asset_path("ca.crt");
    const std::string server_cert_file = cert_asset_path("server.crt");
    const std::string server_key_file = cert_asset_path("server.key");
    if (!expect_true(
            mtls_server.start(ca_file, server_cert_file, server_key_file),
            "failed to start mtls echo server"
        )) {
        return 1;
    }

    rpc::client::ClientInitOptions init_options;
    init_options.discovery = std::make_shared<W17Discovery>(
        stable_backend.port(),
        gray_backend.port(),
        mtls_server.port()
    );
    rpc::client::init_client(std::move(init_options));

    const auto client = rpc::client::default_client();
    if (!expect_true(client != nullptr, "default rpc client is null")) {
        return 1;
    }

    const rpc::client::RpcResponse baseline_gray = client->invoke(make_request("svc.w17.gray", "baseline"));
    if (!expect_true(baseline_gray.code == 0, "baseline gray request failed")) {
        return 1;
    }
    if (!expect_true(baseline_gray.selected_endpoint.find(std::to_string(stable_backend.port())) != std::string::npos,
            "baseline gray route should prefer stable endpoint")) {
        return 1;
    }

    const std::uint64_t rollback_anchor_version = rpc::infra::current_config_version();

    if (!expect_true(
            rpc::infra::publish_config_patch({{"rpc.gray.percent", "100"}}),
            "publish gray config failed"
        )) {
        return 1;
    }

    const rpc::client::RpcResponse gray_release = client->invoke(make_request("svc.w17.gray", "gray-release"));
    if (!expect_true(gray_release.code == 0, "gray release request failed")) {
        return 1;
    }
    if (!expect_true(gray_release.traffic_lane == "gray", "gray release did not route to gray lane")) {
        return 1;
    }

    const auto rollback_started = std::chrono::steady_clock::now();
    if (!expect_true(
            rpc::infra::rollback_config_to(rollback_anchor_version),
            "rollback to anchor version failed"
        )) {
        return 1;
    }
    const auto rollback_cost = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - rollback_started
    );

    if (!expect_true(rollback_cost < std::chrono::minutes(2), "rollback drill exceeded minute-level target")) {
        return 1;
    }

    const rpc::client::RpcResponse after_rollback = client->invoke(make_request("svc.w17.gray", "after-rollback"));
    if (!expect_true(after_rollback.code == 0, "post rollback gray request failed")) {
        return 1;
    }
    if (!expect_true(after_rollback.selected_endpoint.find(std::to_string(stable_backend.port())) != std::string::npos,
            "post rollback route did not return to stable endpoint")) {
        return 1;
    }

    // 3) TLS/mTLS 接入 + 客户端证书轮转。
    const std::string client_a_cert = cert_asset_path("client_a.crt");
    const std::string client_a_key = cert_asset_path("client_a.key");
    const std::string client_b_cert = cert_asset_path("client_b.crt");
    const std::string client_b_key = cert_asset_path("client_b.key");

    if (!expect_true(
            rpc::infra::publish_config_patch(
                {
                    {"net.tls.enabled", "1"},
                    {"net.tls.mtls.enabled", "1"},
                    {"net.tls.insecure_skip_verify", "0"},
                    {"net.tls.ca_file", ca_file},
                    {"net.tls.cert_file", client_a_cert},
                    {"net.tls.key_file", client_a_key},
                    {"net.tls.server_name", "localhost"},
                }
            ),
            "publish initial mtls config failed"
        )) {
        return 1;
    }

    const rpc::client::RpcResponse mtls_a = client->invoke(make_request("svc.w17.mtls", "cert-a"));
    if (!expect_true(mtls_a.code == 0, "mtls request with client cert A failed")) {
        return 1;
    }

    const rpc::net::NetTlsRuntimeSnapshot tls_before_rotate = rpc::net::tls_runtime_snapshot();
    if (!expect_true(tls_before_rotate.tls_enabled && tls_before_rotate.mtls_enabled,
            "tls runtime snapshot does not reflect mtls enabled")) {
        return 1;
    }

    if (!expect_true(
            rpc::infra::publish_config_patch(
                {
                    {"net.tls.cert_file", client_b_cert},
                    {"net.tls.key_file", client_b_key},
                }
            ),
            "publish mtls cert rotation config failed"
        )) {
        return 1;
    }

    const rpc::client::RpcResponse mtls_b = client->invoke(make_request("svc.w17.mtls", "cert-b"));
    if (!expect_true(mtls_b.code == 0, "mtls request with rotated client cert B failed")) {
        return 1;
    }

    const rpc::net::NetTlsRuntimeSnapshot tls_after_rotate = rpc::net::tls_runtime_snapshot();
    if (!expect_true(
            tls_after_rotate.context_reload_count > tls_before_rotate.context_reload_count,
            "tls context reload count did not increase after cert rotation"
        )) {
        return 1;
    }

    std::cout << "w17_hot_reload_tls_rollback_test passed"
              << ", rollback_seconds=" << rollback_cost.count()
              << ", low_stage_429_observed=1"
              << ", tls_reload_count=" << tls_after_rotate.context_reload_count
              << '\n';

    return 0;
}
