#include "rpc/net/net.h"

// 文件用途：
// 提供 W7/W17 阶段 net 能力：
// 1) 计算上游 deadline 与下游超时配置的合并预算
// 2) 提供可控失败的调用桩，用于重试预算与超时分层验证
// 3) 支持 TLS/mTLS 调用与证书上下文热轮转

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/system/error_code.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <openssl/ssl.h>

#include "rpc/infra/infra.h"
#include "rpc/runtime/request_context.h"

namespace rpc::net {

namespace {

namespace asio = boost::asio;
namespace ssl = asio::ssl;

std::atomic<std::uint64_t> g_last_effective_timeout_ms{0};
std::atomic<bool> g_tls_enabled{false};
std::atomic<bool> g_mtls_enabled{false};
std::atomic<std::uint64_t> g_tls_loaded_config_version{0};
std::atomic<std::size_t> g_tls_context_reload_count{0};
std::atomic<std::size_t> g_tls_context_reload_failures{0};

struct TlsSettings {
    bool enabled{false};
    bool mtls_enabled{false};
    bool insecure_skip_verify{false};
    std::string ca_file;
    std::string cert_file;
    std::string key_file;
    std::string server_name;
    std::uint64_t config_version{0};
};

struct TlsContextCache {
    std::shared_ptr<ssl::context> context;
    std::uint64_t config_version{0};
    std::string ca_file;
    std::string cert_file;
    std::string key_file;
    std::optional<std::filesystem::file_time_type> ca_mtime;
    std::optional<std::filesystem::file_time_type> cert_mtime;
    std::optional<std::filesystem::file_time_type> key_mtime;
};

std::mutex g_tls_context_mutex;
TlsContextCache g_tls_context_cache;

std::string trim_copy(std::string_view value) {
    std::size_t begin = 0;
    while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
        ++begin;
    }
    std::size_t end = value.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
        --end;
    }
    return std::string(value.substr(begin, end - begin));
}

std::string lower_ascii_copy(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

bool parse_bool_value(std::string value, bool fallback) {
    if (value.empty()) {
        return fallback;
    }
    const std::string lowered = lower_ascii_copy(trim_copy(value));
    if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on") {
        return true;
    }
    if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "off") {
        return false;
    }
    return fallback;
}

bool parse_ipv4_endpoint(std::string_view endpoint, std::string* host, std::uint16_t* port) {
    if (host == nullptr || port == nullptr) {
        return false;
    }

    const std::size_t colon_pos = endpoint.rfind(':');
    if (colon_pos == std::string_view::npos || colon_pos == 0 || colon_pos + 1 >= endpoint.size()) {
        return false;
    }

    const std::string host_part = trim_copy(endpoint.substr(0, colon_pos));
    const std::string port_part = trim_copy(endpoint.substr(colon_pos + 1));
    if (host_part.empty() || port_part.empty()) {
        return false;
    }

    unsigned long parsed_port = 0;
    try {
        parsed_port = std::stoul(port_part);
    } catch (...) {
        return false;
    }

    if (parsed_port == 0 || parsed_port > static_cast<unsigned long>(std::numeric_limits<std::uint16_t>::max())) {
        return false;
    }

    *host = host_part;
    *port = static_cast<std::uint16_t>(parsed_port);
    return true;
}

bool apply_socket_timeouts(int fd, std::uint64_t timeout_ms) {
    if (fd < 0 || timeout_ms == 0) {
        return true;
    }

    timeval tv{};
    tv.tv_sec = static_cast<long>(timeout_ms / 1000);
    tv.tv_usec = static_cast<long>((timeout_ms % 1000) * 1000);
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, static_cast<socklen_t>(sizeof(tv))) != 0) {
        return false;
    }
    if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, static_cast<socklen_t>(sizeof(tv))) != 0) {
        return false;
    }
    return true;
}

NetCallResponse make_failure(
    int code,
    std::string message,
    bool retryable,
    std::uint64_t effective_timeout_ms,
    bool cancelled = false,
    std::string cancel_reason = {}
) {
    return NetCallResponse{
        code,
        std::move(message),
        {},
        retryable,
        effective_timeout_ms,
        cancelled,
        std::move(cancel_reason),
    };
}

std::optional<std::filesystem::file_time_type> file_mtime_if_exists(const std::string& path) {
    if (path.empty()) {
        return std::nullopt;
    }

    std::error_code ec;
    if (!std::filesystem::exists(path, ec) || ec) {
        return std::nullopt;
    }

    const auto mtime = std::filesystem::last_write_time(path, ec);
    if (ec) {
        return std::nullopt;
    }
    return mtime;
}

TlsSettings load_tls_settings() {
    TlsSettings settings;

    try {
        const rpc::infra::ConfigSnapshot snapshot = rpc::infra::config_repository().snapshot();
        settings.config_version = snapshot.version;

        const auto read_value = [&snapshot](const char* key, const char* fallback) {
            const auto it = snapshot.values.find(key);
            if (it == snapshot.values.end()) {
                return std::string(fallback == nullptr ? "" : fallback);
            }
            return it->second;
        };

        settings.enabled = parse_bool_value(read_value("net.tls.enabled", "0"), false);
        settings.mtls_enabled = parse_bool_value(read_value("net.tls.mtls.enabled", "0"), false);
        settings.insecure_skip_verify = parse_bool_value(
            read_value("net.tls.insecure_skip_verify", "0"),
            false
        );
        settings.ca_file = read_value("net.tls.ca_file", "");
        settings.cert_file = read_value("net.tls.cert_file", "");
        settings.key_file = read_value("net.tls.key_file", "");
        settings.server_name = read_value("net.tls.server_name", "");
    } catch (...) {
        settings = TlsSettings{};
    }

    g_tls_enabled.store(settings.enabled, std::memory_order_release);
    g_mtls_enabled.store(settings.mtls_enabled, std::memory_order_release);
    return settings;
}

std::shared_ptr<ssl::context> build_tls_context(const TlsSettings& settings, std::string* error) {
    try {
        auto context = std::make_shared<ssl::context>(ssl::context::tls_client);
        context->set_options(
            ssl::context::default_workarounds
            | ssl::context::no_sslv2
            | ssl::context::no_sslv3
        );

        if (settings.insecure_skip_verify) {
            context->set_verify_mode(ssl::verify_none);
        } else {
            context->set_verify_mode(ssl::verify_peer);
            if (!settings.ca_file.empty()) {
                context->load_verify_file(settings.ca_file);
            } else {
                context->set_default_verify_paths();
            }
        }

        if (settings.mtls_enabled) {
            if (settings.cert_file.empty() || settings.key_file.empty()) {
                if (error != nullptr) {
                    *error = "mtls_client_cert_or_key_missing";
                }
                return nullptr;
            }
            context->use_certificate_chain_file(settings.cert_file);
            context->use_private_key_file(settings.key_file, ssl::context::pem);
        }

        return context;
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return nullptr;
    } catch (...) {
        if (error != nullptr) {
            *error = "tls_context_build_unknown_error";
        }
        return nullptr;
    }
}

std::shared_ptr<ssl::context> acquire_tls_context(const TlsSettings& settings, std::string* error) {
    const std::optional<std::filesystem::file_time_type> ca_mtime = file_mtime_if_exists(settings.ca_file);
    const std::optional<std::filesystem::file_time_type> cert_mtime = file_mtime_if_exists(settings.cert_file);
    const std::optional<std::filesystem::file_time_type> key_mtime = file_mtime_if_exists(settings.key_file);

    std::lock_guard<std::mutex> lock(g_tls_context_mutex);

    const bool needs_reload = !g_tls_context_cache.context
        || g_tls_context_cache.config_version != settings.config_version
        || g_tls_context_cache.ca_file != settings.ca_file
        || g_tls_context_cache.cert_file != settings.cert_file
        || g_tls_context_cache.key_file != settings.key_file
        || g_tls_context_cache.ca_mtime != ca_mtime
        || g_tls_context_cache.cert_mtime != cert_mtime
        || g_tls_context_cache.key_mtime != key_mtime;

    if (!needs_reload) {
        return g_tls_context_cache.context;
    }

    std::string build_error;
    std::shared_ptr<ssl::context> rebuilt = build_tls_context(settings, &build_error);
    if (!rebuilt) {
        g_tls_context_reload_failures.fetch_add(1, std::memory_order_relaxed);
        if (error != nullptr) {
            *error = build_error;
        }

        // 证书轮转失败时保留上一份可用上下文，避免瞬时配置错误放大故障。
        if (g_tls_context_cache.context) {
            return g_tls_context_cache.context;
        }
        return nullptr;
    }

    g_tls_context_cache.context = rebuilt;
    g_tls_context_cache.config_version = settings.config_version;
    g_tls_context_cache.ca_file = settings.ca_file;
    g_tls_context_cache.cert_file = settings.cert_file;
    g_tls_context_cache.key_file = settings.key_file;
    g_tls_context_cache.ca_mtime = ca_mtime;
    g_tls_context_cache.cert_mtime = cert_mtime;
    g_tls_context_cache.key_mtime = key_mtime;

    g_tls_loaded_config_version.store(settings.config_version, std::memory_order_release);
    g_tls_context_reload_count.fetch_add(1, std::memory_order_relaxed);
    return g_tls_context_cache.context;
}

NetCallResponse invoke_tls(
    const NetCallRequest& request,
    const std::string& host,
    std::uint16_t port,
    std::uint64_t effective_timeout_ms,
    const TlsSettings& settings
) {
    std::string context_error;
    const std::shared_ptr<ssl::context> tls_context = acquire_tls_context(settings, &context_error);
    if (!tls_context) {
        return make_failure(
            503,
            "tls_context_unavailable:" + (context_error.empty() ? std::string("unknown") : context_error),
            true,
            effective_timeout_ms
        );
    }

    asio::io_context io_context;
    ssl::stream<asio::ip::tcp::socket> stream(io_context, *tls_context);

    boost::system::error_code ec;
    auto& socket = stream.next_layer();
    socket.open(asio::ip::tcp::v4(), ec);
    if (ec) {
        return make_failure(503, "tls_socket_open_failed", true, effective_timeout_ms);
    }

    if (!apply_socket_timeouts(socket.native_handle(), effective_timeout_ms)) {
        return make_failure(503, "socket_timeout_config_failed", true, effective_timeout_ms);
    }

    asio::ip::address_v4 ipv4;
    try {
        ipv4 = asio::ip::make_address_v4(host);
    } catch (...) {
        return make_failure(400, "invalid_endpoint_host", false, effective_timeout_ms);
    }

    socket.connect(asio::ip::tcp::endpoint(ipv4, port), ec);
    if (ec) {
        if (ec == asio::error::timed_out) {
            return make_failure(504, "connect_timeout", true, effective_timeout_ms);
        }
        return make_failure(503, "connect_failed:" + std::to_string(ec.value()), true, effective_timeout_ms);
    }

    const std::string server_name = settings.server_name.empty() ? host : settings.server_name;
    if (!server_name.empty()) {
        if (::SSL_set_tlsext_host_name(stream.native_handle(), server_name.c_str()) != 1) {
            return make_failure(503, "tls_sni_set_failed", true, effective_timeout_ms);
        }
    }

    if (!settings.insecure_skip_verify && !server_name.empty()) {
        stream.set_verify_callback(ssl::host_name_verification(server_name));
    }

    stream.handshake(ssl::stream_base::client, ec);
    if (ec) {
        return make_failure(503, "tls_handshake_failed:" + std::to_string(ec.value()), true, effective_timeout_ms);
    }

    asio::write(stream, asio::buffer(request.payload), ec);
    if (ec) {
        if (ec == asio::error::timed_out || ec == asio::error::would_block) {
            return make_failure(504, "send_timeout", true, effective_timeout_ms);
        }
        return make_failure(503, "send_failed:" + std::to_string(ec.value()), true, effective_timeout_ms);
    }

    std::string response_payload;
    char buffer[4096];
    while (true) {
        const std::size_t n = stream.read_some(asio::buffer(buffer, sizeof(buffer)), ec);
        if (!ec && n > 0) {
            response_payload.append(buffer, n);
            if (n < sizeof(buffer)) {
                break;
            }
            continue;
        }

        if (ec == asio::error::eof
            || ec == ssl::error::stream_truncated
            || (!ec && n == 0)) {
            break;
        }

        if (ec == asio::error::timed_out || ec == asio::error::would_block) {
            return make_failure(504, "recv_timeout", true, effective_timeout_ms);
        }

        return make_failure(503, "recv_failed:" + std::to_string(ec.value()), true, effective_timeout_ms);
    }

    boost::system::error_code shutdown_ec;
    stream.shutdown(shutdown_ec);

    if (response_payload.empty()) {
        return make_failure(502, "upstream_closed_without_payload", false, effective_timeout_ms);
    }

    return NetCallResponse{
        0,
        "ok",
        std::move(response_payload),
        false,
        effective_timeout_ms,
        false,
        {},
    };
}

}  // namespace

void init_network() {
    // 当前阶段无实际初始化逻辑，保留空实现用于打通启动链路。
}

// 结合上游 deadline 与下游 timeout 配置，计算 net 层有效超时。
EffectiveTimeout derive_effective_timeout(std::uint64_t downstream_timeout_ms) {
    EffectiveTimeout result;
    result.effective_timeout_ms = downstream_timeout_ms;

    const auto context = rpc::runtime::current_request_context();
    if (!context) {
        return result;
    }

    if (context->cancelled()) {
        const std::string reason = context->cancel_reason();
        if (reason == "deadline_exceeded") {
            result.deadline_exceeded = true;
            result.effective_timeout_ms = 0;
            return result;
        }
        result.cancelled = true;
        result.cancel_reason = reason.empty() ? "cancelled" : reason;
        result.effective_timeout_ms = 0;
        return result;
    }

    if (context->deadline() == rpc::runtime::RequestContext::TimePoint::max()) {
        return result;
    }

    const auto now = rpc::runtime::RequestContext::Clock::now();
    if (context->deadline() <= now) {
        context->cancel("deadline_exceeded");
        result.deadline_exceeded = true;
        result.effective_timeout_ms = 0;
        return result;
    }

    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(context->deadline() - now);
    const std::uint64_t remaining_ms = static_cast<std::uint64_t>(remaining.count());

    if (result.effective_timeout_ms == 0) {
        result.effective_timeout_ms = remaining_ms;
    } else {
        result.effective_timeout_ms = std::min(result.effective_timeout_ms, remaining_ms);
    }

    return result;
}

NetCallResponse invoke_tcp(const NetCallRequest& request) {
    const EffectiveTimeout timeout = derive_effective_timeout(request.downstream_timeout_ms);
    g_last_effective_timeout_ms.store(timeout.effective_timeout_ms, std::memory_order_release);

    if (timeout.cancelled) {
        const std::string reason = timeout.cancel_reason.empty() ? "cancelled" : timeout.cancel_reason;
        return make_failure(499, reason, false, timeout.effective_timeout_ms, true, reason);
    }

    if (timeout.deadline_exceeded) {
        return make_failure(504, "deadline_exceeded", false, timeout.effective_timeout_ms);
    }

    std::string host;
    std::uint16_t port = 0;
    if (!parse_ipv4_endpoint(request.endpoint, &host, &port)) {
        return make_failure(400, "invalid_endpoint", false, timeout.effective_timeout_ms);
    }

    const TlsSettings tls_settings = load_tls_settings();
    if (tls_settings.enabled) {
        return invoke_tls(request, host, port, timeout.effective_timeout_ms, tls_settings);
    }

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return make_failure(503, "socket_create_failed", true, timeout.effective_timeout_ms);
    }

    auto close_fd = [&]() {
        if (fd >= 0) {
            ::close(fd);
        }
    };

    if (!apply_socket_timeouts(fd, timeout.effective_timeout_ms)) {
        close_fd();
        return make_failure(503, "socket_timeout_config_failed", true, timeout.effective_timeout_ms);
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        close_fd();
        return make_failure(400, "invalid_endpoint_host", false, timeout.effective_timeout_ms);
    }

    if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
        const int connect_errno = errno;
        close_fd();
        if (connect_errno == ETIMEDOUT) {
            return make_failure(504, "connect_timeout", true, timeout.effective_timeout_ms);
        }
        return make_failure(503, "connect_failed:" + std::to_string(connect_errno), true, timeout.effective_timeout_ms);
    }

    std::size_t sent = 0;
    while (sent < request.payload.size()) {
        const ssize_t n = ::send(
            fd,
            request.payload.data() + static_cast<std::ptrdiff_t>(sent),
            request.payload.size() - sent,
            0
        );
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }

        const int send_errno = errno;
        close_fd();
        if (send_errno == EAGAIN || send_errno == EWOULDBLOCK || send_errno == ETIMEDOUT) {
            return make_failure(504, "send_timeout", true, timeout.effective_timeout_ms);
        }
        return make_failure(503, "send_failed:" + std::to_string(send_errno), true, timeout.effective_timeout_ms);
    }

    char buffer[4096];
    std::string response_payload;
    while (true) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n > 0) {
            response_payload.append(buffer, static_cast<std::size_t>(n));
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

        const int recv_errno = errno;
        close_fd();
        if (recv_errno == EAGAIN || recv_errno == EWOULDBLOCK || recv_errno == ETIMEDOUT) {
            return make_failure(504, "recv_timeout", true, timeout.effective_timeout_ms);
        }
        return make_failure(503, "recv_failed:" + std::to_string(recv_errno), true, timeout.effective_timeout_ms);
    }

    close_fd();
    if (response_payload.empty()) {
        return make_failure(502, "upstream_closed_without_payload", false, timeout.effective_timeout_ms);
    }

    return NetCallResponse{
        0,
        "ok",
        std::move(response_payload),
        false,
        timeout.effective_timeout_ms,
        false,
        {},
    };
}

std::uint64_t last_effective_timeout_ms() noexcept {
    return g_last_effective_timeout_ms.load(std::memory_order_acquire);
}

NetTlsRuntimeSnapshot tls_runtime_snapshot() noexcept {
    NetTlsRuntimeSnapshot snapshot;
    snapshot.tls_enabled = g_tls_enabled.load(std::memory_order_acquire);
    snapshot.mtls_enabled = g_mtls_enabled.load(std::memory_order_acquire);
    snapshot.loaded_config_version = g_tls_loaded_config_version.load(std::memory_order_acquire);
    snapshot.context_reload_count = g_tls_context_reload_count.load(std::memory_order_acquire);
    snapshot.context_reload_failures = g_tls_context_reload_failures.load(std::memory_order_acquire);
    return snapshot;
}

}  // namespace rpc::net
