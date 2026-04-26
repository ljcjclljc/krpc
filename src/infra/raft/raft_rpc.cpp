#include "rpc/infra/raft/raft_rpc.h"

// 文件用途：
// Raft 节点间 RPC 实现（TCP + Protobuf）：
// - 请求封包/解包
// - RequestVote / AppendEntries / InstallSnapshot 调用
// - 轻量服务端监听与分发

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>

#include "raft.pb.h"

namespace rpc::infra::raft {

namespace {

constexpr std::size_t kMaxFrameBytes = 16U * 1024U * 1024U;

rpc::raft::CommandType to_pb_command_type(PersistCommandType type) {
    switch (type) {
        case PersistCommandType::UpsertConfig:
            return rpc::raft::COMMAND_TYPE_UPSERT_CONFIG;
        case PersistCommandType::EraseConfig:
            return rpc::raft::COMMAND_TYPE_ERASE_CONFIG;
        case PersistCommandType::RegisterService:
            return rpc::raft::COMMAND_TYPE_REGISTER_SERVICE;
        case PersistCommandType::UnregisterService:
            return rpc::raft::COMMAND_TYPE_UNREGISTER_SERVICE;
        case PersistCommandType::BeginJointConsensus:
            return rpc::raft::COMMAND_TYPE_BEGIN_JOINT_CONSENSUS;
        case PersistCommandType::FinalizeJointConsensus:
            return rpc::raft::COMMAND_TYPE_FINALIZE_JOINT_CONSENSUS;
    }
    return rpc::raft::COMMAND_TYPE_UNSPECIFIED;
}

PersistCommandType from_pb_command_type(rpc::raft::CommandType type) {
    switch (type) {
        case rpc::raft::COMMAND_TYPE_UPSERT_CONFIG:
            return PersistCommandType::UpsertConfig;
        case rpc::raft::COMMAND_TYPE_ERASE_CONFIG:
            return PersistCommandType::EraseConfig;
        case rpc::raft::COMMAND_TYPE_REGISTER_SERVICE:
            return PersistCommandType::RegisterService;
        case rpc::raft::COMMAND_TYPE_UNREGISTER_SERVICE:
            return PersistCommandType::UnregisterService;
        case rpc::raft::COMMAND_TYPE_BEGIN_JOINT_CONSENSUS:
            return PersistCommandType::BeginJointConsensus;
        case rpc::raft::COMMAND_TYPE_FINALIZE_JOINT_CONSENSUS:
            return PersistCommandType::FinalizeJointConsensus;
        case rpc::raft::COMMAND_TYPE_UNSPECIFIED:
        default:
            return PersistCommandType::UpsertConfig;
    }
}

void to_pb_service_instance(const PersistServiceInstance& source, rpc::raft::ServiceInstance* target) {
    if (target == nullptr) {
        return;
    }
    target->set_id(source.id);
    target->set_host(source.host);
    target->set_port(source.port);
}

PersistServiceInstance from_pb_service_instance(const rpc::raft::ServiceInstance& source) {
    PersistServiceInstance result;
    result.id = source.id();
    result.host = source.host();
    result.port = static_cast<std::uint16_t>(source.port());
    return result;
}

void to_pb_command(const PersistCommand& source, rpc::raft::Command* target) {
    if (target == nullptr) {
        return;
    }

    target->set_type(to_pb_command_type(source.type));
    target->set_key(source.key);
    target->set_value(source.value);
    target->set_service(source.service);
    to_pb_service_instance(source.instance, target->mutable_instance());
    for (const auto& voter : source.voters) {
        target->add_voters(voter);
    }
}

PersistCommand from_pb_command(const rpc::raft::Command& source) {
    PersistCommand result;
    result.type = from_pb_command_type(source.type());
    result.key = source.key();
    result.value = source.value();
    result.service = source.service();
    result.instance = from_pb_service_instance(source.instance());
    result.voters.reserve(static_cast<std::size_t>(source.voters_size()));
    for (const auto& voter : source.voters()) {
        result.voters.push_back(voter);
    }
    return result;
}

void to_pb_log_entry(const PersistLogEntry& source, rpc::raft::LogEntry* target) {
    if (target == nullptr) {
        return;
    }
    target->set_index(source.index);
    target->set_term(source.term);
    to_pb_command(source.command, target->mutable_command());
}

PersistLogEntry from_pb_log_entry(const rpc::raft::LogEntry& source) {
    PersistLogEntry result;
    result.index = source.index();
    result.term = source.term();
    result.command = from_pb_command(source.command());
    return result;
}

void to_pb_snapshot(const PersistSnapshot& source, rpc::raft::Snapshot* target) {
    if (target == nullptr) {
        return;
    }

    target->set_last_included_index(source.last_included_index);
    target->set_last_included_term(source.last_included_term);

    for (const auto& [key, value] : source.state.configs) {
        rpc::raft::ConfigEntry* entry = target->add_configs();
        entry->set_key(key);
        entry->set_value(value);
    }
    for (const auto& [service, instances] : source.state.services) {
        rpc::raft::ServiceEntry* entry = target->add_services();
        entry->set_service(service);
        for (const auto& instance : instances) {
            to_pb_service_instance(instance, entry->add_instances());
        }
    }
}

PersistSnapshot from_pb_snapshot(const rpc::raft::Snapshot& source) {
    PersistSnapshot result;
    result.last_included_index = source.last_included_index();
    result.last_included_term = source.last_included_term();

    for (const auto& config : source.configs()) {
        result.state.configs.emplace(config.key(), config.value());
    }
    for (const auto& service : source.services()) {
        std::vector<PersistServiceInstance> instances;
        instances.reserve(static_cast<std::size_t>(service.instances_size()));
        for (const auto& instance : service.instances()) {
            instances.push_back(from_pb_service_instance(instance));
        }
        result.state.services.emplace(service.service(), std::move(instances));
    }
    return result;
}

bool parse_endpoint(std::string_view endpoint, std::string* host, std::uint16_t* port) {
    if (host == nullptr || port == nullptr) {
        return false;
    }

    const std::size_t sep = endpoint.rfind(':');
    if (sep == std::string_view::npos || sep == 0 || sep + 1 >= endpoint.size()) {
        return false;
    }

    *host = std::string(endpoint.substr(0, sep));
    const std::string port_text(endpoint.substr(sep + 1));
    unsigned long parsed = 0;
    try {
        parsed = std::stoul(port_text);
    } catch (...) {
        return false;
    }

    if (parsed == 0 || parsed > 65535UL) {
        return false;
    }
    *port = static_cast<std::uint16_t>(parsed);
    return true;
}

bool set_socket_timeout(int fd, std::uint64_t timeout_ms) {
    if (fd < 0) {
        return false;
    }
    if (timeout_ms == 0) {
        timeout_ms = 1000;
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

bool send_all(int fd, const char* data, std::size_t len) {
    std::size_t sent = 0;
    while (sent < len) {
        const ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
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

bool recv_all(int fd, char* data, std::size_t len) {
    std::size_t read = 0;
    while (read < len) {
        const ssize_t n = ::recv(fd, data + read, len - read, 0);
        if (n > 0) {
            read += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

bool send_framed(int fd, const std::string& payload) {
    const std::uint32_t size = static_cast<std::uint32_t>(payload.size());
    const std::uint32_t net_size = htonl(size);
    return send_all(fd, reinterpret_cast<const char*>(&net_size), sizeof(net_size))
        && send_all(fd, payload.data(), payload.size());
}

bool recv_framed(int fd, std::string* payload) {
    if (payload == nullptr) {
        return false;
    }

    std::uint32_t net_size = 0;
    if (!recv_all(fd, reinterpret_cast<char*>(&net_size), sizeof(net_size))) {
        return false;
    }

    const std::uint32_t size = ntohl(net_size);
    if (size > kMaxFrameBytes) {
        return false;
    }

    payload->assign(size, '\0');
    if (size == 0) {
        return true;
    }
    return recv_all(fd, payload->data(), payload->size());
}

}  // namespace

class RaftRpcServer::Impl {
public:
    Impl(std::string bind_host, std::uint16_t bind_port, std::shared_ptr<IRaftRpcHandler> handler)
        : bind_host_(std::move(bind_host)),
          bind_port_(bind_port),
          handler_(std::move(handler)) {}

    ~Impl() {
        stop();
    }

    bool start() {
        if (running_.load(std::memory_order_acquire) || !handler_) {
            return false;
        }

        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return false;
        }

        const int reuse = 1;
        (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, static_cast<socklen_t>(sizeof(reuse)));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(bind_port_);
        if (::inet_pton(AF_INET, bind_host_.c_str(), &addr.sin_addr) != 1) {
            ::close(fd);
            return false;
        }

        if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
            ::close(fd);
            return false;
        }

        if (::listen(fd, 64) != 0) {
            ::close(fd);
            return false;
        }

        listen_fd_.store(fd, std::memory_order_release);
        running_.store(true, std::memory_order_release);
        worker_ = std::thread([this]() { accept_loop(); });
        return true;
    }

    void stop() {
        if (!running_.exchange(false, std::memory_order_acq_rel)) {
            return;
        }

        const int fd = listen_fd_.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
        }

        if (worker_.joinable()) {
            worker_.join();
        }
    }

    bool running() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

    std::string endpoint() const {
        return bind_host_ + ":" + std::to_string(bind_port_);
    }

private:
    void accept_loop() {
        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in peer{};
            socklen_t peer_len = static_cast<socklen_t>(sizeof(peer));
            const int client_fd = ::accept(
                listen_fd_.load(std::memory_order_acquire),
                reinterpret_cast<sockaddr*>(&peer),
                &peer_len
            );
            if (client_fd < 0) {
                if (!running_.load(std::memory_order_acquire)) {
                    return;
                }
                if (errno == EINTR) {
                    continue;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }

            std::thread([this, client_fd]() {
                handle_connection(client_fd);
                ::close(client_fd);
            }).detach();
        }
    }

    void handle_connection(int client_fd) const {
        if (!set_socket_timeout(client_fd, 1000)) {
            return;
        }

        std::string request_payload;
        if (!recv_framed(client_fd, &request_payload)) {
            return;
        }

        rpc::raft::RpcRequestEnvelope request;
        if (!request.ParseFromString(request_payload)) {
            return;
        }

        rpc::raft::RpcResponseEnvelope response;
        switch (request.payload_case()) {
            case rpc::raft::RpcRequestEnvelope::kRequestVote: {
                RequestVoteRequest call;
                call.term = request.request_vote().term();
                call.candidate_id = request.request_vote().candidate_id();
                call.last_log_index = request.request_vote().last_log_index();
                call.last_log_term = request.request_vote().last_log_term();

                const RequestVoteResponse rpc_response = handler_->on_request_vote(call);
                rpc::raft::RequestVoteResponse* target = response.mutable_request_vote();
                target->set_term(rpc_response.term);
                target->set_vote_granted(rpc_response.vote_granted);
                break;
            }
            case rpc::raft::RpcRequestEnvelope::kAppendEntries: {
                AppendEntriesRequest call;
                call.term = request.append_entries().term();
                call.leader_id = request.append_entries().leader_id();
                call.prev_log_index = request.append_entries().prev_log_index();
                call.prev_log_term = request.append_entries().prev_log_term();
                call.leader_commit = request.append_entries().leader_commit();
                call.entries.reserve(static_cast<std::size_t>(request.append_entries().entries_size()));
                for (const auto& entry : request.append_entries().entries()) {
                    call.entries.push_back(from_pb_log_entry(entry));
                }

                const AppendEntriesResponse rpc_response = handler_->on_append_entries(call);
                rpc::raft::AppendEntriesResponse* target = response.mutable_append_entries();
                target->set_term(rpc_response.term);
                target->set_success(rpc_response.success);
                target->set_match_index(rpc_response.match_index);
                break;
            }
            case rpc::raft::RpcRequestEnvelope::kInstallSnapshot: {
                InstallSnapshotRequest call;
                call.term = request.install_snapshot().term();
                call.leader_id = request.install_snapshot().leader_id();
                call.snapshot = from_pb_snapshot(request.install_snapshot().snapshot());

                const InstallSnapshotResponse rpc_response = handler_->on_install_snapshot(call);
                rpc::raft::InstallSnapshotResponse* target = response.mutable_install_snapshot();
                target->set_term(rpc_response.term);
                target->set_success(rpc_response.success);
                break;
            }
            case rpc::raft::RpcRequestEnvelope::PAYLOAD_NOT_SET:
                return;
        }

        std::string response_payload;
        if (!response.SerializeToString(&response_payload)) {
            return;
        }
        (void)send_framed(client_fd, response_payload);
    }

private:
    std::string bind_host_;
    std::uint16_t bind_port_{0};
    std::shared_ptr<IRaftRpcHandler> handler_;

    std::atomic<bool> running_{false};
    std::atomic<int> listen_fd_{-1};
    std::thread worker_;
};

RaftRpcServer::RaftRpcServer(
    std::string bind_host,
    std::uint16_t bind_port,
    std::shared_ptr<IRaftRpcHandler> handler
)
    : impl_(std::make_unique<Impl>(std::move(bind_host), bind_port, std::move(handler))) {}

RaftRpcServer::~RaftRpcServer() = default;

bool RaftRpcServer::start() {
    return impl_->start();
}

void RaftRpcServer::stop() {
    impl_->stop();
}

bool RaftRpcServer::running() const noexcept {
    return impl_->running();
}

std::string RaftRpcServer::endpoint() const {
    return impl_->endpoint();
}

std::optional<RequestVoteResponse> RaftRpcClient::request_vote(
    const std::string& endpoint,
    const RequestVoteRequest& request,
    std::uint64_t timeout_ms
) const {
    std::string host;
    std::uint16_t port = 0;
    if (!parse_endpoint(endpoint, &host, &port)) {
        return std::nullopt;
    }

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }
    auto close_fd = [&]() { ::close(fd); };

    if (!set_socket_timeout(fd, timeout_ms)) {
        close_fd();
        return std::nullopt;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        close_fd();
        return std::nullopt;
    }
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
        close_fd();
        return std::nullopt;
    }

    rpc::raft::RpcRequestEnvelope envelope;
    rpc::raft::RequestVoteRequest* pb_request = envelope.mutable_request_vote();
    pb_request->set_term(request.term);
    pb_request->set_candidate_id(request.candidate_id);
    pb_request->set_last_log_index(request.last_log_index);
    pb_request->set_last_log_term(request.last_log_term);

    std::string payload;
    if (!envelope.SerializeToString(&payload) || !send_framed(fd, payload)) {
        close_fd();
        return std::nullopt;
    }

    std::string response_payload;
    if (!recv_framed(fd, &response_payload)) {
        close_fd();
        return std::nullopt;
    }

    rpc::raft::RpcResponseEnvelope response;
    if (!response.ParseFromString(response_payload)
        || response.payload_case() != rpc::raft::RpcResponseEnvelope::kRequestVote) {
        close_fd();
        return std::nullopt;
    }

    close_fd();
    RequestVoteResponse result;
    result.term = response.request_vote().term();
    result.vote_granted = response.request_vote().vote_granted();
    return result;
}

std::optional<AppendEntriesResponse> RaftRpcClient::append_entries(
    const std::string& endpoint,
    const AppendEntriesRequest& request,
    std::uint64_t timeout_ms
) const {
    std::string host;
    std::uint16_t port = 0;
    if (!parse_endpoint(endpoint, &host, &port)) {
        return std::nullopt;
    }

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }
    auto close_fd = [&]() { ::close(fd); };

    if (!set_socket_timeout(fd, timeout_ms)) {
        close_fd();
        return std::nullopt;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        close_fd();
        return std::nullopt;
    }
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
        close_fd();
        return std::nullopt;
    }

    rpc::raft::RpcRequestEnvelope envelope;
    rpc::raft::AppendEntriesRequest* pb_request = envelope.mutable_append_entries();
    pb_request->set_term(request.term);
    pb_request->set_leader_id(request.leader_id);
    pb_request->set_prev_log_index(request.prev_log_index);
    pb_request->set_prev_log_term(request.prev_log_term);
    pb_request->set_leader_commit(request.leader_commit);
    for (const auto& entry : request.entries) {
        to_pb_log_entry(entry, pb_request->add_entries());
    }

    std::string payload;
    if (!envelope.SerializeToString(&payload) || !send_framed(fd, payload)) {
        close_fd();
        return std::nullopt;
    }

    std::string response_payload;
    if (!recv_framed(fd, &response_payload)) {
        close_fd();
        return std::nullopt;
    }

    rpc::raft::RpcResponseEnvelope response;
    if (!response.ParseFromString(response_payload)
        || response.payload_case() != rpc::raft::RpcResponseEnvelope::kAppendEntries) {
        close_fd();
        return std::nullopt;
    }

    close_fd();
    AppendEntriesResponse result;
    result.term = response.append_entries().term();
    result.success = response.append_entries().success();
    result.match_index = response.append_entries().match_index();
    return result;
}

std::optional<InstallSnapshotResponse> RaftRpcClient::install_snapshot(
    const std::string& endpoint,
    const InstallSnapshotRequest& request,
    std::uint64_t timeout_ms
) const {
    std::string host;
    std::uint16_t port = 0;
    if (!parse_endpoint(endpoint, &host, &port)) {
        return std::nullopt;
    }

    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }
    auto close_fd = [&]() { ::close(fd); };

    if (!set_socket_timeout(fd, timeout_ms)) {
        close_fd();
        return std::nullopt;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        close_fd();
        return std::nullopt;
    }
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), static_cast<socklen_t>(sizeof(addr))) != 0) {
        close_fd();
        return std::nullopt;
    }

    rpc::raft::RpcRequestEnvelope envelope;
    rpc::raft::InstallSnapshotRequest* pb_request = envelope.mutable_install_snapshot();
    pb_request->set_term(request.term);
    pb_request->set_leader_id(request.leader_id);
    to_pb_snapshot(request.snapshot, pb_request->mutable_snapshot());

    std::string payload;
    if (!envelope.SerializeToString(&payload) || !send_framed(fd, payload)) {
        close_fd();
        return std::nullopt;
    }

    std::string response_payload;
    if (!recv_framed(fd, &response_payload)) {
        close_fd();
        return std::nullopt;
    }

    rpc::raft::RpcResponseEnvelope response;
    if (!response.ParseFromString(response_payload)
        || response.payload_case() != rpc::raft::RpcResponseEnvelope::kInstallSnapshot) {
        close_fd();
        return std::nullopt;
    }

    close_fd();
    InstallSnapshotResponse result;
    result.term = response.install_snapshot().term();
    result.success = response.install_snapshot().success();
    return result;
}

}  // namespace rpc::infra::raft

