#pragma once

// 文件用途：
// 提供 Raft 节点间真实 RPC 传输能力（TCP + Protobuf）：
// 1) RequestVote
// 2) AppendEntries
// 3) InstallSnapshot

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rpc/infra/raft/raft_storage.h"

namespace rpc::infra::raft {

struct RequestVoteRequest {
    std::uint64_t term{0};
    std::string candidate_id;
    std::uint64_t last_log_index{0};
    std::uint64_t last_log_term{0};
};

struct RequestVoteResponse {
    std::uint64_t term{0};
    bool vote_granted{false};
};

struct AppendEntriesRequest {
    std::uint64_t term{0};
    std::string leader_id;
    std::uint64_t prev_log_index{0};
    std::uint64_t prev_log_term{0};
    std::vector<PersistLogEntry> entries;
    std::uint64_t leader_commit{0};
};

struct AppendEntriesResponse {
    std::uint64_t term{0};
    bool success{false};
    std::uint64_t match_index{0};
};

struct InstallSnapshotRequest {
    std::uint64_t term{0};
    std::string leader_id;
    PersistSnapshot snapshot;
};

struct InstallSnapshotResponse {
    std::uint64_t term{0};
    bool success{false};
};

class IRaftRpcHandler {
public:
    virtual ~IRaftRpcHandler() = default;

    virtual RequestVoteResponse on_request_vote(const RequestVoteRequest& request) = 0;
    virtual AppendEntriesResponse on_append_entries(const AppendEntriesRequest& request) = 0;
    virtual InstallSnapshotResponse on_install_snapshot(const InstallSnapshotRequest& request) = 0;
};

class RaftRpcServer {
public:
    RaftRpcServer(std::string bind_host, std::uint16_t bind_port, std::shared_ptr<IRaftRpcHandler> handler);
    ~RaftRpcServer();

    bool start();
    void stop();

    bool running() const noexcept;
    std::string endpoint() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class RaftRpcClient {
public:
    std::optional<RequestVoteResponse> request_vote(
        const std::string& endpoint,
        const RequestVoteRequest& request,
        std::uint64_t timeout_ms
    ) const;

    std::optional<AppendEntriesResponse> append_entries(
        const std::string& endpoint,
        const AppendEntriesRequest& request,
        std::uint64_t timeout_ms
    ) const;

    std::optional<InstallSnapshotResponse> install_snapshot(
        const std::string& endpoint,
        const InstallSnapshotRequest& request,
        std::uint64_t timeout_ms
    ) const;
};

}  // namespace rpc::infra::raft

