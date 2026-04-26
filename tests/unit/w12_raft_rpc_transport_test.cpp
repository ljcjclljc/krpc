#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rpc/infra/raft/raft_rpc.h"

namespace {

class MockRaftHandler final : public rpc::infra::raft::IRaftRpcHandler {
public:
    rpc::infra::raft::RequestVoteResponse on_request_vote(
        const rpc::infra::raft::RequestVoteRequest& request
    ) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (request.term < current_term_) {
            return rpc::infra::raft::RequestVoteResponse{current_term_, false};
        }

        if (request.term > current_term_) {
            current_term_ = request.term;
            voted_for_.clear();
        }

        bool granted = false;
        if (voted_for_.empty() || voted_for_ == request.candidate_id) {
            voted_for_ = request.candidate_id;
            granted = true;
        }
        return rpc::infra::raft::RequestVoteResponse{current_term_, granted};
    }

    rpc::infra::raft::AppendEntriesResponse on_append_entries(
        const rpc::infra::raft::AppendEntriesRequest& request
    ) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (request.term < current_term_) {
            return rpc::infra::raft::AppendEntriesResponse{current_term_, false, last_match_index_};
        }

        current_term_ = request.term;
        if (!request.entries.empty()) {
            last_match_index_ = request.entries.back().index;
        }
        return rpc::infra::raft::AppendEntriesResponse{current_term_, true, last_match_index_};
    }

    rpc::infra::raft::InstallSnapshotResponse on_install_snapshot(
        const rpc::infra::raft::InstallSnapshotRequest& request
    ) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (request.term < current_term_) {
            return rpc::infra::raft::InstallSnapshotResponse{current_term_, false};
        }
        current_term_ = request.term;
        snapshot_ = request.snapshot;
        return rpc::infra::raft::InstallSnapshotResponse{current_term_, true};
    }

    bool has_snapshot_key(const std::string& key, const std::string& value) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = snapshot_.state.configs.find(key);
        return it != snapshot_.state.configs.end() && it->second == value;
    }

private:
    mutable std::mutex mutex_;
    std::uint64_t current_term_{0};
    std::string voted_for_;
    std::uint64_t last_match_index_{0};
    rpc::infra::raft::PersistSnapshot snapshot_;
};

}  // namespace

int main() {
    auto handler = std::make_shared<MockRaftHandler>();

    std::uint16_t bound_port = 0;
    std::unique_ptr<rpc::infra::raft::RaftRpcServer> server;
    const std::uint16_t base_port = static_cast<std::uint16_t>(
        32000 + (std::chrono::steady_clock::now().time_since_epoch().count() % 10000)
    );
    for (std::uint16_t offset = 0; offset < 32; ++offset) {
        const std::uint16_t candidate = static_cast<std::uint16_t>(base_port + offset);
        auto candidate_server = std::make_unique<rpc::infra::raft::RaftRpcServer>("127.0.0.1", candidate, handler);
        if (candidate_server->start()) {
            bound_port = candidate;
            server = std::move(candidate_server);
            break;
        }
    }

    if (!server || bound_port == 0) {
        std::cout << "w12_raft_rpc_transport_test skipped: rpc bind/listen not permitted\n";
        return 0;
    }

    rpc::infra::raft::RaftRpcClient client;
    const std::string endpoint = "127.0.0.1:" + std::to_string(bound_port);

    rpc::infra::raft::RequestVoteRequest vote_request;
    vote_request.term = 7;
    vote_request.candidate_id = "cand-A";
    vote_request.last_log_index = 3;
    vote_request.last_log_term = 7;

    const auto vote_response = client.request_vote(endpoint, vote_request, 500);
    if (!vote_response.has_value() || !vote_response->vote_granted || vote_response->term != 7) {
        std::cerr << "request_vote rpc failed\n";
        server->stop();
        return 1;
    }

    vote_request.candidate_id = "cand-B";
    const auto second_vote = client.request_vote(endpoint, vote_request, 500);
    if (!second_vote.has_value() || second_vote->vote_granted) {
        std::cerr << "request_vote repeat semantic mismatch\n";
        server->stop();
        return 1;
    }

    rpc::infra::raft::AppendEntriesRequest append_request;
    append_request.term = 7;
    append_request.leader_id = "cand-A";
    append_request.prev_log_index = 3;
    append_request.prev_log_term = 7;
    append_request.leader_commit = 4;

    rpc::infra::raft::PersistLogEntry entry;
    entry.index = 4;
    entry.term = 7;
    entry.command.type = rpc::infra::raft::PersistCommandType::UpsertConfig;
    entry.command.key = "k";
    entry.command.value = "v";
    append_request.entries.push_back(entry);

    const auto append_response = client.append_entries(endpoint, append_request, 500);
    if (!append_response.has_value() || !append_response->success || append_response->match_index != 4) {
        std::cerr << "append_entries rpc failed\n";
        server->stop();
        return 1;
    }

    rpc::infra::raft::InstallSnapshotRequest snapshot_request;
    snapshot_request.term = 8;
    snapshot_request.leader_id = "cand-A";
    snapshot_request.snapshot.last_included_index = 8;
    snapshot_request.snapshot.last_included_term = 8;
    snapshot_request.snapshot.state.configs["snap.k"] = "snap.v";

    const auto snapshot_response = client.install_snapshot(endpoint, snapshot_request, 500);
    if (!snapshot_response.has_value() || !snapshot_response->success || snapshot_response->term != 8) {
        std::cerr << "install_snapshot rpc failed\n";
        server->stop();
        return 1;
    }

    if (!handler->has_snapshot_key("snap.k", "snap.v")) {
        std::cerr << "snapshot data was not delivered to handler\n";
        server->stop();
        return 1;
    }

    server->stop();
    std::cout << "w12_raft_rpc_transport_test passed\n";
    return 0;
}
