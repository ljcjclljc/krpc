#include "rpc/infra/raft/raft_cluster.h"

// 文件用途：
// W12 Raft 一致性模块实现：
// - Leader 选举 / 日志复制 / 快照
// - Joint Consensus 成员变更
// - 元数据状态机（配置 + 服务发现）
// - 协程调度链路融合（可选）

#include <algorithm>
#include <atomic>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

#include <boost/algorithm/clamp.hpp>
#include <boost/container/flat_set.hpp>

#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/scheduler.h"
#include "rpc/infra/raft/raft_rpc.h"
#include "rpc/infra/raft/raft_storage.h"

namespace rpc::infra::raft {

class RaftCluster::Impl {
public:
    explicit Impl(RaftOptions options)
        : options_(normalize_options(std::move(options))),
          rng_(0xC0FFEEu) {}

    ~Impl() {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_rpc_servers_locked();
    }

    void bootstrap(const std::vector<std::string>& initial_voters) {
        run_on_scheduler([this, initial_voters]() {
            std::lock_guard<std::mutex> lock(mutex_);
            reset_state_locked();
            init_storage_if_needed_locked();

            boost::container::flat_set<std::string> voters;
            for (const auto& id : initial_voters) {
                if (!id.empty()) {
                    voters.insert(id);
                }
            }
            if (voters.empty()) {
                return;
            }

            current_voters_ = voters;
            for (const auto& id : voters) {
                (void)ensure_node_locked(id);
            }
            update_node_voter_flags_locked();
            persist_all_nodes_locked();
        });
    }

    void tick() {
        run_on_scheduler([this]() {
            std::lock_guard<std::mutex> lock(mutex_);
            tick_locked();
            persist_all_nodes_locked();
        });
    }

    void run_ticks(std::size_t ticks) {
        run_on_scheduler([this, ticks]() {
            std::lock_guard<std::mutex> lock(mutex_);
            for (std::size_t i = 0; i < ticks; ++i) {
                tick_locked();
            }
            persist_all_nodes_locked();
        });
    }

    bool wait_for_leader(std::size_t max_ticks) {
        return run_on_scheduler([this, max_ticks]() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (has_live_leader_locked()) {
                persist_all_nodes_locked();
                return true;
            }
            for (std::size_t i = 0; i < max_ticks; ++i) {
                tick_locked();
                if (has_live_leader_locked()) {
                    persist_all_nodes_locked();
                    return true;
                }
            }
            persist_all_nodes_locked();
            return false;
        });
    }

    bool set_node_available(const std::string& node_id, bool available) {
        if (node_id.empty()) {
            return false;
        }
        return run_on_scheduler([this, node_id, available]() {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = nodes_.find(node_id);
            if (it == nodes_.end()) {
                return false;
            }

            Node& node = it->second;
            node.alive = available;
            node.role = Role::Follower;
            if (available) {
                reset_election_timer_locked(node);
            }

            if (!available && leader_id_ == node_id) {
                leader_id_.clear();
            }
            if (!available && !pending_replications_.empty()) {
                pending_replications_.erase(
                    std::remove_if(
                        pending_replications_.begin(),
                        pending_replications_.end(),
                        [&node_id](const PendingReplication& pending) {
                            return pending.follower_id == node_id;
                        }
                    ),
                    pending_replications_.end()
                );
            }
            (void)persist_node_locked(node);
            return true;
        });
    }

    bool upsert_config(std::string key, std::string value) {
        if (key.empty()) {
            return false;
        }
        Command command;
        command.type = CommandType::UpsertConfig;
        command.key = std::move(key);
        command.value = std::move(value);
        return append_and_commit(command);
    }

    bool erase_config(const std::string& key) {
        if (key.empty()) {
            return false;
        }
        Command command;
        command.type = CommandType::EraseConfig;
        command.key = key;
        return append_and_commit(command);
    }

    bool register_service(std::string service, ServiceInstance instance) {
        if (service.empty() || instance.id.empty() || instance.host.empty() || instance.port == 0) {
            return false;
        }
        Command command;
        command.type = CommandType::RegisterService;
        command.service = std::move(service);
        command.instance = std::move(instance);
        return append_and_commit(command);
    }

    bool unregister_service(const std::string& service, const std::string& instance_id) {
        if (service.empty() || instance_id.empty()) {
            return false;
        }
        Command command;
        command.type = CommandType::UnregisterService;
        command.service = service;
        command.instance.id = instance_id;
        return append_and_commit(command);
    }

    std::optional<std::string> config_value(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const Node* reader = select_reader_locked();
        if (reader == nullptr) {
            return std::nullopt;
        }
        const auto it = reader->state.configs.find(key);
        if (it == reader->state.configs.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    std::unordered_map<std::string, std::string> config_snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        const Node* reader = select_reader_locked();
        if (reader == nullptr) {
            return {};
        }
        return reader->state.configs;
    }

    std::vector<ServiceInstance> discover_service(const std::string& service) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const Node* reader = select_reader_locked();
        if (reader == nullptr) {
            return {};
        }
        const auto it = reader->state.services.find(service);
        if (it == reader->state.services.end()) {
            return {};
        }
        return it->second;
    }

    bool begin_joint_consensus(const std::vector<std::string>& new_voters) {
        return run_on_scheduler([this, new_voters]() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (joint_.active) {
                return false;
            }

            boost::container::flat_set<std::string> target_voters;
            for (const auto& id : new_voters) {
                if (!id.empty()) {
                    target_voters.insert(id);
                }
            }
            if (target_voters.empty()) {
                return false;
            }

            Command command;
            command.type = CommandType::BeginJointConsensus;
            command.voters = std::move(target_voters);
            return append_and_commit_locked(command);
        });
    }

    bool finalize_joint_consensus() {
        return run_on_scheduler([this]() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!joint_.active) {
                return false;
            }
            Command command;
            command.type = CommandType::FinalizeJointConsensus;
            command.voters = joint_.new_voters;
            return append_and_commit_locked(command);
        });
    }

    bool reconfigure_joint(const std::vector<std::string>& new_voters, std::size_t max_ticks) {
        return run_on_scheduler([this, new_voters, max_ticks]() {
            std::lock_guard<std::mutex> lock(mutex_);

            boost::container::flat_set<std::string> target_voters;
            for (const auto& id : new_voters) {
                if (!id.empty()) {
                    target_voters.insert(id);
                }
            }
            if (target_voters.empty()) {
                return false;
            }
            if (joint_.active) {
                return false;
            }

            Command begin;
            begin.type = CommandType::BeginJointConsensus;
            begin.voters = target_voters;
            if (!append_and_commit_locked(begin)) {
                return false;
            }

            for (std::size_t i = 0; i < max_ticks; ++i) {
                tick_locked();
            }

            if (!joint_.active) {
                return false;
            }

            Command finalize;
            finalize.type = CommandType::FinalizeJointConsensus;
            finalize.voters = target_voters;
            return append_and_commit_locked(finalize);
        });
    }

    void configure_fault_injection(RaftFaultInjectionOptions options) {
        run_on_scheduler([this, options]() {
            std::lock_guard<std::mutex> lock(mutex_);
            fault_options_ = normalize_fault_options(options);
        });
    }

    RaftFaultInjectionOptions fault_injection_options() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return fault_options_;
    }

    bool metadata_consistent() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (current_voters_.empty()) {
            return true;
        }

        const Node* baseline = nullptr;
        for (const auto& voter : current_voters_) {
            const auto it = nodes_.find(voter);
            if (it == nodes_.end() || !it->second.alive) {
                continue;
            }
            baseline = &it->second;
            break;
        }
        if (baseline == nullptr) {
            return true;
        }

        for (const auto& voter : current_voters_) {
            const auto it = nodes_.find(voter);
            if (it == nodes_.end() || !it->second.alive) {
                continue;
            }
            if (!metadata_equal_locked(baseline->state, it->second.state)) {
                return false;
            }
        }
        return true;
    }

    RaftStatus status() const {
        std::lock_guard<std::mutex> lock(mutex_);

        RaftStatus result;
        result.joint_consensus_active = joint_.active;
        result.leader_changes = leader_changes_;
        result.committed_index = committed_index_;
        result.snapshot_count = snapshot_count_;
        result.scheduler_dispatches = scheduler_dispatches_.load(std::memory_order_acquire);
        result.rpc_mirror_calls = rpc_mirror_calls_.load(std::memory_order_acquire);
        result.replication_delayed = replication_delayed_;
        result.replication_dropped = replication_dropped_;

        auto leader_it = nodes_.find(leader_id_);
        if (leader_it != nodes_.end() && leader_it->second.alive && leader_it->second.role == Role::Leader) {
            result.has_leader = true;
            result.leader_id = leader_id_;
            result.current_term = leader_it->second.current_term;
        }

        result.voters.reserve(current_voters_.size());
        for (const auto& id : current_voters_) {
            result.voters.push_back(id);
        }
        return result;
    }

    std::optional<std::string> leader() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!has_live_leader_locked()) {
            return std::nullopt;
        }
        return leader_id_;
    }

    std::optional<std::string> node_endpoint(const std::string& node_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::string endpoint = endpoint_for_node_locked(node_id);
        if (endpoint.empty()) {
            return std::nullopt;
        }
        return endpoint;
    }

private:
    enum class Role : std::uint8_t {
        Follower = 0,
        Candidate = 1,
        Leader = 2,
    };

    enum class CommandType : std::uint8_t {
        UpsertConfig = 0,
        EraseConfig = 1,
        RegisterService = 2,
        UnregisterService = 3,
        BeginJointConsensus = 4,
        FinalizeJointConsensus = 5,
    };

    struct Command {
        CommandType type{CommandType::UpsertConfig};
        std::string key;
        std::string value;
        std::string service;
        ServiceInstance instance;
        boost::container::flat_set<std::string> voters;
    };

    struct LogEntry {
        std::uint64_t index{0};
        std::uint64_t term{0};
        Command command;
    };

    struct MetadataState {
        std::unordered_map<std::string, std::string> configs;
        std::unordered_map<std::string, std::vector<ServiceInstance>> services;
    };

    struct Snapshot {
        std::uint64_t last_included_index{0};
        std::uint64_t last_included_term{0};
        MetadataState state;
    };

    struct Node {
        std::string id;
        bool alive{true};
        bool voter{true};
        Role role{Role::Follower};
        std::uint64_t current_term{0};
        std::string voted_for;
        std::uint64_t commit_index{0};
        std::uint64_t last_applied{0};
        std::size_t election_elapsed{0};
        std::size_t election_timeout{0};
        std::size_t heartbeat_elapsed{0};
        std::vector<LogEntry> log;
        Snapshot snapshot;
        MetadataState state;
    };

    struct JointConfig {
        bool active{false};
        boost::container::flat_set<std::string> old_voters;
        boost::container::flat_set<std::string> new_voters;
    };

    struct ReplicationPayload {
        std::string leader_id;
        std::uint64_t leader_term{0};
        std::uint64_t leader_commit{0};
        Snapshot snapshot;
        std::vector<LogEntry> log;
        std::vector<PersistLogEntry> persisted_entries;
    };

    struct PendingReplication {
        std::string follower_id;
        std::shared_ptr<ReplicationPayload> payload;
        std::size_t remaining_ticks{0};
    };

    class NodeRpcHandler final : public IRaftRpcHandler {
    public:
        NodeRpcHandler(Impl* owner, std::string node_id)
            : owner_(owner), node_id_(std::move(node_id)) {}

        RequestVoteResponse on_request_vote(const RequestVoteRequest& request) override {
            return owner_->handle_request_vote_rpc(node_id_, request);
        }

        AppendEntriesResponse on_append_entries(const AppendEntriesRequest& request) override {
            return owner_->handle_append_entries_rpc(node_id_, request);
        }

        InstallSnapshotResponse on_install_snapshot(const InstallSnapshotRequest& request) override {
            return owner_->handle_install_snapshot_rpc(node_id_, request);
        }

    private:
        Impl* owner_{nullptr};
        std::string node_id_;
    };

    static RaftOptions normalize_options(RaftOptions options) {
        options.election_timeout_ticks_min = boost::algorithm::clamp<std::size_t>(
            options.election_timeout_ticks_min,
            2,
            1024
        );
        options.election_timeout_ticks_max = boost::algorithm::clamp<std::size_t>(
            options.election_timeout_ticks_max,
            options.election_timeout_ticks_min,
            2048
        );
        const std::size_t max_heartbeat =
            options.election_timeout_ticks_min > 1 ? options.election_timeout_ticks_min - 1 : 1;
        options.heartbeat_interval_ticks = boost::algorithm::clamp<std::size_t>(
            options.heartbeat_interval_ticks,
            1,
            std::max<std::size_t>(1, max_heartbeat)
        );
        options.snapshot_log_threshold = boost::algorithm::clamp<std::size_t>(
            options.snapshot_log_threshold,
            4,
            100000
        );
        if (options.rpc_bind_host.empty()) {
            options.rpc_bind_host = "127.0.0.1";
        }
        if (options.rpc_base_port == 0) {
            options.rpc_base_port = 19000;
        }
        return options;
    }

    static RaftFaultInjectionOptions normalize_fault_options(RaftFaultInjectionOptions options) {
        options.packet_loss_percent = boost::algorithm::clamp<double>(options.packet_loss_percent, 0.0, 100.0);
        if (options.replicate_delay_ticks_max < options.replicate_delay_ticks_min) {
            options.replicate_delay_ticks_max = options.replicate_delay_ticks_min;
        }
        return options;
    }

    PersistCommand to_persist_command(const Command& source) const {
        PersistCommand target;
        switch (source.type) {
            case CommandType::UpsertConfig:
                target.type = PersistCommandType::UpsertConfig;
                break;
            case CommandType::EraseConfig:
                target.type = PersistCommandType::EraseConfig;
                break;
            case CommandType::RegisterService:
                target.type = PersistCommandType::RegisterService;
                break;
            case CommandType::UnregisterService:
                target.type = PersistCommandType::UnregisterService;
                break;
            case CommandType::BeginJointConsensus:
                target.type = PersistCommandType::BeginJointConsensus;
                break;
            case CommandType::FinalizeJointConsensus:
                target.type = PersistCommandType::FinalizeJointConsensus;
                break;
        }
        target.key = source.key;
        target.value = source.value;
        target.service = source.service;
        target.instance.id = source.instance.id;
        target.instance.host = source.instance.host;
        target.instance.port = source.instance.port;
        target.voters.reserve(source.voters.size());
        for (const auto& voter : source.voters) {
            target.voters.push_back(voter);
        }
        return target;
    }

    Command from_persist_command(const PersistCommand& source) const {
        Command target;
        switch (source.type) {
            case PersistCommandType::UpsertConfig:
                target.type = CommandType::UpsertConfig;
                break;
            case PersistCommandType::EraseConfig:
                target.type = CommandType::EraseConfig;
                break;
            case PersistCommandType::RegisterService:
                target.type = CommandType::RegisterService;
                break;
            case PersistCommandType::UnregisterService:
                target.type = CommandType::UnregisterService;
                break;
            case PersistCommandType::BeginJointConsensus:
                target.type = CommandType::BeginJointConsensus;
                break;
            case PersistCommandType::FinalizeJointConsensus:
                target.type = CommandType::FinalizeJointConsensus;
                break;
        }
        target.key = source.key;
        target.value = source.value;
        target.service = source.service;
        target.instance.id = source.instance.id;
        target.instance.host = source.instance.host;
        target.instance.port = source.instance.port;
        for (const auto& voter : source.voters) {
            target.voters.insert(voter);
        }
        return target;
    }

    PersistLogEntry to_persist_log_entry(const LogEntry& source) const {
        PersistLogEntry target;
        target.index = source.index;
        target.term = source.term;
        target.command = to_persist_command(source.command);
        return target;
    }

    LogEntry from_persist_log_entry(const PersistLogEntry& source) const {
        LogEntry target;
        target.index = source.index;
        target.term = source.term;
        target.command = from_persist_command(source.command);
        return target;
    }

    PersistSnapshot to_persist_snapshot(const Snapshot& source) const {
        PersistSnapshot target;
        target.last_included_index = source.last_included_index;
        target.last_included_term = source.last_included_term;
        target.state.configs = source.state.configs;
        for (const auto& [service, instances] : source.state.services) {
            std::vector<PersistServiceInstance> copied;
            copied.reserve(instances.size());
            for (const auto& instance : instances) {
                PersistServiceInstance value;
                value.id = instance.id;
                value.host = instance.host;
                value.port = instance.port;
                copied.push_back(std::move(value));
            }
            target.state.services.emplace(service, std::move(copied));
        }
        return target;
    }

    Snapshot from_persist_snapshot(const PersistSnapshot& source) const {
        Snapshot target;
        target.last_included_index = source.last_included_index;
        target.last_included_term = source.last_included_term;
        target.state.configs = source.state.configs;
        for (const auto& [service, instances] : source.state.services) {
            std::vector<ServiceInstance> copied;
            copied.reserve(instances.size());
            for (const auto& instance : instances) {
                ServiceInstance value;
                value.id = instance.id;
                value.host = instance.host;
                value.port = instance.port;
                copied.push_back(std::move(value));
            }
            target.state.services.emplace(service, std::move(copied));
        }
        return target;
    }

    PersistNodeMeta to_persist_meta(const Node& node) const {
        PersistNodeMeta meta;
        meta.node_id = node.id;
        meta.current_term = node.current_term;
        meta.voted_for = node.voted_for;
        meta.commit_index = node.commit_index;
        meta.last_applied = node.last_applied;
        return meta;
    }

    void init_storage_if_needed_locked() {
        if (!options_.enable_persistence) {
            storage_.reset();
            return;
        }

        if (storage_) {
            return;
        }

        std::string root = options_.storage_root_dir;
        if (root.empty()) {
            root = "/tmp/rpc_raft_wal";
        }
        storage_ = std::make_unique<RaftStorage>(std::move(root));
    }

    std::string endpoint_for_node_locked(const std::string& node_id) const {
        const auto it = node_ports_.find(node_id);
        if (it == node_ports_.end()) {
            return {};
        }
        return options_.rpc_bind_host + ":" + std::to_string(it->second);
    }

    void stop_rpc_servers_locked() {
        for (auto& [node_id, server] : rpc_servers_) {
            (void)node_id;
            if (server) {
                server->stop();
            }
        }
        rpc_servers_.clear();
        rpc_handlers_.clear();
        node_ports_.clear();
        pending_rpc_calls_.clear();
    }

    void ensure_rpc_server_for_node_locked(const std::string& node_id) {
        if (!options_.enable_rpc_transport) {
            return;
        }

        if (node_ports_.find(node_id) == node_ports_.end()) {
            const std::uint16_t port = static_cast<std::uint16_t>(
                options_.rpc_base_port + static_cast<std::uint16_t>(node_ports_.size())
            );
            node_ports_.emplace(node_id, port);
        }

        if (rpc_servers_.find(node_id) != rpc_servers_.end()) {
            return;
        }

        const auto port_it = node_ports_.find(node_id);
        if (port_it == node_ports_.end()) {
            return;
        }

        auto handler = std::make_shared<NodeRpcHandler>(this, node_id);
        auto server = std::make_unique<RaftRpcServer>(options_.rpc_bind_host, port_it->second, handler);
        if (!server->start()) {
            return;
        }

        rpc_handlers_[node_id] = std::move(handler);
        rpc_servers_[node_id] = std::move(server);
    }

    void queue_request_vote_rpc_mirror_locked(const std::string& peer_id, const RequestVoteRequest& request) {
        const std::string endpoint = endpoint_for_node_locked(peer_id);
        if (endpoint.empty()) {
            return;
        }
        pending_rpc_calls_.push_back([this, endpoint, request]() {
            rpc_mirror_calls_.fetch_add(1, std::memory_order_relaxed);
            (void)rpc_client_.request_vote(endpoint, request, 300);
        });
    }

    void queue_append_entries_rpc_mirror_locked(const std::string& peer_id, const AppendEntriesRequest& request) {
        const std::string endpoint = endpoint_for_node_locked(peer_id);
        if (endpoint.empty()) {
            return;
        }
        pending_rpc_calls_.push_back([this, endpoint, request]() {
            rpc_mirror_calls_.fetch_add(1, std::memory_order_relaxed);
            (void)rpc_client_.append_entries(endpoint, request, 500);
        });
    }

    void queue_install_snapshot_rpc_mirror_locked(const std::string& peer_id, const InstallSnapshotRequest& request) {
        const std::string endpoint = endpoint_for_node_locked(peer_id);
        if (endpoint.empty()) {
            return;
        }
        pending_rpc_calls_.push_back([this, endpoint, request]() {
            rpc_mirror_calls_.fetch_add(1, std::memory_order_relaxed);
            (void)rpc_client_.install_snapshot(endpoint, request, 500);
        });
    }

    void flush_pending_rpc_calls() {
        std::vector<std::function<void()>> calls;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (pending_rpc_calls_.empty()) {
                return;
            }
            calls.swap(pending_rpc_calls_);
        }
        for (auto& call : calls) {
            call();
        }
    }

    void restore_node_from_storage_locked(Node& node) {
        if (!storage_) {
            return;
        }
        const std::optional<PersistNodeState> persisted = storage_->load_node_state(node.id);
        if (!persisted.has_value()) {
            return;
        }

        node.current_term = persisted->meta.current_term;
        node.voted_for = persisted->meta.voted_for;
        node.commit_index = persisted->meta.commit_index;
        node.snapshot = from_persist_snapshot(persisted->snapshot);
        node.state = node.snapshot.state;
        node.last_applied = node.snapshot.last_included_index;
        node.log.clear();
        node.log.reserve(persisted->wal_entries.size());
        for (const auto& entry : persisted->wal_entries) {
            node.log.push_back(from_persist_log_entry(entry));
        }
        node.role = Role::Follower;
        reset_election_timer_locked(node);

        next_log_index_ = std::max(next_log_index_, last_log_index_locked(node) + 1);
        committed_index_ = std::max(committed_index_, node.commit_index);
        apply_entries_locked(node);
    }

    bool persist_node_locked(const Node& node) {
        if (!storage_) {
            return true;
        }
        std::vector<PersistLogEntry> wal;
        wal.reserve(node.log.size());
        for (const auto& entry : node.log) {
            wal.push_back(to_persist_log_entry(entry));
        }
        return storage_->save_meta(node.id, to_persist_meta(node))
            && storage_->save_snapshot(node.id, to_persist_snapshot(node.snapshot))
            && storage_->rewrite_wal(node.id, wal);
    }
// 将所有节点状态持久化到存储中（如果启用），通常在关键状态变更后调用以确保数据安全。
    void persist_all_nodes_locked() {
        if (!storage_) {
            return;
        }
        for (const auto& [id, node] : nodes_) {
            (void)id;
            (void)persist_node_locked(node);
        }
    }
// 处理 RequestVote RPC 请求，主要用于 Leader 选举过程。函数会验证请求的合法性，更新节点状态，并返回响应结果。
    RequestVoteResponse handle_request_vote_rpc(const std::string& target_node_id, const RequestVoteRequest& request) {
        std::lock_guard<std::mutex> lock(mutex_);

        RequestVoteResponse response;
        auto it = nodes_.find(target_node_id);
        if (it == nodes_.end()) {
            return response;
        }

        Node& voter = it->second;
        response.term = voter.current_term;
        if (!voter.alive || !voter.voter) {
            return response;
        }

        if (request.term < voter.current_term) {
            return response;
        }
        if (request.term > voter.current_term) {
            voter.current_term = request.term;
            voter.voted_for.clear();
            voter.role = Role::Follower;
        }

        const std::uint64_t voter_last_term = last_log_term_locked(voter);
        const std::uint64_t voter_last_index = last_log_index_locked(voter);
        const bool candidate_up_to_date =
            (request.last_log_term > voter_last_term)
            || (request.last_log_term == voter_last_term && request.last_log_index >= voter_last_index);

        if (!candidate_up_to_date) {
            response.term = voter.current_term;
            return response;
        }

        if (!voter.voted_for.empty() && voter.voted_for != request.candidate_id) {
            response.term = voter.current_term;
            return response;
        }

        voter.voted_for = request.candidate_id;
        reset_election_timer_locked(voter);
        response.term = voter.current_term;
        response.vote_granted = true;
        (void)persist_node_locked(voter);
        return response;
    }
// 处理 AppendEntries RPC 请求，主要用于日志复制和心跳机制。函数会验证请求的合法性，更新节点状态，并返回响应结果。
    AppendEntriesResponse handle_append_entries_rpc(
        const std::string& target_node_id,
        const AppendEntriesRequest& request
    ) {
        std::lock_guard<std::mutex> lock(mutex_);

        AppendEntriesResponse response;
        auto it = nodes_.find(target_node_id);
        if (it == nodes_.end()) {
            return response;
        }
        Node& follower = it->second;
        response.term = follower.current_term;
        response.match_index = last_log_index_locked(follower);

        if (!follower.alive) {
            return response;
        }
        if (request.term < follower.current_term) {
            return response;
        }

        if (request.term > follower.current_term) {
            follower.current_term = request.term;
            follower.voted_for.clear();
        }
        follower.role = Role::Follower;
        leader_id_ = request.leader_id;
        reset_election_timer_locked(follower);

        if (request.prev_log_index > last_log_index_locked(follower)) {
            response.term = follower.current_term;
            return response;
        }
        if (request.prev_log_index > 0
            && term_at_locked(follower, request.prev_log_index) != request.prev_log_term) {
            response.term = follower.current_term;
            return response;
        }

        for (const auto& persisted_entry : request.entries) {
            const LogEntry incoming = from_persist_log_entry(persisted_entry);
            const LogEntry* existing = find_entry_locked(follower, incoming.index);
            if (existing != nullptr && existing->term != incoming.term) {
                const auto erase_it = std::find_if(
                    follower.log.begin(),
                    follower.log.end(),
                    [incoming](const LogEntry& entry) { return entry.index >= incoming.index; }
                );
                follower.log.erase(erase_it, follower.log.end());
            }

            if (incoming.index > follower.snapshot.last_included_index
                && find_entry_locked(follower, incoming.index) == nullptr) {
                follower.log.push_back(incoming);
            }
        }

        if (request.leader_commit > follower.commit_index) {
            follower.commit_index = std::min(request.leader_commit, last_log_index_locked(follower));
            apply_entries_locked(follower);
        }

        response.term = follower.current_term;
        response.success = true;
        response.match_index = last_log_index_locked(follower);
        (void)persist_node_locked(follower);
        return response;
    }

    InstallSnapshotResponse handle_install_snapshot_rpc(
        const std::string& target_node_id,
        const InstallSnapshotRequest& request
    ) {
        std::lock_guard<std::mutex> lock(mutex_);

        InstallSnapshotResponse response;
        auto it = nodes_.find(target_node_id);
        if (it == nodes_.end()) {
            return response;
        }
        Node& follower = it->second;
        response.term = follower.current_term;

        if (!follower.alive) {
            return response;
        }
        if (request.term < follower.current_term) {
            return response;
        }

        if (request.term > follower.current_term) {
            follower.current_term = request.term;
            follower.voted_for.clear();
        }
        follower.role = Role::Follower;
        leader_id_ = request.leader_id;
        reset_election_timer_locked(follower);

        const Snapshot incoming = from_persist_snapshot(request.snapshot);
        if (incoming.last_included_index >= follower.snapshot.last_included_index) {
            follower.snapshot = incoming;
            follower.state = incoming.state;
            follower.commit_index = std::max(follower.commit_index, incoming.last_included_index);
            follower.last_applied = std::max(follower.last_applied, incoming.last_included_index);
            trim_log_prefix_locked(follower, incoming.last_included_index);
            apply_entries_locked(follower);
        }

        response.term = follower.current_term;
        response.success = true;
        (void)persist_node_locked(follower);
        return response;
    }

    template <typename Fn>
    auto run_on_scheduler(Fn&& fn) -> std::invoke_result_t<Fn> {
        using ReturnT = std::invoke_result_t<Fn>;
        const bool scheduler_ready =
            options_.scheduler != nullptr
            && options_.scheduler->running()
            && rpc::runtime::Coroutine::current() == nullptr;

        if (!scheduler_ready) {
            if constexpr (std::is_void<ReturnT>::value) {
                fn();
                flush_pending_rpc_calls();
                return;
            } else {
                ReturnT value = fn();
                flush_pending_rpc_calls();
                return value;
            }
        }

        auto task = std::make_shared<std::packaged_task<ReturnT()>>(std::forward<Fn>(fn));
        auto future = task->get_future();

        try {
            options_.scheduler->schedule([task]() { (*task)(); });
            scheduler_dispatches_.fetch_add(1, std::memory_order_release);
        } catch (...) {
            (*task)();
            if constexpr (std::is_void<ReturnT>::value) {
                future.get();
                flush_pending_rpc_calls();
                return;
            } else {
                ReturnT value = future.get();
                flush_pending_rpc_calls();
                return value;
            }
        }

        if constexpr (std::is_void<ReturnT>::value) {
            future.get();
            flush_pending_rpc_calls();
            return;
        } else {
            ReturnT value = future.get();
            flush_pending_rpc_calls();
            return value;
        }
    }

    void reset_state_locked() {
        stop_rpc_servers_locked();
        nodes_.clear();
        node_order_.clear();
        current_voters_.clear();
        joint_ = JointConfig{};
        leader_id_.clear();
        next_log_index_ = 1;
        committed_index_ = 0;
        cluster_applied_index_ = 0;
        leader_changes_ = 0;
        snapshot_count_ = 0;
        replication_delayed_ = 0;
        replication_dropped_ = 0;
        rpc_mirror_calls_.store(0, std::memory_order_release);
        pending_replications_.clear();
    }
//显示节点并确保其存在，如果不存在则创建一个新的节点    
    Node& ensure_node_locked(const std::string& id) {
        auto it = nodes_.find(id);
        if (it != nodes_.end()) {
            return it->second;
        }

        Node node;
        node.id = id;
        reset_election_timer_locked(node);

        nodes_.emplace(id, std::move(node));
        node_order_.push_back(id);
        Node& created = nodes_.at(id);
        restore_node_from_storage_locked(created);
        ensure_rpc_server_for_node_locked(id);
        return created;
    }

    std::size_t random_election_timeout_locked() {
        std::uniform_int_distribution<std::size_t> dist(
            options_.election_timeout_ticks_min,
            options_.election_timeout_ticks_max
        );
        return dist(rng_);
    }

    void reset_election_timer_locked(Node& node) {
        node.election_elapsed = 0;
        node.election_timeout = random_election_timeout_locked();
    }

    bool has_live_leader_locked() const {
        if (leader_id_.empty()) {
            return false;
        }
        const auto it = nodes_.find(leader_id_);
        if (it == nodes_.end()) {
            return false;
        }
        return it->second.alive && it->second.role == Role::Leader && it->second.voter;
    }

    std::uint64_t last_log_index_locked(const Node& node) const {
        if (!node.log.empty()) {
            return node.log.back().index;
        }
        return node.snapshot.last_included_index;
    }

    std::uint64_t last_log_term_locked(const Node& node) const {
        if (!node.log.empty()) {
            return node.log.back().term;
        }
        return node.snapshot.last_included_term;
    }

    std::uint64_t term_at_locked(const Node& node, std::uint64_t index) const {
        if (index == 0) {
            return 0;
        }
        if (index == node.snapshot.last_included_index) {
            return node.snapshot.last_included_term;
        }
        const LogEntry* entry = find_entry_locked(node, index);
        if (entry == nullptr) {
            return 0;
        }
        return entry->term;
    }

    const LogEntry* find_entry_locked(const Node& node, std::uint64_t index) const {
        if (index <= node.snapshot.last_included_index || node.log.empty()) {
            return nullptr;
        }

        const std::uint64_t first = node.snapshot.last_included_index + 1;
        if (index >= first) {
            const std::uint64_t offset = index - first;
            if (offset < node.log.size()) {
                const LogEntry& entry = node.log[static_cast<std::size_t>(offset)];
                if (entry.index == index) {
                    return &entry;
                }
            }
        }

        for (const auto& entry : node.log) {
            if (entry.index == index) {
                return &entry;
            }
        }
        return nullptr;
    }

    static bool majority(std::size_t granted, std::size_t total) {
        return total > 0 && granted > total / 2;
    }

    boost::container::flat_set<std::string> effective_voters_locked() const {
        if (!joint_.active) {
            return current_voters_;
        }

        boost::container::flat_set<std::string> merged = joint_.old_voters;
        merged.insert(joint_.new_voters.begin(), joint_.new_voters.end());
        return merged;
    }

    void update_node_voter_flags_locked() {
        const boost::container::flat_set<std::string> voters = effective_voters_locked();
        for (auto& [id, node] : nodes_) {
            (void)id;
            node.voter = voters.find(node.id) != voters.end();
        }
    }

    bool has_election_quorum_locked(const boost::container::flat_set<std::string>& granted) const {
        if (!joint_.active) {
            std::size_t granted_current = 0;
            for (const auto& id : current_voters_) {
                if (granted.find(id) != granted.end()) {
                    ++granted_current;
                }
            }
            return majority(granted_current, current_voters_.size());
        }

        std::size_t granted_old = 0;
        std::size_t granted_new = 0;
        for (const auto& id : joint_.old_voters) {
            if (granted.find(id) != granted.end()) {
                ++granted_old;
            }
        }
        for (const auto& id : joint_.new_voters) {
            if (granted.find(id) != granted.end()) {
                ++granted_new;
            }
        }

        return majority(granted_old, joint_.old_voters.size())
            && majority(granted_new, joint_.new_voters.size());
    }

    bool node_has_entry_locked(const std::string& node_id, std::uint64_t index) const {
        const auto it = nodes_.find(node_id);
        if (it == nodes_.end() || !it->second.alive) {
            return false;
        }

        const Node& node = it->second;
        if (index <= node.snapshot.last_included_index) {
            return true;
        }
        return last_log_index_locked(node) >= index;
    }

    bool has_commit_quorum_locked(std::uint64_t index) const {
        if (!joint_.active) {
            std::size_t replicated = 0;
            for (const auto& id : current_voters_) {
                if (node_has_entry_locked(id, index)) {
                    ++replicated;
                }
            }
            return majority(replicated, current_voters_.size());
        }

        std::size_t replicated_old = 0;
        std::size_t replicated_new = 0;
        for (const auto& id : joint_.old_voters) {
            if (node_has_entry_locked(id, index)) {
                ++replicated_old;
            }
        }
        for (const auto& id : joint_.new_voters) {
            if (node_has_entry_locked(id, index)) {
                ++replicated_new;
            }
        }

        return majority(replicated_old, joint_.old_voters.size())
            && majority(replicated_new, joint_.new_voters.size());
    }

    static bool log_up_to_date_locked(const Node& candidate, const Node& voter, const Impl* impl) {
        const std::uint64_t c_term = impl->last_log_term_locked(candidate);
        const std::uint64_t v_term = impl->last_log_term_locked(voter);
        if (c_term != v_term) {
            return c_term > v_term;
        }
        return impl->last_log_index_locked(candidate) >= impl->last_log_index_locked(voter);
    }

    void start_election_locked(const std::string& candidate_id) {
        auto candidate_it = nodes_.find(candidate_id);
        if (candidate_it == nodes_.end()) {
            return;
        }

        Node& candidate = candidate_it->second;
        if (!candidate.alive || !candidate.voter) {
            return;
        }

        leader_id_.clear();
        candidate.role = Role::Candidate;
        ++candidate.current_term;
        candidate.voted_for = candidate.id;
        reset_election_timer_locked(candidate);

        const std::uint64_t election_term = candidate.current_term;
        boost::container::flat_set<std::string> granted_votes;
        granted_votes.insert(candidate.id);

        const boost::container::flat_set<std::string> voters = effective_voters_locked();
        RequestVoteRequest vote_request;
        vote_request.term = election_term;
        vote_request.candidate_id = candidate.id;
        vote_request.last_log_index = last_log_index_locked(candidate);
        vote_request.last_log_term = last_log_term_locked(candidate);
        for (const auto& voter_id : voters) {
            if (voter_id == candidate.id) {
                continue;
            }

            auto voter_it = nodes_.find(voter_id);
            if (voter_it == nodes_.end()) {
                continue;
            }

            Node& voter = voter_it->second;
            if (!voter.alive || !voter.voter) {
                continue;
            }

            if (options_.enable_rpc_transport) {
                queue_request_vote_rpc_mirror_locked(voter_id, vote_request);
            }

            if (voter.current_term > election_term) {
                candidate.current_term = voter.current_term;
                candidate.role = Role::Follower;
                candidate.voted_for.clear();
                return;
            }

            if (voter.current_term < election_term) {
                voter.current_term = election_term;
                voter.voted_for.clear();
                voter.role = Role::Follower;
            }

            if (!voter.voted_for.empty() && voter.voted_for != candidate.id) {
                continue;
            }
            if (!log_up_to_date_locked(candidate, voter, this)) {
                continue;
            }

            voter.voted_for = candidate.id;
            reset_election_timer_locked(voter);
            granted_votes.insert(voter_id);
        }

        if (!has_election_quorum_locked(granted_votes)) {
            return;
        }

        for (auto& [id, node] : nodes_) {
            (void)id;
            if (node.id == candidate.id) {
                continue;
            }
            node.role = Role::Follower;
        }

        candidate.role = Role::Leader;
        candidate.heartbeat_elapsed = 0;
        leader_id_ = candidate.id;
        ++leader_changes_;

        replicate_from_leader_locked();
        advance_commit_locked();
    }

    void trim_log_prefix_locked(Node& node, std::uint64_t upto_index) {
        if (node.log.empty()) {
            return;
        }
        const auto it = std::find_if(
            node.log.begin(),
            node.log.end(),
            [upto_index](const LogEntry& entry) { return entry.index > upto_index; }
        );
        node.log.erase(node.log.begin(), it);
    }

    bool should_drop_replication_locked() {
        if (fault_options_.packet_loss_percent <= 0.0) {
            return false;
        }
        std::uniform_real_distribution<double> dist(0.0, 100.0);
        return dist(rng_) < fault_options_.packet_loss_percent;
    }

    std::size_t replication_delay_ticks_locked() {
        if (fault_options_.replicate_delay_ticks_max == 0) {
            return 0;
        }
        std::uniform_int_distribution<std::size_t> dist(
            fault_options_.replicate_delay_ticks_min,
            fault_options_.replicate_delay_ticks_max
        );
        return dist(rng_);
    }

    bool apply_replication_payload_to_follower_locked(const std::string& follower_id, const ReplicationPayload& payload) {
        auto follower_it = nodes_.find(follower_id);
        if (follower_it == nodes_.end()) {
            return false;
        }

        Node& follower = follower_it->second;
        if (!follower.alive || payload.leader_term < follower.current_term) {
            return false;
        }

        if (follower.current_term < payload.leader_term) {
            follower.current_term = payload.leader_term;
            follower.voted_for.clear();
        }
        follower.role = Role::Follower;
        reset_election_timer_locked(follower);

        if (follower.snapshot.last_included_index < payload.snapshot.last_included_index) {
            follower.snapshot = payload.snapshot;
            follower.state = payload.snapshot.state;
            follower.last_applied = std::max(follower.last_applied, follower.snapshot.last_included_index);
            follower.commit_index = std::max(follower.commit_index, follower.snapshot.last_included_index);
            trim_log_prefix_locked(follower, follower.snapshot.last_included_index);
        }

        follower.log = payload.log;
        follower.commit_index = std::min(payload.leader_commit, last_log_index_locked(follower));
        apply_entries_locked(follower);

        if (options_.enable_rpc_transport) {
            AppendEntriesRequest append_request;
            append_request.term = payload.leader_term;
            append_request.leader_id = payload.leader_id;
            append_request.prev_log_index = payload.snapshot.last_included_index;
            append_request.prev_log_term = payload.snapshot.last_included_term;
            append_request.entries = payload.persisted_entries;
            append_request.leader_commit = payload.leader_commit;
            queue_append_entries_rpc_mirror_locked(follower_id, append_request);

            if (payload.snapshot.last_included_index > 0) {
                InstallSnapshotRequest snapshot_request;
                snapshot_request.term = payload.leader_term;
                snapshot_request.leader_id = payload.leader_id;
                snapshot_request.snapshot = to_persist_snapshot(payload.snapshot);
                queue_install_snapshot_rpc_mirror_locked(follower_id, snapshot_request);
            }
        }
        return true;
    }

    void process_pending_replications_locked() {
        if (pending_replications_.empty()) {
            return;
        }

        std::deque<PendingReplication> remaining;
        while (!pending_replications_.empty()) {
            PendingReplication pending = std::move(pending_replications_.front());
            pending_replications_.pop_front();
            if (pending.remaining_ticks > 0) {
                --pending.remaining_ticks;
            }
            if (pending.remaining_ticks > 0 || !pending.payload) {
                remaining.push_back(std::move(pending));
                continue;
            }

            if (!apply_replication_payload_to_follower_locked(pending.follower_id, *pending.payload)) {
                continue;
            }
        }
        pending_replications_.swap(remaining);
    }

    void replicate_from_leader_locked() {
        auto leader_it = nodes_.find(leader_id_);
        if (leader_it == nodes_.end()) {
            leader_id_.clear();
            return;
        }

        Node& leader = leader_it->second;
        if (!leader.alive || leader.role != Role::Leader || !leader.voter) {
            leader_id_.clear();
            return;
        }

        auto payload = std::make_shared<ReplicationPayload>();
        payload->leader_id = leader.id;
        payload->leader_term = leader.current_term;
        payload->leader_commit = leader.commit_index;
        payload->snapshot = leader.snapshot;
        payload->log = leader.log;
        payload->persisted_entries.reserve(leader.log.size());
        for (const auto& entry : leader.log) {
            payload->persisted_entries.push_back(to_persist_log_entry(entry));
        }

        for (const auto& node_id : node_order_) {
            if (node_id == leader_id_) {
                continue;
            }

            auto follower_it = nodes_.find(node_id);
            if (follower_it == nodes_.end()) {
                continue;
            }

            Node& follower = follower_it->second;
            if (!follower.alive) {
                continue;
            }

            if (should_drop_replication_locked()) {
                ++replication_dropped_;
                continue;
            }

            const std::size_t delay_ticks = replication_delay_ticks_locked();
            if (delay_ticks > 0) {
                pending_replications_.push_back(PendingReplication{node_id, payload, delay_ticks});
                ++replication_delayed_;
                continue;
            }

            (void)apply_replication_payload_to_follower_locked(node_id, *payload);
        }
    }

    void apply_command_locked(MetadataState& state, const Command& command) {
        switch (command.type) {
            case CommandType::UpsertConfig: {
                state.configs[command.key] = command.value;
                return;
            }
            case CommandType::EraseConfig: {
                state.configs.erase(command.key);
                return;
            }
            case CommandType::RegisterService: {
                auto& instances = state.services[command.service];
                auto it = std::find_if(
                    instances.begin(),
                    instances.end(),
                    [&command](const ServiceInstance& instance) {
                        return instance.id == command.instance.id;
                    }
                );
                if (it == instances.end()) {
                    instances.push_back(command.instance);
                } else {
                    *it = command.instance;
                }
                return;
            }
            case CommandType::UnregisterService: {
                auto service_it = state.services.find(command.service);
                if (service_it == state.services.end()) {
                    return;
                }
                auto& instances = service_it->second;
                instances.erase(
                    std::remove_if(
                        instances.begin(),
                        instances.end(),
                        [&command](const ServiceInstance& instance) {
                            return instance.id == command.instance.id;
                        }
                    ),
                    instances.end()
                );
                if (instances.empty()) {
                    state.services.erase(service_it);
                }
                return;
            }
            case CommandType::BeginJointConsensus:
            case CommandType::FinalizeJointConsensus:
                return;
        }
    }

    void apply_cluster_command_locked(const LogEntry& entry) {
        if (entry.index <= cluster_applied_index_) {
            return;
        }
        cluster_applied_index_ = entry.index;

        if (entry.command.type == CommandType::BeginJointConsensus) {
            joint_.active = true;
            joint_.old_voters = current_voters_;
            joint_.new_voters = entry.command.voters;

            for (const auto& id : joint_.old_voters) {
                (void)ensure_node_locked(id);
            }
            for (const auto& id : joint_.new_voters) {
                (void)ensure_node_locked(id);
            }
            update_node_voter_flags_locked();
            return;
        }

        if (entry.command.type == CommandType::FinalizeJointConsensus) {
            current_voters_ = entry.command.voters;
            for (const auto& id : current_voters_) {
                (void)ensure_node_locked(id);
            }
            joint_ = JointConfig{};
            update_node_voter_flags_locked();

            if (!leader_id_.empty()) {
                auto leader_it = nodes_.find(leader_id_);
                if (leader_it != nodes_.end() && !leader_it->second.voter) {
                    leader_it->second.role = Role::Follower;
                    leader_id_.clear();
                }
            }
        }
    }

    void apply_entries_locked(Node& node) {
        if (node.last_applied < node.snapshot.last_included_index) {
            node.state = node.snapshot.state;
            node.last_applied = node.snapshot.last_included_index;
        }

        while (node.last_applied < node.commit_index) {
            const std::uint64_t next_index = node.last_applied + 1;
            const LogEntry* entry = find_entry_locked(node, next_index);
            if (entry != nullptr) {
                apply_command_locked(node.state, entry->command);
                if (node.id == leader_id_) {
                    apply_cluster_command_locked(*entry);
                }
            }
            node.last_applied = next_index;
        }
    }

    void maybe_snapshot_leader_locked() {
        auto leader_it = nodes_.find(leader_id_);
        if (leader_it == nodes_.end()) {
            return;
        }

        Node& leader = leader_it->second;
        if (leader.commit_index == 0 || leader.commit_index <= leader.snapshot.last_included_index) {
            return;
        }

        if (leader.commit_index - leader.snapshot.last_included_index < options_.snapshot_log_threshold) {
            return;
        }

        leader.snapshot.last_included_index = leader.commit_index;
        leader.snapshot.last_included_term = term_at_locked(leader, leader.commit_index);
        leader.snapshot.state = leader.state;
        trim_log_prefix_locked(leader, leader.snapshot.last_included_index);
        ++snapshot_count_;
    }

    void advance_commit_locked() {
        auto leader_it = nodes_.find(leader_id_);
        if (leader_it == nodes_.end()) {
            return;
        }

        Node& leader = leader_it->second;
        const std::uint64_t last_index = last_log_index_locked(leader);
        std::uint64_t target_commit = leader.commit_index;

        for (std::uint64_t index = last_index; index > leader.commit_index; --index) {
            const LogEntry* entry = find_entry_locked(leader, index);
            if (entry == nullptr || entry->term != leader.current_term) {
                continue;
            }
            if (has_commit_quorum_locked(index)) {
                target_commit = index;
                break;
            }
        }

        if (target_commit == leader.commit_index) {
            return;
        }

        leader.commit_index = target_commit;
        committed_index_ = target_commit;

        for (auto& [id, node] : nodes_) {
            (void)id;
            if (!node.alive) {
                continue;
            }
            node.commit_index = std::min(target_commit, last_log_index_locked(node));
            apply_entries_locked(node);
        }

        maybe_snapshot_leader_locked();
        replicate_from_leader_locked();
    }

    bool ensure_leader_locked(std::size_t max_ticks) {
        if (has_live_leader_locked()) {
            return true;
        }
        for (std::size_t i = 0; i < max_ticks; ++i) {
            tick_locked();
            if (has_live_leader_locked()) {
                return true;
            }
        }
        return false;
    }

    bool append_and_commit_locked(const Command& command) {
        const std::size_t convergence_ticks = options_.election_timeout_ticks_max * 2;
        if (!ensure_leader_locked(convergence_ticks)) {
            persist_all_nodes_locked();
            return false;
        }

        auto leader_it = nodes_.find(leader_id_);
        if (leader_it == nodes_.end() || !leader_it->second.alive || leader_it->second.role != Role::Leader) {
            persist_all_nodes_locked();
            return false;
        }

        Node& leader = leader_it->second;

        LogEntry entry;
        entry.index = next_log_index_;
        entry.term = leader.current_term;
        entry.command = command;
        ++next_log_index_;

        leader.log.push_back(entry);

        replicate_from_leader_locked();
        advance_commit_locked();

        if (committed_index_ >= entry.index) {
            persist_all_nodes_locked();
            return true;
        }

        for (std::size_t i = 0; i < convergence_ticks; ++i) {
            tick_locked();
            if (committed_index_ >= entry.index) {
                persist_all_nodes_locked();
                return true;
            }
        }
        persist_all_nodes_locked();
        return false;
    }

    bool append_and_commit(const Command& command) {
        return run_on_scheduler([this, command]() {
            std::lock_guard<std::mutex> lock(mutex_);
            return append_and_commit_locked(command);
        });
    }

    void tick_locked() {
        if (nodes_.empty()) {
            return;
        }

        process_pending_replications_locked();

        auto leader_it = nodes_.find(leader_id_);
        if (leader_it == nodes_.end() || !leader_it->second.alive || leader_it->second.role != Role::Leader || !leader_it->second.voter) {
            leader_id_.clear();
        }

        if (!leader_id_.empty()) {
            for (const auto& id : node_order_) {
                auto it = nodes_.find(id);
                if (it == nodes_.end() || !it->second.alive || !it->second.voter || id == leader_id_) {
                    continue;
                }
                ++it->second.election_elapsed;
                if (it->second.election_elapsed >= it->second.election_timeout) {
                    start_election_locked(id);
                    break;
                }
            }

            auto stable_leader = nodes_.find(leader_id_);
            if (stable_leader != nodes_.end() && stable_leader->second.alive && stable_leader->second.role == Role::Leader) {
                ++stable_leader->second.heartbeat_elapsed;
                if (stable_leader->second.heartbeat_elapsed >= options_.heartbeat_interval_ticks) {
                    stable_leader->second.heartbeat_elapsed = 0;
                    replicate_from_leader_locked();
                    advance_commit_locked();
                    process_pending_replications_locked();
                }
            }
            return;
        }

        for (const auto& id : node_order_) {
            auto it = nodes_.find(id);
            if (it == nodes_.end()) {
                continue;
            }
            Node& node = it->second;
            if (!node.alive || !node.voter) {
                continue;
            }
            ++node.election_elapsed;
            if (node.election_elapsed >= node.election_timeout) {
                start_election_locked(id);
                if (has_live_leader_locked()) {
                    break;
                }
            }
        }
    }

    static std::vector<ServiceInstance> normalize_instances(const std::vector<ServiceInstance>& instances) {
        std::vector<ServiceInstance> sorted = instances;
        std::sort(
            sorted.begin(),
            sorted.end(),
            [](const ServiceInstance& lhs, const ServiceInstance& rhs) {
                return std::tie(lhs.id, lhs.host, lhs.port) < std::tie(rhs.id, rhs.host, rhs.port);
            }
        );
        return sorted;
    }

    static bool metadata_equal_locked(const MetadataState& lhs, const MetadataState& rhs) {
        if (lhs.configs != rhs.configs || lhs.services.size() != rhs.services.size()) {
            return false;
        }

        for (const auto& [service, lhs_instances] : lhs.services) {
            const auto it = rhs.services.find(service);
            if (it == rhs.services.end()) {
                return false;
            }
            const std::vector<ServiceInstance> left = normalize_instances(lhs_instances);
            const std::vector<ServiceInstance> right = normalize_instances(it->second);
            if (left.size() != right.size()) {
                return false;
            }
            for (std::size_t i = 0; i < left.size(); ++i) {
                if (left[i].id != right[i].id
                    || left[i].host != right[i].host
                    || left[i].port != right[i].port) {
                    return false;
                }
            }
        }
        return true;
    }

    const Node* select_reader_locked() const {
        auto leader_it = nodes_.find(leader_id_);
        if (leader_it != nodes_.end() && leader_it->second.alive && leader_it->second.role == Role::Leader) {
            return &leader_it->second;
        }

        const Node* best = nullptr;
        for (const auto& id : node_order_) {
            const auto it = nodes_.find(id);
            if (it == nodes_.end() || !it->second.alive) {
                continue;
            }

            const Node& node = it->second;
            if (best == nullptr || node.commit_index > best->commit_index) {
                best = &node;
            }
        }
        return best;
    }

private:
    RaftOptions options_;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Node> nodes_;
    std::vector<std::string> node_order_;

    boost::container::flat_set<std::string> current_voters_;
    JointConfig joint_;

    std::string leader_id_;
    std::uint64_t next_log_index_{1};
    std::uint64_t committed_index_{0};
    std::uint64_t cluster_applied_index_{0};

    std::size_t leader_changes_{0};
    std::size_t snapshot_count_{0};
    std::size_t replication_delayed_{0};
    std::size_t replication_dropped_{0};
    std::mt19937 rng_;
    RaftFaultInjectionOptions fault_options_{};
    std::deque<PendingReplication> pending_replications_;

    std::atomic<std::size_t> scheduler_dispatches_{0};
    std::atomic<std::size_t> rpc_mirror_calls_{0};

    std::unique_ptr<RaftStorage> storage_;
    RaftRpcClient rpc_client_;

    std::unordered_map<std::string, std::uint16_t> node_ports_;
    std::unordered_map<std::string, std::shared_ptr<IRaftRpcHandler>> rpc_handlers_;
    std::unordered_map<std::string, std::unique_ptr<RaftRpcServer>> rpc_servers_;
    std::vector<std::function<void()>> pending_rpc_calls_;
};

RaftCluster::RaftCluster(RaftOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

RaftCluster::~RaftCluster() = default;

void RaftCluster::bootstrap(const std::vector<std::string>& initial_voters) {
    impl_->bootstrap(initial_voters);
}

void RaftCluster::tick() {
    impl_->tick();
}

void RaftCluster::run_ticks(std::size_t ticks) {
    impl_->run_ticks(ticks);
}

bool RaftCluster::wait_for_leader(std::size_t max_ticks) {
    return impl_->wait_for_leader(max_ticks);
}

bool RaftCluster::set_node_available(const std::string& node_id, bool available) {
    return impl_->set_node_available(node_id, available);
}

bool RaftCluster::upsert_config(std::string key, std::string value) {
    return impl_->upsert_config(std::move(key), std::move(value));
}

bool RaftCluster::erase_config(const std::string& key) {
    return impl_->erase_config(key);
}

bool RaftCluster::register_service(std::string service, ServiceInstance instance) {
    return impl_->register_service(std::move(service), std::move(instance));
}

bool RaftCluster::unregister_service(std::string service, std::string instance_id) {
    return impl_->unregister_service(service, instance_id);
}

std::optional<std::string> RaftCluster::config_value(const std::string& key) const {
    return impl_->config_value(key);
}

std::unordered_map<std::string, std::string> RaftCluster::config_snapshot() const {
    return impl_->config_snapshot();
}

std::vector<ServiceInstance> RaftCluster::discover_service(const std::string& service) const {
    return impl_->discover_service(service);
}

bool RaftCluster::begin_joint_consensus(const std::vector<std::string>& new_voters) {
    return impl_->begin_joint_consensus(new_voters);
}

bool RaftCluster::finalize_joint_consensus() {
    return impl_->finalize_joint_consensus();
}

bool RaftCluster::reconfigure_joint(const std::vector<std::string>& new_voters, std::size_t max_ticks) {
    return impl_->reconfigure_joint(new_voters, max_ticks);
}

void RaftCluster::configure_fault_injection(RaftFaultInjectionOptions options) {
    impl_->configure_fault_injection(std::move(options));
}

RaftFaultInjectionOptions RaftCluster::fault_injection_options() const {
    return impl_->fault_injection_options();
}

bool RaftCluster::metadata_consistent() const {
    return impl_->metadata_consistent();
}

RaftStatus RaftCluster::status() const {
    return impl_->status();
}

std::optional<std::string> RaftCluster::leader() const {
    return impl_->leader();
}

std::optional<std::string> RaftCluster::node_endpoint(const std::string& node_id) const {
    return impl_->node_endpoint(node_id);
}

RaftServiceDiscovery::RaftServiceDiscovery(std::shared_ptr<RaftCluster> cluster)
    : cluster_(std::move(cluster)) {}

std::vector<rpc::client::ServiceNode> RaftServiceDiscovery::list_nodes(const std::string& service_name) {
    if (!cluster_) {
        return {};
    }

    const std::vector<ServiceInstance> instances = cluster_->discover_service(service_name);
    std::vector<rpc::client::ServiceNode> nodes;
    nodes.reserve(instances.size());
    for (const auto& instance : instances) {
        rpc::client::ServiceNode node;
        node.id = instance.id;
        node.host = instance.host;
        node.port = instance.port;
        nodes.push_back(std::move(node));
    }
    return nodes;
}

RaftConfigCenterClient::RaftConfigCenterClient(std::shared_ptr<RaftCluster> cluster)
    : cluster_(std::move(cluster)) {}

std::optional<rpc::infra::ConfigSnapshot> RaftConfigCenterClient::fetch_latest() {
    if (!cluster_) {
        return std::nullopt;
    }

    rpc::infra::ConfigSnapshot snapshot;
    snapshot.values = cluster_->config_snapshot();
    snapshot.version = cluster_->status().committed_index;
    if (snapshot.version == 0 && snapshot.values.empty()) {
        return std::nullopt;
    }
    return snapshot;
}

}  // namespace rpc::infra::raft
