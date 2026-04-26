#pragma once

// 文件用途：
// 提供 Raft 持久化能力：
// 1) WAL 顺序追加与重写
// 2) 快照落盘与加载
// 3) 节点元信息持久化（term/vote/commit/applied）

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace rpc::infra::raft {

enum class PersistCommandType : std::uint8_t {
    UpsertConfig = 1,
    EraseConfig = 2,
    RegisterService = 3,
    UnregisterService = 4,
    BeginJointConsensus = 5,
    FinalizeJointConsensus = 6,
};

struct PersistServiceInstance {
    std::string id;
    std::string host;
    std::uint16_t port{0};
};

struct PersistCommand {
    PersistCommandType type{PersistCommandType::UpsertConfig};
    std::string key;
    std::string value;
    std::string service;
    PersistServiceInstance instance;
    std::vector<std::string> voters;
};

struct PersistLogEntry {
    std::uint64_t index{0};
    std::uint64_t term{0};
    PersistCommand command;
};

struct PersistMetadataState {
    std::unordered_map<std::string, std::string> configs;
    std::unordered_map<std::string, std::vector<PersistServiceInstance>> services;
};

struct PersistSnapshot {
    std::uint64_t last_included_index{0};
    std::uint64_t last_included_term{0};
    PersistMetadataState state;
};

struct PersistNodeMeta {
    std::string node_id;
    std::uint64_t current_term{0};
    std::string voted_for;
    std::uint64_t commit_index{0};
    std::uint64_t last_applied{0};
};

struct PersistNodeState {
    PersistNodeMeta meta;
    PersistSnapshot snapshot;
    std::vector<PersistLogEntry> wal_entries;
};

class RaftStorage {
public:
    explicit RaftStorage(std::string root_dir);

    bool append_wal(const std::string& node_id, const PersistLogEntry& entry);
    bool rewrite_wal(const std::string& node_id, const std::vector<PersistLogEntry>& entries);
    bool save_snapshot(const std::string& node_id, const PersistSnapshot& snapshot);
    bool save_meta(const std::string& node_id, const PersistNodeMeta& meta);

    std::optional<PersistNodeState> load_node_state(const std::string& node_id) const;

private:
    std::string root_dir_;
};

}  // namespace rpc::infra::raft

