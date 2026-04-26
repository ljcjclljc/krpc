#include "rpc/infra/raft/raft_storage.h"

// 文件用途：
// Raft 持久化实现：
// - WAL 追加/重写
// - Snapshot 落盘/加载
// - Meta 落盘/加载

#include <arpa/inet.h>

#include <filesystem>
#include <fstream>
#include <system_error>
#include <utility>

#include "raft.pb.h"

namespace rpc::infra::raft {

namespace {

namespace fs = std::filesystem;

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

void to_pb_meta(const PersistNodeMeta& source, rpc::raft::NodeMeta* target) {
    if (target == nullptr) {
        return;
    }
    target->set_node_id(source.node_id);
    target->set_current_term(source.current_term);
    target->set_voted_for(source.voted_for);
    target->set_commit_index(source.commit_index);
    target->set_last_applied(source.last_applied);
}

PersistNodeMeta from_pb_meta(const rpc::raft::NodeMeta& source) {
    PersistNodeMeta result;
    result.node_id = source.node_id();
    result.current_term = source.current_term();
    result.voted_for = source.voted_for();
    result.commit_index = source.commit_index();
    result.last_applied = source.last_applied();
    return result;
}

fs::path node_dir(const std::string& root_dir, const std::string& node_id) {
    return fs::path(root_dir) / node_id;
}

fs::path wal_path(const std::string& root_dir, const std::string& node_id) {
    return node_dir(root_dir, node_id) / "wal.bin";
}

fs::path snapshot_path(const std::string& root_dir, const std::string& node_id) {
    return node_dir(root_dir, node_id) / "snapshot.pb";
}

fs::path meta_path(const std::string& root_dir, const std::string& node_id) {
    return node_dir(root_dir, node_id) / "meta.pb";
}

bool ensure_node_dir(const std::string& root_dir, const std::string& node_id) {
    std::error_code ec;
    (void)fs::create_directories(node_dir(root_dir, node_id), ec);
    return !ec;
}

bool write_atomic_file(const fs::path& path, const std::string& bytes) {
    std::error_code ec;
    (void)fs::create_directories(path.parent_path(), ec);
    if (ec) {
        return false;
    }

    const fs::path tmp_path = path.string() + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            return false;
        }
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        if (!out.good()) {
            return false;
        }
        out.flush();
        if (!out.good()) {
            return false;
        }
    }

    fs::remove(path, ec);
    ec.clear();
    fs::rename(tmp_path, path, ec);
    if (ec) {
        return false;
    }
    return true;
}

bool read_file(const fs::path& path, std::string* out) {
    if (out == nullptr) {
        return false;
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        return false;
    }

    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    if (size < 0) {
        return false;
    }
    in.seekg(0, std::ios::beg);

    out->assign(static_cast<std::size_t>(size), '\0');
    if (size > 0) {
        in.read(out->data(), size);
        if (!in.good() && !in.eof()) {
            return false;
        }
    }
    return true;
}

}  // namespace

RaftStorage::RaftStorage(std::string root_dir)
    : root_dir_(std::move(root_dir)) {
    std::error_code ec;
    (void)fs::create_directories(fs::path(root_dir_), ec);
}

bool RaftStorage::append_wal(const std::string& node_id, const PersistLogEntry& entry) {
    if (node_id.empty() || !ensure_node_dir(root_dir_, node_id)) {
        return false;
    }

    rpc::raft::LogEntry pb_entry;
    to_pb_log_entry(entry, &pb_entry);

    std::string bytes;
    if (!pb_entry.SerializeToString(&bytes)) {
        return false;
    }

    std::ofstream out(wal_path(root_dir_, node_id), std::ios::binary | std::ios::app);
    if (!out.is_open()) {
        return false;
    }

    const std::uint32_t frame_size = static_cast<std::uint32_t>(bytes.size());
    const std::uint32_t net_frame_size = htonl(frame_size);
    out.write(reinterpret_cast<const char*>(&net_frame_size), sizeof(net_frame_size));
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
    return out.good();
}

bool RaftStorage::rewrite_wal(const std::string& node_id, const std::vector<PersistLogEntry>& entries) {
    if (node_id.empty() || !ensure_node_dir(root_dir_, node_id)) {
        return false;
    }

    std::string bytes;
    for (const auto& entry : entries) {
        rpc::raft::LogEntry pb_entry;
        to_pb_log_entry(entry, &pb_entry);

        std::string entry_bytes;
        if (!pb_entry.SerializeToString(&entry_bytes)) {
            return false;
        }

        const std::uint32_t frame_size = static_cast<std::uint32_t>(entry_bytes.size());
        const std::uint32_t net_frame_size = htonl(frame_size);

        bytes.append(
            reinterpret_cast<const char*>(&net_frame_size),
            reinterpret_cast<const char*>(&net_frame_size) + sizeof(net_frame_size)
        );
        bytes.append(entry_bytes);
    }

    return write_atomic_file(wal_path(root_dir_, node_id), bytes);
}

bool RaftStorage::save_snapshot(const std::string& node_id, const PersistSnapshot& snapshot) {
    if (node_id.empty() || !ensure_node_dir(root_dir_, node_id)) {
        return false;
    }

    rpc::raft::Snapshot pb_snapshot;
    to_pb_snapshot(snapshot, &pb_snapshot);

    std::string bytes;
    if (!pb_snapshot.SerializeToString(&bytes)) {
        return false;
    }

    return write_atomic_file(snapshot_path(root_dir_, node_id), bytes);
}

bool RaftStorage::save_meta(const std::string& node_id, const PersistNodeMeta& meta) {
    if (node_id.empty() || !ensure_node_dir(root_dir_, node_id)) {
        return false;
    }

    rpc::raft::NodeMeta pb_meta;
    to_pb_meta(meta, &pb_meta);

    std::string bytes;
    if (!pb_meta.SerializeToString(&bytes)) {
        return false;
    }

    return write_atomic_file(meta_path(root_dir_, node_id), bytes);
}

std::optional<PersistNodeState> RaftStorage::load_node_state(const std::string& node_id) const {
    if (node_id.empty()) {
        return std::nullopt;
    }

    PersistNodeState state;
    state.meta.node_id = node_id;

    bool has_any = false;

    {
        std::string meta_bytes;
        if (read_file(meta_path(root_dir_, node_id), &meta_bytes)) {
            rpc::raft::NodeMeta pb_meta;
            if (!pb_meta.ParseFromString(meta_bytes)) {
                return std::nullopt;
            }
            state.meta = from_pb_meta(pb_meta);
            has_any = true;
        }
    }

    {
        std::string snapshot_bytes;
        if (read_file(snapshot_path(root_dir_, node_id), &snapshot_bytes)) {
            if (!snapshot_bytes.empty()) {
                rpc::raft::Snapshot pb_snapshot;
                if (!pb_snapshot.ParseFromString(snapshot_bytes)) {
                    return std::nullopt;
                }
                state.snapshot = from_pb_snapshot(pb_snapshot);
            }
            has_any = true;
        }
    }

    {
        std::ifstream wal_in(wal_path(root_dir_, node_id), std::ios::binary);
        if (wal_in.is_open()) {
            while (true) {
                std::uint32_t net_frame_size = 0;
                wal_in.read(reinterpret_cast<char*>(&net_frame_size), sizeof(net_frame_size));
                if (wal_in.eof()) {
                    break;
                }
                if (!wal_in.good()) {
                    return std::nullopt;
                }

                const std::uint32_t frame_size = ntohl(net_frame_size);
                std::string frame(frame_size, '\0');
                wal_in.read(frame.data(), static_cast<std::streamsize>(frame.size()));
                if (!wal_in.good()) {
                    return std::nullopt;
                }

                rpc::raft::LogEntry pb_entry;
                if (!pb_entry.ParseFromString(frame)) {
                    return std::nullopt;
                }
                state.wal_entries.push_back(from_pb_log_entry(pb_entry));
                has_any = true;
            }
        }
    }

    if (!has_any) {
        return std::nullopt;
    }

    return state;
}

}  // namespace rpc::infra::raft
