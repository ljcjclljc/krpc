#pragma once

// 文件用途：
// 提供 W12 阶段自研 Raft 一致性模块：
// 1) Leader 选举、日志复制、快照裁剪
// 2) Joint Consensus 在线扩缩容
// 3) 服务注册发现与配置同步（替代外部 ZooKeeper 强依赖）
// 4) 与协程调度链路融合（可选接入 CoroutineScheduler）

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rpc/infra/infra.h"
#include "rpc/rpc/client.h"

namespace rpc::runtime {
class CoroutineScheduler;
}

namespace rpc::infra::raft {

struct ServiceInstance {
    std::string id;
    std::string host;
    std::uint16_t port{0};
};

struct RaftOptions {
    std::size_t election_timeout_ticks_min{6};
    std::size_t election_timeout_ticks_max{12};
    std::size_t heartbeat_interval_ticks{2};
    std::size_t snapshot_log_threshold{32};
    bool enable_persistence{false};
    std::string storage_root_dir;
    bool enable_rpc_transport{false};
    std::string rpc_bind_host{"127.0.0.1"};
    std::uint16_t rpc_base_port{19000};
    rpc::runtime::CoroutineScheduler* scheduler{nullptr};
};

struct RaftFaultInjectionOptions {
    // 复制链路丢包率，范围 [0, 100]。
    double packet_loss_percent{0.0};
    // 复制链路额外延迟（tick），0 表示无延迟注入。
    std::size_t replicate_delay_ticks_min{0};
    std::size_t replicate_delay_ticks_max{0};
};

struct RaftStatus {
    bool has_leader{false};
    std::string leader_id;
    std::uint64_t current_term{0};
    bool joint_consensus_active{false};
    std::size_t leader_changes{0};
    std::uint64_t committed_index{0};
    std::size_t snapshot_count{0};
    std::size_t scheduler_dispatches{0};
    std::size_t rpc_mirror_calls{0};
    std::size_t replication_delayed{0};
    std::size_t replication_dropped{0};
    std::vector<std::string> voters;
};

class RaftCluster {
public:
    explicit RaftCluster(RaftOptions options = {});
    ~RaftCluster();

    RaftCluster(const RaftCluster&) = delete;
    RaftCluster& operator=(const RaftCluster&) = delete;
    RaftCluster(RaftCluster&&) = delete;
    RaftCluster& operator=(RaftCluster&&) = delete;

    // 使用初始投票成员启动集群。
    void bootstrap(const std::vector<std::string>& initial_voters);

    // 推进一轮逻辑时钟（选举、心跳、复制、提交）。
    void tick();
    void run_ticks(std::size_t ticks);

    // 在 max_ticks 内等待 Leader 收敛。
    bool wait_for_leader(std::size_t max_ticks);

    // 节点上下线模拟（用于 Leader 切换与恢复验证）。
    bool set_node_available(const std::string& node_id, bool available);

    // 配置与服务元数据写入（走 Raft 日志复制）。
    bool upsert_config(std::string key, std::string value);
    bool erase_config(const std::string& key);
    bool register_service(std::string service, ServiceInstance instance);
    bool unregister_service(std::string service, std::string instance_id);

    // 读取接口（无 Leader 时可回退到最新已提交副本，保障读可用性）。
    std::optional<std::string> config_value(const std::string& key) const;
    std::unordered_map<std::string, std::string> config_snapshot() const;
    std::vector<ServiceInstance> discover_service(const std::string& service) const;

    // Joint Consensus 成员变更。
    bool begin_joint_consensus(const std::vector<std::string>& new_voters);
    bool finalize_joint_consensus();
    bool reconfigure_joint(const std::vector<std::string>& new_voters, std::size_t max_ticks = 64);

    // 故障注入：用于稳定性压测（延迟抖动/丢包）。
    void configure_fault_injection(RaftFaultInjectionOptions options);
    RaftFaultInjectionOptions fault_injection_options() const;

    // 一致性与运行态观测。
    bool metadata_consistent() const;
    RaftStatus status() const;
    std::optional<std::string> leader() const;
    std::optional<std::string> node_endpoint(const std::string& node_id) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// 将 Raft 元数据状态适配为现有 RPC 服务发现接口。
class RaftServiceDiscovery final : public rpc::client::IServiceDiscovery {
public:
    explicit RaftServiceDiscovery(std::shared_ptr<RaftCluster> cluster);

    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override;

private:
    std::shared_ptr<RaftCluster> cluster_;
};

// 将 Raft 配置状态适配为现有 infra 配置中心接口。
class RaftConfigCenterClient final : public rpc::infra::IConfigCenterClient {
public:
    explicit RaftConfigCenterClient(std::shared_ptr<RaftCluster> cluster);

    std::optional<rpc::infra::ConfigSnapshot> fetch_latest() override;

private:
    std::shared_ptr<RaftCluster> cluster_;
};

}  // namespace rpc::infra::raft
