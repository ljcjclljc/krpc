#pragma once

// 文件用途：
// 定义基础设施层中的配置治理抽象，包括配置中心客户端接口、
// 本地快照仓库、版本更新与回滚入口。

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace rpc::infra {

// 键值配置集合类型。
using ConfigMap = std::unordered_map<std::string, std::string>;

// 一份配置快照：
// version 必须单调递增，用于新旧配置判定。
struct ConfigSnapshot {
    std::uint64_t version{0};
    ConfigMap values;
};

// 配置中心客户端抽象。
// 后续可对接 ZooKeeper/Etcd/Nacos 等外部系统。
class IConfigCenterClient {
public:
    virtual ~IConfigCenterClient() = default;

    // 拉取最新配置快照：
    // - 有结果：返回 snapshot
    // - 暂无结果或失败：返回 std::nullopt
    virtual std::optional<ConfigSnapshot> fetch_latest() = 0;
};

// 本地配置仓库：
// 提供线程安全的读取、版本更新、回滚能力。
class ConfigRepository {
public:
    ConfigRepository();
    explicit ConfigRepository(ConfigSnapshot initial_snapshot);

    // 获取当前快照副本（线程安全）。
    ConfigSnapshot snapshot() const;

    // 仅在 incoming.version 更新时应用新配置。
    // 返回值：
    // - true  : 发生更新
    // - false : 版本不新，忽略更新
    bool update_if_newer(const ConfigSnapshot& incoming);

    // 回滚到历史版本。
    // 返回值：
    // - true  : 回滚成功（或当前已是目标版本）
    // - false : 未找到目标版本
    bool rollback_to(std::uint64_t version);

    // 获取历史版本号列表（按追加顺序）。
    std::vector<std::uint64_t> history_versions() const;

private:
    ConfigSnapshot current_;
    std::vector<ConfigSnapshot> history_;
    mutable std::mutex mutex_;
};

// 基础设施初始化参数。
struct InfraInitOptions {
    // 可选配置中心客户端实现；为空时使用默认静态客户端。
    std::shared_ptr<IConfigCenterClient> config_client;

    // 启动期基础配置快照；未设置版本时使用内置默认快照。
    ConfigSnapshot bootstrap_snapshot;
};

// 初始化 infra 全局状态（配置仓库 + 配置中心客户端）。
void init_infra(InfraInitOptions options = {});

// 触发一次“从配置中心刷新到本地”的同步流程。
bool refresh_config_from_center();

// 获取全局配置仓库只读引用。
const ConfigRepository& config_repository();

}  // namespace rpc::infra
