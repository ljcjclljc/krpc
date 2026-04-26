#include "rpc/infra/infra.h"

// 文件用途：
// 提供 W1 阶段配置治理最小实现：
// 1) 配置中心客户端抽象（默认静态实现）
// 2) 线程安全本地快照仓库
// 3) 配置刷新与回滚基础流程

#include <algorithm>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace rpc::infra {

namespace {

class StaticConfigCenterClient final : public IConfigCenterClient {
public:
    explicit StaticConfigCenterClient(ConfigSnapshot snapshot) : snapshot_(std::move(snapshot)) {}

    std::optional<ConfigSnapshot> fetch_latest() override {
        // 静态实现始终返回同一份快照，便于本地验证流程。
        return snapshot_;
    }

private:
    ConfigSnapshot snapshot_;
};

std::mutex g_state_mutex;
std::shared_ptr<IConfigCenterClient> g_config_client;
std::unique_ptr<ConfigRepository> g_repository;

ConfigSnapshot default_bootstrap_snapshot() {
    // 启动默认配置：保证系统在无外部配置中心时也可运行。
    ConfigSnapshot snapshot;
    snapshot.version = 1;
    snapshot.values.emplace("rpc.timeout_ms", "200");
    snapshot.values.emplace("rpc.retry.max_retries", "1");
    snapshot.values.emplace("rpc.retry.window_ms", "1000");
    snapshot.values.emplace("rpc.retry.ratio", "0.2");
    snapshot.values.emplace("rpc.retry.min_tokens", "1");
    snapshot.values.emplace("rpc.retry_budget_ratio", "0.2");
    snapshot.values.emplace("rpc.circuit.failure_threshold", "3");
    snapshot.values.emplace("rpc.circuit.open_ms", "280");
    snapshot.values.emplace("rpc.circuit.half_open_success", "2");
    snapshot.values.emplace("rpc.circuit.half_open_max_probes", "1");
    snapshot.values.emplace("rpc.lb.weight.cpu", "0.30");
    snapshot.values.emplace("rpc.lb.weight.mem", "0.20");
    snapshot.values.emplace("rpc.lb.weight.qps", "0.20");
    snapshot.values.emplace("rpc.lb.weight.latency", "0.30");
    snapshot.values.emplace("rpc.lb.target_qps", "220");
    snapshot.values.emplace("rpc.lb.target_latency_ms", "80");
    snapshot.values.emplace("rpc.gray.percent", "0");
    snapshot.values.emplace("gateway.queue.max_size", "4096");
    snapshot.values.emplace("gateway.rate_limit.qps", "10000");
    snapshot.values.emplace("gateway.rate_limit.burst", "2000");
    snapshot.values.emplace("net.tls.enabled", "0");
    snapshot.values.emplace("net.tls.mtls.enabled", "0");
    snapshot.values.emplace("net.tls.insecure_skip_verify", "0");
    snapshot.values.emplace("net.tls.ca_file", "");
    snapshot.values.emplace("net.tls.cert_file", "");
    snapshot.values.emplace("net.tls.key_file", "");
    snapshot.values.emplace("net.tls.server_name", "");
    return snapshot;
}

}  // namespace

ConfigRepository::ConfigRepository() : ConfigRepository(default_bootstrap_snapshot()) {}

ConfigRepository::ConfigRepository(ConfigSnapshot initial_snapshot)
    : current_(std::move(initial_snapshot)) {
    // 历史列表保留第一版快照，作为回滚基线。
    history_.push_back(current_);
}

ConfigSnapshot ConfigRepository::snapshot() const {
    // 返回当前快照副本，避免外部直接修改内部状态。
    std::lock_guard<std::mutex> lock(mutex_);
    return current_;
}

bool ConfigRepository::update_if_newer(const ConfigSnapshot& incoming) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (incoming.version <= current_.version) {
        // 只允许更高版本覆盖，防止旧配置回写。
        return false;
    }

    // 应用新版本并写入历史。
    current_ = incoming;
    history_.push_back(current_);
    return true;
}

bool ConfigRepository::rollback_to(std::uint64_t version) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = std::find_if(
        history_.rbegin(),
        history_.rend(),
        [version](const ConfigSnapshot& snapshot) { return snapshot.version == version; }
    );
    if (it == history_.rend()) {
        // 未找到目标版本，回滚失败。
        return false;
    }

    if (current_.version == version) {
        // 当前已经是目标版本，视为成功。
        return true;
    }

    // 应用历史版本，并记录一次“回滚后快照”。
    current_ = *it;
    history_.push_back(current_);
    return true;
}

std::vector<std::uint64_t> ConfigRepository::history_versions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::uint64_t> versions;
    versions.reserve(history_.size());
    for (const auto& snapshot : history_) {
        versions.push_back(snapshot.version);
    }
    // 返回版本轨迹，便于调试与审计。
    return versions;
}

void init_infra(InfraInitOptions options) {
    {
        // 初始化全局状态（客户端与仓库）。
        std::lock_guard<std::mutex> lock(g_state_mutex);

        ConfigSnapshot bootstrap = std::move(options.bootstrap_snapshot);
        if (bootstrap.version == 0) {
            // 未提供有效启动快照时，回退到默认配置。
            bootstrap = default_bootstrap_snapshot();
        }

        g_repository = std::make_unique<ConfigRepository>(bootstrap);

        if (options.config_client) {
            // 使用外部注入的配置中心客户端。
            g_config_client = std::move(options.config_client);
        } else {
            // 默认使用静态客户端。
            g_config_client = std::make_shared<StaticConfigCenterClient>(bootstrap);
        }
    }

    // 启动后主动拉取一次，保证本地状态尽可能新。
    (void)refresh_config_from_center();
}

bool refresh_config_from_center() {
    // 刷新流程串行化，避免并发刷新造成覆盖竞争。
    std::lock_guard<std::mutex> lock(g_state_mutex);
    if (!g_repository || !g_config_client) {
        // 未初始化时直接返回 false。
        return false;
    }

    const std::optional<ConfigSnapshot> latest = g_config_client->fetch_latest();
    if (!latest.has_value()) {
        // 配置中心暂无结果或拉取失败。
        return false;
    }

    // 仅当版本更新时才会实际生效。
    return g_repository->update_if_newer(*latest);
}

bool publish_config_snapshot(const ConfigSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(g_state_mutex);
    if (!g_repository) {
        return false;
    }
    return g_repository->update_if_newer(snapshot);
}

bool publish_config_patch(const ConfigMap& patch, std::uint64_t* applied_version) {
    std::lock_guard<std::mutex> lock(g_state_mutex);
    if (!g_repository) {
        return false;
    }

    ConfigSnapshot next = g_repository->snapshot();
    for (const auto& entry : patch) {
        next.values[entry.first] = entry.second;
    }
    next.version = next.version + 1;

    const bool applied = g_repository->update_if_newer(next);
    if (applied && applied_version != nullptr) {
        *applied_version = next.version;
    }
    return applied;
}

bool rollback_config_to(std::uint64_t version) {
    std::lock_guard<std::mutex> lock(g_state_mutex);
    if (!g_repository) {
        return false;
    }
    return g_repository->rollback_to(version);
}

std::uint64_t current_config_version() {
    std::lock_guard<std::mutex> lock(g_state_mutex);
    if (!g_repository) {
        return 0;
    }
    return g_repository->snapshot().version;
}

const ConfigRepository& config_repository() {
    std::lock_guard<std::mutex> lock(g_state_mutex);
    if (!g_repository) {
        // 强约束：必须先 init_infra 再读取仓库。
        throw std::logic_error("infra is not initialized");
    }
    return *g_repository;
}

}  // namespace rpc::infra
