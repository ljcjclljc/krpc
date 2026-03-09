#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <utility>

// 文件用途：
// 验证 W1 配置治理骨架能力：
// 1) 本地快照仓库可按版本更新
// 2) 仓库可回滚到历史版本
// 3) infra 初始化后可从配置中心刷新最新配置

#include "rpc/infra/infra.h"

namespace {

class FakeConfigCenterClient final : public rpc::infra::IConfigCenterClient {
public:
    explicit FakeConfigCenterClient(rpc::infra::ConfigSnapshot snapshot)
        : snapshot_(std::move(snapshot)) {}

    std::optional<rpc::infra::ConfigSnapshot> fetch_latest() override {
        // 测试替身：固定返回预设快照。
        return snapshot_;
    }

private:
    rpc::infra::ConfigSnapshot snapshot_;
};

}  // namespace

int main() {
    // 准备基线配置版本（v1）。
    rpc::infra::ConfigSnapshot base;
    base.version = 1;
    base.values.emplace("rpc.timeout_ms", "200");

    // 构造仓库并尝试应用更高版本（v2）。
    rpc::infra::ConfigRepository repository(base);
    rpc::infra::ConfigSnapshot incoming;
    incoming.version = 2;
    incoming.values.emplace("rpc.timeout_ms", "300");

    if (!repository.update_if_newer(incoming)) {
        std::cerr << "expected repository update success\n";
        return 1;
    }

    const rpc::infra::ConfigSnapshot latest = repository.snapshot();
    if (latest.version != 2 || latest.values.at("rpc.timeout_ms") != "300") {
        std::cerr << "unexpected repository latest snapshot\n";
        return 1;
    }

    // 回滚到 v1，并校验配置值恢复。
    if (!repository.rollback_to(1)) {
        std::cerr << "expected rollback to version 1 success\n";
        return 1;
    }

    const rpc::infra::ConfigSnapshot rolled_back = repository.snapshot();
    if (rolled_back.version != 1 || rolled_back.values.at("rpc.timeout_ms") != "200") {
        std::cerr << "unexpected rollback result\n";
        return 1;
    }

    rpc::infra::ConfigSnapshot remote;
    remote.version = 5;
    remote.values.emplace("gateway.queue.max_size", "8192");

    // 使用 fake 配置中心初始化 infra，验证启动期刷新能力。
    rpc::infra::InfraInitOptions options;
    options.bootstrap_snapshot = base;
    options.config_client = std::make_shared<FakeConfigCenterClient>(remote);
    rpc::infra::init_infra(std::move(options));

    const rpc::infra::ConfigSnapshot global_snapshot = rpc::infra::config_repository().snapshot();
    if (global_snapshot.version != 5) {
        std::cerr << "expected global snapshot version 5, got " << global_snapshot.version << '\n';
        return 1;
    }

    if (global_snapshot.values.find("gateway.queue.max_size") == global_snapshot.values.end()) {
        std::cerr << "missing refreshed key gateway.queue.max_size\n";
        return 1;
    }

    std::cout << "config_snapshot_test passed\n";
    return 0;
}
