#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include "rpc/infra/raft/raft_cluster.h"
#include "rpc/runtime/scheduler.h"

namespace {

std::string make_temp_dir(const char* suffix) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::string("/tmp/rpc_w12_") + suffix + "_" + std::to_string(now);
}

}  // namespace

int main() {
    const std::string storage_dir = make_temp_dir("storage_recovery");
    std::filesystem::remove_all(storage_dir);

    std::uint64_t first_commit_index = 0;

    {
        rpc::runtime::CoroutineScheduler scheduler;
        scheduler.start(2);

        rpc::infra::raft::RaftOptions options;
        options.scheduler = &scheduler;
        options.enable_persistence = true;
        options.storage_root_dir = storage_dir;
        options.snapshot_log_threshold = 4;

        auto cluster = std::make_shared<rpc::infra::raft::RaftCluster>(options);
        cluster->bootstrap({"n1", "n2", "n3"});
        if (!cluster->wait_for_leader(64)) {
            std::cerr << "initial leader election failed\n";
            scheduler.stop();
            return 1;
        }

        if (!cluster->upsert_config("persist.key", "persist.value")) {
            std::cerr << "initial config write failed\n";
            scheduler.stop();
            return 1;
        }
        if (!cluster->register_service("svc.persist", {"node-a", "127.0.0.1", 9101})) {
            std::cerr << "initial service register failed\n";
            scheduler.stop();
            return 1;
        }

        cluster->run_ticks(20);
        first_commit_index = cluster->status().committed_index;
        if (first_commit_index == 0) {
            std::cerr << "initial commit index is zero\n";
            scheduler.stop();
            return 1;
        }

        scheduler.stop();
    }

    {
        rpc::runtime::CoroutineScheduler scheduler;
        scheduler.start(2);

        rpc::infra::raft::RaftOptions options;
        options.scheduler = &scheduler;
        options.enable_persistence = true;
        options.storage_root_dir = storage_dir;
        options.snapshot_log_threshold = 4;

        auto cluster = std::make_shared<rpc::infra::raft::RaftCluster>(options);
        cluster->bootstrap({"n1", "n2", "n3"});
        if (!cluster->wait_for_leader(64)) {
            std::cerr << "recovery leader election failed\n";
            scheduler.stop();
            return 1;
        }

        cluster->run_ticks(10);

        const auto restored = cluster->config_value("persist.key");
        if (!restored.has_value() || *restored != "persist.value") {
            std::cerr << "restored config mismatch\n";
            scheduler.stop();
            return 1;
        }

        const auto services = cluster->discover_service("svc.persist");
        if (services.empty() || services.front().id != "node-a") {
            std::cerr << "restored service registry mismatch\n";
            scheduler.stop();
            return 1;
        }

        if (cluster->status().committed_index < first_commit_index) {
            std::cerr << "committed index rollback after recovery\n";
            scheduler.stop();
            return 1;
        }

        scheduler.stop();
    }

    std::filesystem::remove_all(storage_dir);
    std::cout << "w12_raft_storage_recovery_test passed\n";
    return 0;
}

