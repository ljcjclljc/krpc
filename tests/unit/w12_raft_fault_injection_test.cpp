#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
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
    const std::string storage_dir = make_temp_dir("fault_inject");
    std::filesystem::remove_all(storage_dir);

    rpc::runtime::CoroutineScheduler scheduler;
    scheduler.start(4);

    rpc::infra::raft::RaftOptions options;
    options.scheduler = &scheduler;
    options.enable_persistence = true;
    options.storage_root_dir = storage_dir;
    options.enable_rpc_transport = true;
    options.rpc_bind_host = "127.0.0.1";
    options.rpc_base_port = 22300;
    options.snapshot_log_threshold = 8;

    auto cluster = std::make_shared<rpc::infra::raft::RaftCluster>(options);
    cluster->bootstrap({"n1", "n2", "n3", "n4", "n5"});

    if (!cluster->wait_for_leader(128)) {
        std::cerr << "initial leader convergence failed\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->register_service("svc.fault", {"base", "127.0.0.1", 9300})) {
        std::cerr << "base service register failed\n";
        scheduler.stop();
        return 1;
    }

    std::mt19937 rng(20260405u);
    std::size_t max_failover_ticks = 0;
    bool use_alt_members = false;

    for (std::size_t round = 0; round < 60; ++round) {
        if (round % 15 == 0) {
            const std::vector<std::string> next_members = use_alt_members
                ? std::vector<std::string>{"n1", "n2", "n3", "n4", "n5"}
                : std::vector<std::string>{"n1", "n2", "n3", "n4", "n6"};
            if (!cluster->reconfigure_joint(next_members, 48)) {
                std::cerr << "joint reconfigure failed at round " << round << '\n';
                scheduler.stop();
                return 1;
            }
            use_alt_members = !use_alt_members;
        }

        if (round % 7 == 0) {
            const auto old_leader = cluster->leader();
            if (!old_leader.has_value()) {
                std::cerr << "leader missing during fault injection\n";
                scheduler.stop();
                return 1;
            }
            if (!cluster->set_node_available(*old_leader, false)) {
                std::cerr << "failed to isolate leader during fault injection\n";
                scheduler.stop();
                return 1;
            }

            bool switched = false;
            std::size_t failover_ticks = 0;
            for (std::size_t t = 1; t <= 64; ++t) {
                cluster->tick();
                const auto new_leader = cluster->leader();
                if (new_leader.has_value() && *new_leader != *old_leader) {
                    switched = true;
                    failover_ticks = t;
                    break;
                }
            }
            if (!switched) {
                std::cerr << "leader failover timeout during fault injection\n";
                scheduler.stop();
                return 1;
            }

            max_failover_ticks = std::max(max_failover_ticks, failover_ticks);
            if (!cluster->set_node_available(*old_leader, true)) {
                std::cerr << "failed to recover isolated leader\n";
                scheduler.stop();
                return 1;
            }
        }

        const std::string key = "fault.round." + std::to_string(round);
        const std::string value = "v" + std::to_string(round);
        if (!cluster->upsert_config(key, value)) {
            std::cerr << "config write failed at round " << round << '\n';
            scheduler.stop();
            return 1;
        }

        if (round % 5 == 0) {
            const std::uint16_t port = static_cast<std::uint16_t>(9310 + (rng() % 200));
            if (!cluster->register_service(
                    "svc.fault",
                    {"inst-" + std::to_string(round), "127.0.0.1", port})) {
                std::cerr << "service register failed at round " << round << '\n';
                scheduler.stop();
                return 1;
            }
        }

        cluster->run_ticks(6);
        if (!cluster->wait_for_leader(64)) {
            std::cerr << "leader convergence failed at round " << round << '\n';
            scheduler.stop();
            return 1;
        }

        const auto nodes = cluster->discover_service("svc.fault");
        if (nodes.empty()) {
            std::cerr << "service discovery interrupted at round " << round << '\n';
            scheduler.stop();
            return 1;
        }

        if (!cluster->metadata_consistent()) {
            std::cerr << "metadata inconsistency detected at round " << round << '\n';
            scheduler.stop();
            return 1;
        }
    }

    const auto status = cluster->status();
    if (status.rpc_mirror_calls == 0) {
        std::cerr << "rpc mirror calls were not observed\n";
        scheduler.stop();
        return 1;
    }

    if (status.snapshot_count == 0) {
        std::cerr << "snapshot was not observed in fault injection run\n";
        scheduler.stop();
        return 1;
    }

    if (max_failover_ticks > 64) {
        std::cerr << "failover ticks exceed budget\n";
        scheduler.stop();
        return 1;
    }

    scheduler.stop();
    std::filesystem::remove_all(storage_dir);

    std::cout << "w12_raft_fault_injection_test passed"
              << ", max_failover_ticks=" << max_failover_ticks
              << ", rpc_mirror_calls=" << status.rpc_mirror_calls
              << ", snapshot_count=" << status.snapshot_count
              << '\n';
    return 0;
}

