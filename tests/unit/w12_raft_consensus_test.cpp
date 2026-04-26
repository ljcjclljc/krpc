#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "rpc/infra/raft/raft_cluster.h"
#include "rpc/runtime/scheduler.h"

int main() {
    rpc::runtime::CoroutineScheduler scheduler;
    scheduler.start(2);

    rpc::infra::raft::RaftOptions options;
    options.election_timeout_ticks_min = 4;
    options.election_timeout_ticks_max = 8;
    options.heartbeat_interval_ticks = 1;
    options.snapshot_log_threshold = 6;
    options.scheduler = &scheduler;

    auto cluster = std::make_shared<rpc::infra::raft::RaftCluster>(options);
    cluster->bootstrap({"n1", "n2", "n3"});

    if (!cluster->wait_for_leader(64)) {
        std::cerr << "leader election did not converge within 64 ticks\n";
        scheduler.stop();
        return 1;
    }

    rpc::infra::raft::RaftServiceDiscovery discovery(cluster);
    rpc::infra::raft::RaftConfigCenterClient config_client(cluster);

    if (!cluster->register_service("svc.registry", {"inst-a", "127.0.0.1", 9010})
        || !cluster->register_service("svc.registry", {"inst-b", "127.0.0.1", 9011})) {
        std::cerr << "failed to register service instances\n";
        scheduler.stop();
        return 1;
    }

    for (int i = 0; i < 14; ++i) {
        if (!cluster->upsert_config("cfg." + std::to_string(i), "v" + std::to_string(i))) {
            std::cerr << "failed to write config at index " << i << '\n';
            scheduler.stop();
            return 1;
        }
    }

    auto status = cluster->status();
    if (status.snapshot_count == 0) {
        std::cerr << "snapshot was not triggered\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->metadata_consistent()) {
        std::cerr << "metadata is inconsistent before failover\n";
        scheduler.stop();
        return 1;
    }

    const auto old_leader = cluster->leader();
    if (!old_leader.has_value()) {
        std::cerr << "leader is missing before failover\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->set_node_available(*old_leader, false)) {
        std::cerr << "failed to isolate old leader\n";
        scheduler.stop();
        return 1;
    }

    std::size_t failover_ticks = 0;
    bool failover_ok = false;
    for (std::size_t i = 1; i <= 40; ++i) {
        cluster->tick();

        const auto nodes = discovery.list_nodes("svc.registry");
        if (nodes.empty()) {
            std::cerr << "service discovery interrupted during leader failover\n";
            scheduler.stop();
            return 1;
        }

        const auto current_leader = cluster->leader();
        if (current_leader.has_value() && *current_leader != *old_leader) {
            failover_ticks = i;
            failover_ok = true;
            break;
        }
    }

    if (!failover_ok) {
        std::cerr << "leader failover did not converge within 40 ticks\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->set_node_available(*old_leader, true)) {
        std::cerr << "failed to recover old leader\n";
        scheduler.stop();
        return 1;
    }
    cluster->run_ticks(32);

    if (!cluster->reconfigure_joint({"n1", "n2", "n3", "n4"}, 32)) {
        std::cerr << "joint consensus expand failed\n";
        scheduler.stop();
        return 1;
    }

    auto expanded_status = cluster->status();
    if (expanded_status.voters.size() != 4
        || std::find(expanded_status.voters.begin(), expanded_status.voters.end(), "n4") == expanded_status.voters.end()) {
        std::cerr << "expand result voters mismatch\n";
        scheduler.stop();
        return 1;
    }

    if (discovery.list_nodes("svc.registry").empty()) {
        std::cerr << "service discovery interrupted after expand\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->upsert_config("joint.phase", "expand")) {
        std::cerr << "failed to write config after expand\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->reconfigure_joint({"n2", "n3", "n4"}, 32)) {
        std::cerr << "joint consensus shrink failed\n";
        scheduler.stop();
        return 1;
    }

    auto shrunk_status = cluster->status();
    if (shrunk_status.voters.size() != 3
        || std::find(shrunk_status.voters.begin(), shrunk_status.voters.end(), "n1") != shrunk_status.voters.end()) {
        std::cerr << "shrink result voters mismatch\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->upsert_config("joint.phase", "shrink")) {
        std::cerr << "failed to write config after shrink\n";
        scheduler.stop();
        return 1;
    }

    if (!cluster->metadata_consistent()) {
        std::cerr << "metadata is inconsistent after reconfiguration\n";
        scheduler.stop();
        return 1;
    }

    const auto config_snapshot = config_client.fetch_latest();
    if (!config_snapshot.has_value()) {
        std::cerr << "raft config center adapter returned empty snapshot\n";
        scheduler.stop();
        return 1;
    }

    const auto cfg_it = config_snapshot->values.find("joint.phase");
    if (cfg_it == config_snapshot->values.end() || cfg_it->second != "shrink") {
        std::cerr << "config adapter snapshot mismatch\n";
        scheduler.stop();
        return 1;
    }

    if (cluster->status().scheduler_dispatches == 0) {
        std::cerr << "scheduler fusion was not observed\n";
        scheduler.stop();
        return 1;
    }

    scheduler.stop();

    std::cout << "w12_raft_consensus_test passed"
              << ", failover_ticks=" << failover_ticks
              << ", voters=" << cluster->status().voters.size()
              << ", snapshot_count=" << cluster->status().snapshot_count
              << ", scheduler_dispatches=" << cluster->status().scheduler_dispatches
              << '\n';
    return 0;
}

