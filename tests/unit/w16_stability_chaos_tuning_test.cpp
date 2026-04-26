#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rpc/infra/infra.h"
#include "rpc/infra/memory_pool.h"
#include "rpc/infra/raft/raft_cluster.h"
#include "rpc/rpc/client.h"
#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/scheduler.h"

namespace {

bool expect_true(bool condition, const char* message) {
    if (condition) {
        return true;
    }
    std::cerr << message << '\n';
    return false;
}

std::size_t parse_env_size(const char* key, std::size_t default_value) {
    if (key == nullptr) {
        return default_value;
    }
    const char* raw = std::getenv(key);
    if (raw == nullptr || *raw == '\0') {
        return default_value;
    }
    try {
        return static_cast<std::size_t>(std::stoull(raw));
    } catch (...) {
        return default_value;
    }
}

bool env_enabled(const char* key) {
    const char* raw = std::getenv(key);
    return raw != nullptr && *raw != '\0';
}

class GrayOnlyDiscovery final : public rpc::client::IServiceDiscovery {
public:
    std::vector<rpc::client::ServiceNode> list_nodes(const std::string& service_name) override {
        if (service_name != "svc.w16.gray") {
            return {};
        }
        return {
            rpc::client::ServiceNode{
                "stable-a",
                "127.0.0.1",
                static_cast<std::uint16_t>(1),
                {{"lane", "stable"}},
                0.30,
                0.28,
                120.0,
                22.0,
            },
            rpc::client::ServiceNode{
                "gray-b",
                "127.0.0.1",
                static_cast<std::uint16_t>(2),
                {{"lane", "gray"}},
                0.48,
                0.44,
                180.0,
                52.0,
            },
        };
    }
};

bool write_config_with_retry(
    rpc::infra::raft::RaftCluster& cluster,
    std::string key,
    std::string value,
    std::size_t retries
) {
    for (std::size_t i = 0; i < retries; ++i) {
        if (cluster.upsert_config(key, value)) {
            return true;
        }
        cluster.run_ticks(8);
        (void)cluster.wait_for_leader(64);
    }
    return false;
}

}  // namespace

int main() {
    const bool long_run = env_enabled("RPC_W16_LONG_RUN");

    // 1) 调度窃取场景。
    {
        rpc::runtime::CoroutineScheduler scheduler;
        scheduler.start(6);

        const std::size_t child_tasks = parse_env_size(
            "RPC_W16_SCHED_TASKS",
            long_run ? 8000 : 2500
        );
        std::atomic<std::size_t> finished{0};

        (void)scheduler.schedule([&]() {
            for (std::size_t i = 0; i < child_tasks; ++i) {
                (void)scheduler.schedule([&finished]() {
                    for (int j = 0; j < 3; ++j) {
                        rpc::runtime::Coroutine::yield_current();
                    }
                    finished.fetch_add(1, std::memory_order_relaxed);
                });
            }
            for (int i = 0; i < 128; ++i) {
                rpc::runtime::Coroutine::yield_current();
            }
        });

        scheduler.wait_idle();
        const rpc::runtime::SchedulerProfileSnapshot profile = scheduler.profile_snapshot();

        if (!expect_true(
                finished.load(std::memory_order_acquire) == child_tasks,
                "scheduler task completion mismatch"
            )) {
            scheduler.stop();
            return 1;
        }
        if (!expect_true(
                scheduler.steal_count() > 0 || profile.dequeue_steal > 0,
                "work stealing was not observed"
            )) {
            scheduler.stop();
            return 1;
        }

        scheduler.stop();
    }

    // 2) 分代内存池热扩缩容场景。
    {
        rpc::infra::AdaptiveMemoryPoolOptions options;
        options.short_slot_size = 64;
        options.long_slot_size = 1024;
        options.short_initial_slots = 8;
        options.long_initial_slots = 4;
        options.short_max_slots = 1024;
        options.long_max_slots = 512;
        options.hot_expand_utilization = 0.50;
        options.cold_shrink_utilization = 0.20;
        options.expand_step_min_slots = 8;
        options.shrink_step_min_slots = 8;
        options.cold_shrink_idle_ticks = 2;

        rpc::infra::AdaptiveMemoryPool pool(options);

        std::vector<void*> batch;
        batch.reserve(320);
        for (std::size_t i = 0; i < 320; ++i) {
            void* ptr = pool.allocate(32, rpc::infra::MemoryGeneration::ShortLived);
            if (!expect_true(ptr != nullptr, "memory pool burst allocation failed")) {
                return 1;
            }
            batch.push_back(ptr);
        }

        for (void* ptr : batch) {
            if (!expect_true(pool.deallocate(ptr), "memory pool deallocation failed")) {
                return 1;
            }
        }

        for (int i = 0; i < 8; ++i) {
            pool.maintenance_tick();
        }

        const rpc::infra::MemoryPoolStats stats = pool.stats();
        if (!expect_true(stats.short_lived.hot_expand_count > 0, "memory pool hot expand not observed")) {
            return 1;
        }
        if (!expect_true(stats.short_lived.cold_shrink_count > 0, "memory pool cold shrink not observed")) {
            return 1;
        }
        if (!expect_true(stats.active_allocations == 0, "memory pool active allocations should be zero")) {
            return 1;
        }
    }

    // 3) 参数默认收敛 + 灰度流量分配。
    {
        rpc::infra::init_infra();
        const rpc::infra::ConfigSnapshot config = rpc::infra::config_repository().snapshot();
        if (!expect_true(
                config.values.find("rpc.retry.ratio") != config.values.end(),
                "missing rpc.retry.ratio default config"
            )) {
            return 1;
        }
        if (!expect_true(
                config.values.find("rpc.circuit.failure_threshold") != config.values.end(),
                "missing rpc.circuit.failure_threshold default config"
            )) {
            return 1;
        }
        if (!expect_true(
                config.values.find("rpc.lb.target_qps") != config.values.end(),
                "missing rpc.lb.target_qps default config"
            )) {
            return 1;
        }
        if (!expect_true(
                config.values.find("gateway.rate_limit.qps") != config.values.end(),
                "missing gateway.rate_limit.qps default config"
            )) {
            return 1;
        }

        rpc::client::ClientInitOptions options;
        options.discovery = std::make_shared<GrayOnlyDiscovery>();
        rpc::client::init_client(std::move(options));

        const auto client = rpc::client::default_client();
        if (!expect_true(client != nullptr, "default client is null")) {
            return 1;
        }

        const std::size_t requests = parse_env_size(
            "RPC_W16_GRAY_REQUESTS",
            long_run ? 1500 : 400
        );
        std::size_t gray_hits = 0;
        std::size_t stable_hits = 0;

        for (std::size_t i = 0; i < requests; ++i) {
            rpc::client::RpcRequest request;
            request.service = "svc.w16.gray";
            request.method = "Echo";
            request.payload = "payload-" + std::to_string(i);
            request.timeout_ms = 120;
            request.metadata["x-user-id"] = "user-" + std::to_string(i);
            request.metadata["x-gray-percent"] = "30";
            request.metadata["x-fallback-cache"] = "0";

            const rpc::client::RpcResponse response = client->invoke(request);
            if (response.traffic_lane == "gray") {
                ++gray_hits;
            } else if (response.traffic_lane == "stable") {
                ++stable_hits;
            }
        }

        const std::size_t routed = gray_hits + stable_hits;
        if (!expect_true(routed > 0, "gray routing produced no stable/gray traffic samples")) {
            return 1;
        }

        const double gray_ratio = static_cast<double>(gray_hits) / static_cast<double>(routed);
        if (!expect_true(
                gray_ratio >= 0.15 && gray_ratio <= 0.45,
                "gray routing ratio out of expected range"
            )) {
            return 1;
        }

        rpc::client::RpcRequest retry_observe;
        retry_observe.service = "svc.w16.gray";
        retry_observe.method = "Echo";
        retry_observe.payload = "observe-retry-budget";
        retry_observe.timeout_ms = 80;
        retry_observe.metadata["x-fallback-cache"] = "0";
        const rpc::client::RpcResponse observed = client->invoke(retry_observe);
        if (!expect_true(observed.retry_budget_max_tokens >= 1, "retry budget defaults were not applied")) {
            return 1;
        }
    }

    // 4) Raft 选主 + 故障演练（延迟抖动/丢包/节点上下线/Leader 切换）。
    {
        rpc::runtime::CoroutineScheduler scheduler;
        scheduler.start(4);

        rpc::infra::raft::RaftOptions options;
        options.scheduler = &scheduler;
        options.snapshot_log_threshold = 12;

        rpc::infra::raft::RaftCluster cluster(options);
        cluster.bootstrap({"n1", "n2", "n3", "n4", "n5"});

        if (!expect_true(cluster.wait_for_leader(128), "raft initial leader convergence failed")) {
            scheduler.stop();
            return 1;
        }

        cluster.configure_fault_injection(rpc::infra::raft::RaftFaultInjectionOptions{
            35.0,
            1,
            4,
        });

        if (!expect_true(
                cluster.register_service("svc.w16", {"base", "127.0.0.1", 9300}),
                "raft base service register failed"
            )) {
            scheduler.stop();
            return 1;
        }

        std::mt19937 rng(20260410u);
        const std::size_t rounds = parse_env_size("RPC_W16_RAFT_ROUNDS", long_run ? 1800 : 260);
        std::size_t observed_switches = 0;

        for (std::size_t round = 0; round < rounds; ++round) {
            if (round % 45 == 0) {
                const auto old_leader = cluster.leader();
                if (old_leader.has_value()) {
                    (void)cluster.set_node_available(*old_leader, false);
                    bool switched = false;
                    for (std::size_t t = 0; t < 96; ++t) {
                        cluster.tick();
                        const auto current = cluster.leader();
                        if (current.has_value() && *current != *old_leader) {
                            switched = true;
                            break;
                        }
                    }
                    if (!expect_true(switched, "raft leader switch not observed during chaos")) {
                        scheduler.stop();
                        return 1;
                    }
                    ++observed_switches;
                    (void)cluster.set_node_available(*old_leader, true);
                }
            }

            if (round % 30 == 0) {
                const std::string target = (rng() % 2 == 0) ? "n4" : "n5";
                (void)cluster.set_node_available(target, false);
                cluster.run_ticks(4);
                (void)cluster.set_node_available(target, true);
            }

            const bool write_ok = write_config_with_retry(
                cluster,
                "w16.round." + std::to_string(round),
                "v" + std::to_string(round),
                4
            );
            if (!expect_true(write_ok, "raft config write failed under chaos injection")) {
                scheduler.stop();
                return 1;
            }

            if (round % 9 == 0) {
                const std::uint16_t port = static_cast<std::uint16_t>(9400 + (round % 120));
                (void)cluster.register_service(
                    "svc.w16",
                    {"inst-" + std::to_string(round), "127.0.0.1", port}
                );
            }

            cluster.run_ticks(3);
            if (!expect_true(cluster.wait_for_leader(96), "raft leader convergence failed during chaos")) {
                scheduler.stop();
                return 1;
            }
        }

        cluster.configure_fault_injection(rpc::infra::raft::RaftFaultInjectionOptions{});
        cluster.run_ticks(256);

        if (!expect_true(cluster.wait_for_leader(128), "raft leader convergence failed after chaos")) {
            scheduler.stop();
            return 1;
        }
        if (!expect_true(cluster.metadata_consistent(), "raft metadata is inconsistent after chaos")) {
            scheduler.stop();
            return 1;
        }

        const rpc::infra::raft::RaftStatus status = cluster.status();
        if (!expect_true(status.replication_dropped > 0, "raft packet loss injection was not observed")) {
            scheduler.stop();
            return 1;
        }
        if (!expect_true(status.replication_delayed > 0, "raft delay injection was not observed")) {
            scheduler.stop();
            return 1;
        }
        if (!expect_true(status.leader_changes >= observed_switches, "raft leader change metric mismatch")) {
            scheduler.stop();
            return 1;
        }

        const auto nodes = cluster.discover_service("svc.w16");
        if (!expect_true(!nodes.empty(), "raft service discovery interrupted")) {
            scheduler.stop();
            return 1;
        }

        scheduler.stop();
    }

    std::cout << "w16_stability_chaos_tuning_test passed"
              << " mode=" << (long_run ? "long" : "quick")
              << '\n';
    return 0;
}
