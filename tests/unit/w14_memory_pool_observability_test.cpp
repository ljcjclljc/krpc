#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <boost/json/object.hpp>

#include "rpc/infra/memory_pool.h"
#include "rpc/infra/structured_log.h"

namespace {

using rpc::infra::AdaptiveMemoryPool;
using rpc::infra::AdaptiveMemoryPoolOptions;
using rpc::infra::LogLevel;
using rpc::infra::MemoryGeneration;

bool expect_true(bool condition, const char* message) {
    if (condition) {
        return true;
    }
    std::cerr << message << '\n';
    return false;
}

}  // namespace

int main() {
    AdaptiveMemoryPoolOptions options;
    options.short_slot_size = 64;
    options.long_slot_size = 2048;
    options.short_initial_slots = 4;
    options.long_initial_slots = 2;
    options.short_max_slots = 256;
    options.long_max_slots = 128;
    options.hot_expand_utilization = 0.50;
    options.cold_shrink_utilization = 0.25;
    options.expand_step_min_slots = 4;
    options.shrink_step_min_slots = 2;
    options.cold_shrink_idle_ticks = 2;
    options.enable_guard_checks = true;

    AdaptiveMemoryPool pool(options);

    // 1) 高并发下分代分配/释放稳定。
    std::atomic<bool> concurrent_ok{true};
    std::vector<std::thread> workers;
    workers.reserve(8);
    for (std::size_t worker = 0; worker < 8; ++worker) {
        workers.emplace_back([&pool, &concurrent_ok, worker]() {
            for (std::size_t i = 0; i < 1500; ++i) {
                const bool use_long = ((i + worker) % 5) == 0;
                const std::size_t size = use_long ? 1024 : 32;
                const MemoryGeneration generation = use_long
                    ? MemoryGeneration::LongLived
                    : MemoryGeneration::ShortLived;

                void* memory = pool.allocate(size, generation);
                if (memory == nullptr) {
                    concurrent_ok.store(false, std::memory_order_release);
                    return;
                }

                std::memset(memory, 0x5A, size);
                if (!pool.deallocate(memory)) {
                    concurrent_ok.store(false, std::memory_order_release);
                    return;
                }
            }
        });
    }
    for (std::thread& worker : workers) {
        worker.join();
    }

    if (!expect_true(concurrent_ok.load(std::memory_order_acquire), "concurrent allocate/deallocate failed")) {
        return 1;
    }

    // 2) 热扩容与冷缩容。
    std::vector<void*> burst_allocations;
    burst_allocations.reserve(80);
    for (std::size_t i = 0; i < 80; ++i) {
        void* ptr = pool.allocate(40, MemoryGeneration::ShortLived);
        if (!expect_true(ptr != nullptr, "short-lived burst allocation failed")) {
            return 1;
        }
        burst_allocations.push_back(ptr);
    }

    const std::size_t expanded_capacity = pool.stats().short_lived.capacity_slots;
    if (!expect_true(expanded_capacity > options.short_initial_slots, "hot expansion did not trigger")) {
        return 1;
    }

    for (void* ptr : burst_allocations) {
        if (!expect_true(pool.deallocate(ptr), "short-lived burst deallocation failed")) {
            return 1;
        }
    }

    for (std::size_t i = 0; i < 4; ++i) {
        pool.maintenance_tick();
    }

    const std::size_t shrunk_capacity = pool.stats().short_lived.capacity_slots;
    if (!expect_true(shrunk_capacity < expanded_capacity, "cold shrink did not trigger")) {
        return 1;
    }

    // 3) 分代命中率。
    for (std::size_t i = 0; i < 16; ++i) {
        void* ptr = pool.allocate(1200, MemoryGeneration::LongLived);
        if (!expect_true(ptr != nullptr, "long-lived allocation failed")) {
            return 1;
        }
        if (!expect_true(pool.deallocate(ptr), "long-lived deallocation failed")) {
            return 1;
        }
    }

    const rpc::infra::MemoryPoolStats hit_stats = pool.stats();
    if (!expect_true(hit_stats.short_lived.hit_ratio > 0.0, "short-lived hit ratio is zero")) {
        return 1;
    }
    if (!expect_true(hit_stats.long_lived.hit_ratio > 0.0, "long-lived hit ratio is zero")) {
        return 1;
    }

    // 4) 越界检测。
    void* overflow = pool.allocate(8, MemoryGeneration::ShortLived);
    if (!expect_true(overflow != nullptr, "overflow allocation failed")) {
        return 1;
    }
    auto* overflow_bytes = static_cast<unsigned char*>(overflow);
    overflow_bytes[8] = 0xAB;  // 逻辑越界（命中尾哨兵）

    if (!expect_true(!pool.deallocate(overflow), "overflow should fail guard check")) {
        return 1;
    }

    const rpc::infra::MemoryPoolStats guard_stats = pool.stats();
    if (!expect_true(guard_stats.guard_violations > 0, "guard violation metric not updated")) {
        return 1;
    }

    // 5) 泄漏检测快照。
    void* leak = pool.allocate(16, MemoryGeneration::LongLived);
    if (!expect_true(leak != nullptr, "leak probe allocation failed")) {
        return 1;
    }

    const rpc::infra::LeakReport leak_report = pool.detect_leaks(8);
    if (!expect_true(leak_report.leaked_allocations >= 1, "leak detector did not find active allocation")) {
        return 1;
    }
    if (!expect_true(leak_report.leaked_bytes >= 16, "leak detector leaked bytes mismatch")) {
        return 1;
    }

    if (!expect_true(pool.deallocate(leak), "leak probe deallocation failed")) {
        return 1;
    }

    // 6) 结构化日志采样策略。
    rpc::infra::configure_structured_log(rpc::infra::LogSamplingConfig{
        0.0,
        0.0,
        1.0,
        1.0,
        20,
    });
    rpc::infra::reset_structured_log_stats();

    if (!expect_true(
            !rpc::infra::should_sample_log(LogLevel::Info, "trace-w14", "quick", 5, false),
            "quick info log should be dropped by sampling"
        )) {
        return 1;
    }

    if (!expect_true(
            rpc::infra::should_sample_log(LogLevel::Info, "trace-w14", "slow", 40, false),
            "slow info log should be force-sampled"
        )) {
        return 1;
    }

    rpc::infra::structured_log(
        LogLevel::Info,
        "w14.quick",
        boost::json::object{{"phase", "quick"}},
        "trace-w14",
        5,
        false
    );
    rpc::infra::structured_log(
        LogLevel::Info,
        "w14.slow",
        boost::json::object{{"phase", "slow"}},
        "trace-w14",
        40,
        false
    );
    rpc::infra::structured_log(
        LogLevel::Error,
        "w14.error",
        boost::json::object{{"phase", "error"}},
        "trace-w14",
        1,
        false
    );

    const rpc::infra::StructuredLogStats log_stats = rpc::infra::structured_log_stats();
    if (!expect_true(log_stats.emitted_total >= 2, "structured log emitted_total mismatch")) {
        return 1;
    }
    if (!expect_true(log_stats.dropped_total >= 1, "structured log dropped_total mismatch")) {
        return 1;
    }
    if (!expect_true(log_stats.forced_by_slow_total >= 1, "structured log slow-force metric mismatch")) {
        return 1;
    }

    const rpc::infra::MemoryPoolStats final_stats = pool.stats();
    if (!expect_true(final_stats.active_allocations == 0, "active allocations should be zero")) {
        return 1;
    }

    std::cout << "w14_memory_pool_observability_test passed"
              << " short_hit_ratio=" << final_stats.short_lived.hit_ratio
              << " long_hit_ratio=" << final_stats.long_lived.hit_ratio
              << " short_capacity=" << final_stats.short_lived.capacity_slots
              << " guard_violations=" << final_stats.guard_violations
              << '\n';
    return 0;
}
