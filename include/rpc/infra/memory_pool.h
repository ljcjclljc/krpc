#pragma once

// 文件用途：
// 提供分代式自适应内存池能力：
// 1) 短生命周期/长生命周期分池
// 2) 热扩容与冷缩容
// 3) 越界检查与泄漏检测
// 4) 高水位与分代命中指标

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace rpc::infra {

enum class MemoryGeneration : std::uint8_t {
    ShortLived = 0,
    LongLived = 1,
};

struct AdaptiveMemoryPoolOptions {
    std::size_t short_slot_size{256};
    std::size_t long_slot_size{4096};

    std::size_t short_initial_slots{64};
    std::size_t long_initial_slots{64};

    std::size_t short_max_slots{8192};
    std::size_t long_max_slots{4096};

    double hot_expand_utilization{0.85};
    double cold_shrink_utilization{0.25};

    std::size_t expand_step_min_slots{8};
    std::size_t shrink_step_min_slots{8};
    std::size_t cold_shrink_idle_ticks{3};

    bool enable_guard_checks{true};
};

struct MemoryPoolGenerationStats {
    std::size_t slot_size{0};
    std::size_t capacity_slots{0};
    std::size_t in_use_slots{0};
    std::size_t in_use_high_watermark{0};

    std::size_t allocation_requests{0};
    std::size_t allocation_hits{0};

    std::size_t hot_expand_count{0};
    std::size_t cold_shrink_count{0};

    double utilization{0.0};
    double hit_ratio{0.0};
};

struct MemoryPoolStats {
    MemoryPoolGenerationStats short_lived;
    MemoryPoolGenerationStats long_lived;

    std::size_t total_allocations{0};
    std::size_t total_frees{0};

    std::size_t fallback_allocations{0};
    std::size_t fallback_in_use{0};

    std::size_t active_allocations{0};
    std::size_t active_bytes{0};
    std::size_t active_bytes_high_watermark{0};

    std::size_t guard_violations{0};
    std::size_t invalid_frees{0};
    std::uint64_t lock_wait_ns_total{0};
    std::size_t lock_wait_samples{0};
    std::uint64_t lock_wait_ns_avg{0};

    std::size_t leak_suspects{0};
};

struct LeakRecord {
    std::uint64_t allocation_id{0};
    MemoryGeneration generation{MemoryGeneration::ShortLived};
    std::size_t requested_bytes{0};
    bool pooled{false};
};

struct LeakReport {
    std::size_t leaked_allocations{0};
    std::size_t leaked_bytes{0};
    std::vector<LeakRecord> records;
};

class AdaptiveMemoryPool {
public:
    explicit AdaptiveMemoryPool(AdaptiveMemoryPoolOptions options = {});
    ~AdaptiveMemoryPool();

    AdaptiveMemoryPool(const AdaptiveMemoryPool&) = delete;
    AdaptiveMemoryPool& operator=(const AdaptiveMemoryPool&) = delete;

    void* allocate(std::size_t bytes, MemoryGeneration generation = MemoryGeneration::ShortLived);
    bool deallocate(void* ptr) noexcept;

    // 低负载阶段调用：触发冷缩容判定。
    void maintenance_tick();

    MemoryPoolStats stats() const;
    LeakReport detect_leaks(std::size_t max_records = 32) const;

    // 全局池（便于跨模块复用）。
    static AdaptiveMemoryPool& instance();
    static void initialize_global(AdaptiveMemoryPoolOptions options);

private:
    struct BlockHeader {
        std::uint64_t front_guard{0};
        std::uint64_t allocation_id{0};
        std::uint32_t flags{0};
        std::uint32_t generation{0};
        std::size_t slot_size{0};
        std::size_t requested_size{0};
    };

    struct GenerationPool {
        MemoryGeneration generation{MemoryGeneration::ShortLived};

        std::size_t slot_size{0};
        std::size_t initial_slots{0};
        std::size_t max_slots{0};

        std::size_t capacity_slots{0};
        std::size_t in_use_slots{0};
        std::size_t in_use_high_watermark{0};

        std::size_t allocation_requests{0};
        std::size_t allocation_hits{0};

        std::size_t hot_expand_count{0};
        std::size_t cold_shrink_count{0};
        std::size_t cold_idle_ticks{0};

        std::unordered_map<BlockHeader*, std::unique_ptr<unsigned char[]>> blocks;
        std::vector<BlockHeader*> free_list;
    };

    struct AllocationRecord {
        BlockHeader* header{nullptr};
        MemoryGeneration generation{MemoryGeneration::ShortLived};
        std::size_t requested_bytes{0};
        bool pooled{false};
    };

    static GenerationPool make_generation_pool(
        MemoryGeneration generation,
        std::size_t slot_size,
        std::size_t initial_slots,
        std::size_t max_slots
    );

    void bootstrap_pool(GenerationPool& pool);
    void grow_pool(GenerationPool& pool, std::size_t slots_to_add);
    std::size_t shrink_pool(GenerationPool& pool, std::size_t max_slots_to_remove);

    GenerationPool& generation_pool(MemoryGeneration generation);
    const GenerationPool& generation_pool(MemoryGeneration generation) const;

    void maybe_hot_expand(GenerationPool& pool);
    void maybe_cold_shrink(GenerationPool& pool);

    void on_allocation_locked(std::size_t requested_bytes);
    void on_deallocation_locked(std::size_t released_bytes);

    bool validate_block_locked(const AllocationRecord& record, void* payload) noexcept;
    static std::size_t clamp_capacity(std::size_t value, std::size_t min_value, std::size_t max_value);

    AdaptiveMemoryPoolOptions options_{};

    GenerationPool* short_pool_{nullptr};
    GenerationPool* long_pool_{nullptr};

    mutable std::mutex mutex_;

    std::uint64_t allocation_seq_{0};

    std::size_t total_allocations_{0};
    std::size_t total_frees_{0};

    std::size_t fallback_allocations_{0};
    std::size_t fallback_in_use_{0};

    std::size_t active_bytes_{0};
    std::size_t active_bytes_high_watermark_{0};

    std::size_t guard_violations_{0};
    std::size_t invalid_frees_{0};
    std::uint64_t lock_wait_ns_total_{0};
    std::size_t lock_wait_samples_{0};

    std::unordered_map<void*, AllocationRecord> active_allocations_;

    // fallback 分配块 owner（仅用于不命中分池的场景）。
    std::unordered_map<BlockHeader*, std::unique_ptr<unsigned char[]>> fallback_blocks_;

    GenerationPool short_pool_storage_;
    GenerationPool long_pool_storage_;
};

}  // namespace rpc::infra
