#include "rpc/infra/memory_pool.h"

// 文件用途：
// 实现分代式自适应内存池：
// 1) 短生命周期/长生命周期分池
// 2) 热扩容与冷缩容
// 3) 越界检测与泄漏检测
// 4) 指标快照导出

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <utility>

namespace rpc::infra {

namespace {

constexpr std::uint64_t kFrontGuard = 0xF1357AAAF1357AAAULL;
constexpr std::uint64_t kTailGuard = 0x0DDC0FFE0DDC0FFEULL;

constexpr std::uint32_t kBlockFlagPooled = 1U << 0;

std::mutex g_global_pool_mutex;
std::unique_ptr<AdaptiveMemoryPool> g_global_pool;

std::size_t safe_bytes(std::size_t bytes) {
    return bytes == 0 ? 1 : bytes;
}

double clamp_rate(double value) {
    if (value < 0.0) {
        return 0.0;
    }
    if (value > 1.0) {
        return 1.0;
    }
    return value;
}

void write_tail_guard(unsigned char* payload, std::size_t logical_size) {
    if (payload == nullptr) {
        return;
    }
    const std::uint64_t guard = kTailGuard;
    std::memcpy(payload + logical_size, &guard, sizeof(guard));
}

bool check_tail_guard(const unsigned char* payload, std::size_t logical_size) {
    if (payload == nullptr) {
        return false;
    }
    std::uint64_t guard = 0;
    std::memcpy(&guard, payload + logical_size, sizeof(guard));
    return guard == kTailGuard;
}

}  // namespace

AdaptiveMemoryPool::GenerationPool AdaptiveMemoryPool::make_generation_pool(
    MemoryGeneration generation,
    std::size_t slot_size,
    std::size_t initial_slots,
    std::size_t max_slots
) {
    GenerationPool pool;
    pool.generation = generation;
    pool.slot_size = std::max<std::size_t>(1, slot_size);
    pool.initial_slots = clamp_capacity(initial_slots, 1, std::max<std::size_t>(1, max_slots));
    pool.max_slots = std::max<std::size_t>(pool.initial_slots, max_slots);
    return pool;
}

AdaptiveMemoryPool::AdaptiveMemoryPool(AdaptiveMemoryPoolOptions options) : options_(std::move(options)) {
    options_.short_slot_size = std::max<std::size_t>(1, options_.short_slot_size);
    options_.long_slot_size = std::max<std::size_t>(1, options_.long_slot_size);

    options_.short_max_slots = std::max<std::size_t>(1, options_.short_max_slots);
    options_.long_max_slots = std::max<std::size_t>(1, options_.long_max_slots);

    options_.short_initial_slots = clamp_capacity(options_.short_initial_slots, 1, options_.short_max_slots);
    options_.long_initial_slots = clamp_capacity(options_.long_initial_slots, 1, options_.long_max_slots);

    options_.hot_expand_utilization = clamp_rate(options_.hot_expand_utilization);
    if (options_.hot_expand_utilization <= 0.0) {
        options_.hot_expand_utilization = 0.85;
    }

    options_.cold_shrink_utilization = clamp_rate(options_.cold_shrink_utilization);

    options_.expand_step_min_slots = std::max<std::size_t>(1, options_.expand_step_min_slots);
    options_.shrink_step_min_slots = std::max<std::size_t>(1, options_.shrink_step_min_slots);
    options_.cold_shrink_idle_ticks = std::max<std::size_t>(1, options_.cold_shrink_idle_ticks);

    short_pool_storage_ = make_generation_pool(
        MemoryGeneration::ShortLived,
        options_.short_slot_size,
        options_.short_initial_slots,
        options_.short_max_slots
    );
    long_pool_storage_ = make_generation_pool(
        MemoryGeneration::LongLived,
        options_.long_slot_size,
        options_.long_initial_slots,
        options_.long_max_slots
    );

    short_pool_ = &short_pool_storage_;
    long_pool_ = &long_pool_storage_;

    bootstrap_pool(*short_pool_);
    bootstrap_pool(*long_pool_);
}

AdaptiveMemoryPool::~AdaptiveMemoryPool() {
    const LeakReport leaks = detect_leaks(8);
    if (leaks.leaked_allocations == 0) {
        return;
    }

    std::cerr << "[memory_pool] leak_detected"
              << " leaked_allocations=" << leaks.leaked_allocations
              << " leaked_bytes=" << leaks.leaked_bytes
              << '\n';
}

void AdaptiveMemoryPool::bootstrap_pool(GenerationPool& pool) {
    grow_pool(pool, pool.initial_slots);
}

void AdaptiveMemoryPool::grow_pool(GenerationPool& pool, std::size_t slots_to_add) {
    if (slots_to_add == 0 || pool.capacity_slots >= pool.max_slots) {
        return;
    }

    const std::size_t room = pool.max_slots - pool.capacity_slots;
    const std::size_t actual_add = std::min(slots_to_add, room);
    if (actual_add == 0) {
        return;
    }

    const std::size_t payload_bytes = safe_bytes(pool.slot_size);
    const std::size_t block_bytes = sizeof(BlockHeader) + payload_bytes + sizeof(std::uint64_t);

    for (std::size_t i = 0; i < actual_add; ++i) {
        auto raw = std::make_unique<unsigned char[]>(block_bytes);
        auto* header = reinterpret_cast<BlockHeader*>(raw.get());

        header->front_guard = kFrontGuard;
        header->allocation_id = 0;
        header->flags = kBlockFlagPooled;
        header->generation = static_cast<std::uint32_t>(pool.generation);
        header->slot_size = payload_bytes;
        header->requested_size = 0;

        unsigned char* payload = reinterpret_cast<unsigned char*>(header) + sizeof(BlockHeader);
        if (options_.enable_guard_checks) {
            write_tail_guard(payload, payload_bytes);
        }

        pool.free_list.push_back(header);
        pool.blocks.emplace(header, std::move(raw));
        ++pool.capacity_slots;
    }
}

std::size_t AdaptiveMemoryPool::shrink_pool(GenerationPool& pool, std::size_t max_slots_to_remove) {
    if (max_slots_to_remove == 0 || pool.capacity_slots <= pool.initial_slots) {
        return 0;
    }

    std::size_t removed = 0;
    while (removed < max_slots_to_remove
           && pool.capacity_slots > pool.initial_slots
           && !pool.free_list.empty()) {
        BlockHeader* header = pool.free_list.back();
        pool.free_list.pop_back();

        const std::size_t erased = pool.blocks.erase(header);
        if (erased == 0) {
            continue;
        }

        ++removed;
        --pool.capacity_slots;
    }

    return removed;
}

AdaptiveMemoryPool::GenerationPool& AdaptiveMemoryPool::generation_pool(MemoryGeneration generation) {
    return generation == MemoryGeneration::LongLived ? *long_pool_ : *short_pool_;
}

const AdaptiveMemoryPool::GenerationPool& AdaptiveMemoryPool::generation_pool(MemoryGeneration generation) const {
    return generation == MemoryGeneration::LongLived ? *long_pool_ : *short_pool_;
}

void AdaptiveMemoryPool::maybe_hot_expand(GenerationPool& pool) {
    if (pool.capacity_slots >= pool.max_slots || pool.capacity_slots == 0) {
        return;
    }

    const double utilization = static_cast<double>(pool.in_use_slots) / static_cast<double>(pool.capacity_slots);
    if (utilization < options_.hot_expand_utilization) {
        return;
    }

    const std::size_t before = pool.capacity_slots;
    const std::size_t step = std::max(options_.expand_step_min_slots, pool.capacity_slots / 2);
    grow_pool(pool, step);
    if (pool.capacity_slots > before) {
        ++pool.hot_expand_count;
    }
}

void AdaptiveMemoryPool::maybe_cold_shrink(GenerationPool& pool) {
    if (pool.capacity_slots <= pool.initial_slots || pool.capacity_slots == 0) {
        pool.cold_idle_ticks = 0;
        return;
    }

    const double utilization = static_cast<double>(pool.in_use_slots) / static_cast<double>(pool.capacity_slots);
    if (utilization > options_.cold_shrink_utilization) {
        pool.cold_idle_ticks = 0;
        return;
    }

    ++pool.cold_idle_ticks;
    if (pool.cold_idle_ticks < options_.cold_shrink_idle_ticks) {
        return;
    }

    const std::size_t required_capacity = std::max(pool.initial_slots, pool.in_use_slots);
    if (pool.capacity_slots <= required_capacity) {
        pool.cold_idle_ticks = 0;
        return;
    }

    const std::size_t removable = pool.capacity_slots - required_capacity;
    const std::size_t remove_goal = std::max(options_.shrink_step_min_slots, pool.capacity_slots / 4);
    const std::size_t removed = shrink_pool(pool, std::min(removable, remove_goal));
    if (removed > 0) {
        ++pool.cold_shrink_count;
    }
    pool.cold_idle_ticks = 0;
}

void AdaptiveMemoryPool::on_allocation_locked(std::size_t requested_bytes) {
    ++total_allocations_;
    active_bytes_ += requested_bytes;
    if (active_bytes_ > active_bytes_high_watermark_) {
        active_bytes_high_watermark_ = active_bytes_;
    }
}

void AdaptiveMemoryPool::on_deallocation_locked(std::size_t released_bytes) {
    ++total_frees_;
    if (released_bytes >= active_bytes_) {
        active_bytes_ = 0;
    } else {
        active_bytes_ -= released_bytes;
    }
}

bool AdaptiveMemoryPool::validate_block_locked(const AllocationRecord& record, void* payload) noexcept {
    if (!options_.enable_guard_checks || payload == nullptr || record.header == nullptr) {
        return true;
    }

    bool valid = true;
    if (record.header->front_guard != kFrontGuard) {
        ++guard_violations_;
        valid = false;
    }

    const auto* payload_bytes = static_cast<const unsigned char*>(payload);
    if (!check_tail_guard(payload_bytes, record.requested_bytes)) {
        ++guard_violations_;
        valid = false;
    }

    return valid;
}

std::size_t AdaptiveMemoryPool::clamp_capacity(std::size_t value, std::size_t min_value, std::size_t max_value) {
    if (min_value > max_value) {
        return min_value;
    }
    if (value < min_value) {
        return min_value;
    }
    if (value > max_value) {
        return max_value;
    }
    return value;
}

void* AdaptiveMemoryPool::allocate(std::size_t bytes, MemoryGeneration generation) {
    const std::size_t requested = safe_bytes(bytes);
    const auto lock_wait_started = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    const auto lock_wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - lock_wait_started
    ).count();
    lock_wait_ns_total_ += static_cast<std::uint64_t>(lock_wait_ns);
    ++lock_wait_samples_;

    GenerationPool& pool = generation_pool(generation);
    ++pool.allocation_requests;

    if (requested <= pool.slot_size) {
        if (pool.free_list.empty()) {
            const std::size_t step = std::max(options_.expand_step_min_slots, std::max<std::size_t>(1, pool.capacity_slots / 2));
            const std::size_t before = pool.capacity_slots;
            grow_pool(pool, step);
            if (pool.capacity_slots > before) {
                ++pool.hot_expand_count;
            }
        }

        if (!pool.free_list.empty()) {
            BlockHeader* header = pool.free_list.back();
            pool.free_list.pop_back();

            ++pool.allocation_hits;
            ++pool.in_use_slots;
            if (pool.in_use_slots > pool.in_use_high_watermark) {
                pool.in_use_high_watermark = pool.in_use_slots;
            }

            header->front_guard = kFrontGuard;
            header->allocation_id = ++allocation_seq_;
            header->flags = kBlockFlagPooled;
            header->generation = static_cast<std::uint32_t>(pool.generation);
            header->slot_size = pool.slot_size;
            header->requested_size = requested;

            unsigned char* payload = reinterpret_cast<unsigned char*>(header) + sizeof(BlockHeader);
            if (options_.enable_guard_checks) {
                write_tail_guard(payload, requested);
            }

            active_allocations_[payload] = AllocationRecord{header, generation, requested, true};
            on_allocation_locked(requested);
            maybe_hot_expand(pool);
            return payload;
        }
    }

    const std::size_t payload_bytes = requested;
    const std::size_t block_bytes = sizeof(BlockHeader) + payload_bytes + sizeof(std::uint64_t);

    auto raw = std::make_unique<unsigned char[]>(block_bytes);
    auto* header = reinterpret_cast<BlockHeader*>(raw.get());
    header->front_guard = kFrontGuard;
    header->allocation_id = ++allocation_seq_;
    header->flags = 0;
    header->generation = static_cast<std::uint32_t>(generation);
    header->slot_size = payload_bytes;
    header->requested_size = requested;

    unsigned char* payload = reinterpret_cast<unsigned char*>(header) + sizeof(BlockHeader);
    if (options_.enable_guard_checks) {
        write_tail_guard(payload, requested);
    }

    fallback_blocks_[header] = std::move(raw);
    ++fallback_allocations_;
    ++fallback_in_use_;

    active_allocations_[payload] = AllocationRecord{header, generation, requested, false};
    on_allocation_locked(requested);
    return payload;
}

bool AdaptiveMemoryPool::deallocate(void* ptr) noexcept {
    if (ptr == nullptr) {
        return true;
    }

    const auto lock_wait_started = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    const auto lock_wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - lock_wait_started
    ).count();
    lock_wait_ns_total_ += static_cast<std::uint64_t>(lock_wait_ns);
    ++lock_wait_samples_;
    const auto it = active_allocations_.find(ptr);
    if (it == active_allocations_.end()) {
        ++invalid_frees_;
        return false;
    }

    const AllocationRecord record = it->second;
    bool valid = validate_block_locked(record, ptr);

    active_allocations_.erase(it);
    on_deallocation_locked(record.requested_bytes);

    if (record.pooled) {
        GenerationPool& pool = generation_pool(record.generation);
        if (pool.in_use_slots > 0) {
            --pool.in_use_slots;
        }

        if (record.header != nullptr) {
            record.header->requested_size = 0;
            pool.free_list.push_back(record.header);
        }

        maybe_cold_shrink(pool);
        return valid;
    }

    if (record.header != nullptr) {
        fallback_blocks_.erase(record.header);
    }
    if (fallback_in_use_ > 0) {
        --fallback_in_use_;
    }

    return valid;
}

void AdaptiveMemoryPool::maintenance_tick() {
    const auto lock_wait_started = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    const auto lock_wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - lock_wait_started
    ).count();
    lock_wait_ns_total_ += static_cast<std::uint64_t>(lock_wait_ns);
    ++lock_wait_samples_;
    maybe_cold_shrink(*short_pool_);
    maybe_cold_shrink(*long_pool_);
}

MemoryPoolStats AdaptiveMemoryPool::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto fill_generation_stats = [](const GenerationPool& pool, MemoryPoolGenerationStats* out) {
        if (out == nullptr) {
            return;
        }
        out->slot_size = pool.slot_size;
        out->capacity_slots = pool.capacity_slots;
        out->in_use_slots = pool.in_use_slots;
        out->in_use_high_watermark = pool.in_use_high_watermark;

        out->allocation_requests = pool.allocation_requests;
        out->allocation_hits = pool.allocation_hits;

        out->hot_expand_count = pool.hot_expand_count;
        out->cold_shrink_count = pool.cold_shrink_count;

        out->utilization = pool.capacity_slots == 0
            ? 0.0
            : static_cast<double>(pool.in_use_slots) / static_cast<double>(pool.capacity_slots);

        out->hit_ratio = pool.allocation_requests == 0
            ? 0.0
            : static_cast<double>(pool.allocation_hits) / static_cast<double>(pool.allocation_requests);
    };

    MemoryPoolStats snapshot;
    fill_generation_stats(*short_pool_, &snapshot.short_lived);
    fill_generation_stats(*long_pool_, &snapshot.long_lived);

    snapshot.total_allocations = total_allocations_;
    snapshot.total_frees = total_frees_;

    snapshot.fallback_allocations = fallback_allocations_;
    snapshot.fallback_in_use = fallback_in_use_;

    snapshot.active_allocations = active_allocations_.size();
    snapshot.active_bytes = active_bytes_;
    snapshot.active_bytes_high_watermark = active_bytes_high_watermark_;

    snapshot.guard_violations = guard_violations_;
    snapshot.invalid_frees = invalid_frees_;
    snapshot.lock_wait_ns_total = lock_wait_ns_total_;
    snapshot.lock_wait_samples = lock_wait_samples_;
    snapshot.lock_wait_ns_avg = lock_wait_samples_ == 0
        ? 0
        : (lock_wait_ns_total_ / lock_wait_samples_);

    snapshot.leak_suspects = active_allocations_.size();
    return snapshot;
}

LeakReport AdaptiveMemoryPool::detect_leaks(std::size_t max_records) const {
    std::lock_guard<std::mutex> lock(mutex_);

    LeakReport report;
    report.leaked_allocations = active_allocations_.size();

    const std::size_t cap = std::max<std::size_t>(1, max_records);
    report.records.reserve(std::min(report.leaked_allocations, cap));

    for (const auto& [payload, record] : active_allocations_) {
        (void)payload;
        report.leaked_bytes += record.requested_bytes;

        if (report.records.size() >= cap) {
            continue;
        }

        LeakRecord leak;
        leak.allocation_id = record.header == nullptr ? 0 : record.header->allocation_id;
        leak.generation = record.generation;
        leak.requested_bytes = record.requested_bytes;
        leak.pooled = record.pooled;
        report.records.push_back(leak);
    }

    return report;
}

AdaptiveMemoryPool& AdaptiveMemoryPool::instance() {
    std::lock_guard<std::mutex> lock(g_global_pool_mutex);
    if (!g_global_pool) {
        g_global_pool = std::make_unique<AdaptiveMemoryPool>();
    }
    return *g_global_pool;
}

void AdaptiveMemoryPool::initialize_global(AdaptiveMemoryPoolOptions options) {
    std::lock_guard<std::mutex> lock(g_global_pool_mutex);
    g_global_pool = std::make_unique<AdaptiveMemoryPool>(std::move(options));
}

}  // namespace rpc::infra
