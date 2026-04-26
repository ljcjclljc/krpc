#include "rpc/infra/structured_log.h"

// 文件用途：
// 实现结构化日志能力：
// 1) JSON 行输出
// 2) 级别采样
// 3) 慢请求强制采样
// 4) 采样统计

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>

#include <boost/functional/hash.hpp>
#include <boost/json/serialize.hpp>

#include "rpc/infra/memory_pool.h"

namespace rpc::infra {

namespace {

struct SamplingDecision {
    bool sampled{true};
    bool forced_by_slow{false};
};

struct LogRuntimeState {
    LogSamplingConfig config{};
    StructuredLogStats stats{};
    std::mutex state_mutex;
    std::mutex output_mutex;
};

LogRuntimeState g_log_runtime;

std::uint64_t unix_time_ms() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count()
    );
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

double sample_rate_for(const LogSamplingConfig& config, LogLevel level) {
    switch (level) {
        case LogLevel::Debug:
            return clamp_rate(config.debug_sample_rate);
        case LogLevel::Info:
            return clamp_rate(config.info_sample_rate);
        case LogLevel::Warn:
            return clamp_rate(config.warn_sample_rate);
        case LogLevel::Error:
            return clamp_rate(config.error_sample_rate);
        default:
            return 1.0;
    }
}

SamplingDecision decide_sampling(
    const LogSamplingConfig& config,
    LogLevel level,
    std::string_view trace_id,
    std::string_view event,
    std::uint64_t latency_ms,
    bool force
) {
    SamplingDecision decision;

    if (force) {
        decision.sampled = true;
        return decision;
    }

    if (latency_ms >= config.slow_request_threshold_ms && config.slow_request_threshold_ms > 0) {
        decision.sampled = true;
        decision.forced_by_slow = true;
        return decision;
    }

    if (level == LogLevel::Error) {
        decision.sampled = true;
        return decision;
    }

    const double rate = sample_rate_for(config, level);
    if (rate <= 0.0) {
        decision.sampled = false;
        return decision;
    }
    if (rate >= 1.0) {
        decision.sampled = true;
        return decision;
    }

    std::size_t seed = 0;
    boost::hash_combine(seed, static_cast<unsigned int>(level));
    boost::hash_combine(seed, std::string(event));
    boost::hash_combine(seed, std::string(trace_id));

    const std::size_t bucket = seed % 10000U;
    const std::size_t threshold = static_cast<std::size_t>(rate * 10000.0);
    decision.sampled = bucket < threshold;
    return decision;
}

void update_emitted_stats(LogLevel level, bool forced_by_slow) {
    std::lock_guard<std::mutex> lock(g_log_runtime.state_mutex);
    ++g_log_runtime.stats.emitted_total;
    if (forced_by_slow) {
        ++g_log_runtime.stats.forced_by_slow_total;
    }

    switch (level) {
        case LogLevel::Debug:
            ++g_log_runtime.stats.debug_emitted;
            break;
        case LogLevel::Info:
            ++g_log_runtime.stats.info_emitted;
            break;
        case LogLevel::Warn:
            ++g_log_runtime.stats.warn_emitted;
            break;
        case LogLevel::Error:
            ++g_log_runtime.stats.error_emitted;
            break;
        default:
            break;
    }
}

void update_dropped_stats() {
    std::lock_guard<std::mutex> lock(g_log_runtime.state_mutex);
    ++g_log_runtime.stats.dropped_total;
}

}  // namespace

void configure_structured_log(LogSamplingConfig config) {
    config.debug_sample_rate = clamp_rate(config.debug_sample_rate);
    config.info_sample_rate = clamp_rate(config.info_sample_rate);
    config.warn_sample_rate = clamp_rate(config.warn_sample_rate);
    config.error_sample_rate = clamp_rate(config.error_sample_rate);

    std::lock_guard<std::mutex> lock(g_log_runtime.state_mutex);
    g_log_runtime.config = config;
}

LogSamplingConfig structured_log_config() {
    std::lock_guard<std::mutex> lock(g_log_runtime.state_mutex);
    return g_log_runtime.config;
}

StructuredLogStats structured_log_stats() {
    std::lock_guard<std::mutex> lock(g_log_runtime.state_mutex);
    return g_log_runtime.stats;
}

void reset_structured_log_stats() {
    std::lock_guard<std::mutex> lock(g_log_runtime.state_mutex);
    g_log_runtime.stats = StructuredLogStats{};
}

const char* log_level_name(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Debug:
            return "debug";
        case LogLevel::Info:
            return "info";
        case LogLevel::Warn:
            return "warn";
        case LogLevel::Error:
            return "error";
        default:
            return "info";
    }
}

bool should_sample_log(
    LogLevel level,
    std::string_view trace_id,
    std::string_view event,
    std::uint64_t latency_ms,
    bool force
) {
    const LogSamplingConfig config = structured_log_config();
    return decide_sampling(config, level, trace_id, event, latency_ms, force).sampled;
}

void structured_log(
    LogLevel level,
    std::string_view event,
    boost::json::object fields,
    std::string_view trace_id,
    std::uint64_t latency_ms,
    bool force
) {
    const LogSamplingConfig config = structured_log_config();
    const SamplingDecision decision = decide_sampling(
        config,
        level,
        trace_id,
        event,
        latency_ms,
        force
    );
    if (!decision.sampled) {
        update_dropped_stats();
        return;
    }

    fields["ts_unix_ms"] = unix_time_ms();
    fields["level"] = log_level_name(level);
    fields["event"] = std::string(event);
    if (!trace_id.empty()) {
        fields["trace_id"] = std::string(trace_id);
    }
    if (latency_ms > 0) {
        fields["latency_ms"] = latency_ms;
    }

    const std::string serialized = boost::json::serialize(fields);

    AdaptiveMemoryPool& memory_pool = AdaptiveMemoryPool::instance();
    const MemoryGeneration generation = serialized.size() <= 512
        ? MemoryGeneration::ShortLived
        : MemoryGeneration::LongLived;

    void* scratch = memory_pool.allocate(serialized.size() + 1, generation);
    if (scratch != nullptr) {
        std::memcpy(scratch, serialized.data(), serialized.size());
        static_cast<char*>(scratch)[serialized.size()] = '\0';

        {
            std::lock_guard<std::mutex> lock(g_log_runtime.output_mutex);
            std::cout.write(static_cast<const char*>(scratch), static_cast<std::streamsize>(serialized.size()));
            std::cout.put('\n');
        }

        (void)memory_pool.deallocate(scratch);
    } else {
        std::lock_guard<std::mutex> lock(g_log_runtime.output_mutex);
        std::cout << serialized << '\n';
    }

    update_emitted_stats(level, decision.forced_by_slow);
}

}  // namespace rpc::infra
