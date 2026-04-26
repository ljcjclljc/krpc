#pragma once

// 文件用途：
// 提供结构化日志与采样策略：
// 1) JSON 行日志
// 2) 按级别采样
// 3) 慢请求强制采样
// 4) 采样统计观测

#include <cstddef>
#include <cstdint>
#include <string_view>

#include <boost/json/object.hpp>

namespace rpc::infra {

enum class LogLevel : std::uint8_t {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
};

struct LogSamplingConfig {
    double debug_sample_rate{0.0};
    double info_sample_rate{0.10};
    double warn_sample_rate{1.0};
    double error_sample_rate{1.0};

    std::uint64_t slow_request_threshold_ms{80};
};

struct StructuredLogStats {
    std::size_t emitted_total{0};
    std::size_t dropped_total{0};
    std::size_t forced_by_slow_total{0};

    std::size_t debug_emitted{0};
    std::size_t info_emitted{0};
    std::size_t warn_emitted{0};
    std::size_t error_emitted{0};
};

void configure_structured_log(LogSamplingConfig config);
LogSamplingConfig structured_log_config();

StructuredLogStats structured_log_stats();
void reset_structured_log_stats();

const char* log_level_name(LogLevel level) noexcept;

bool should_sample_log(
    LogLevel level,
    std::string_view trace_id,
    std::string_view event,
    std::uint64_t latency_ms = 0,
    bool force = false
);

void structured_log(
    LogLevel level,
    std::string_view event,
    boost::json::object fields,
    std::string_view trace_id = {},
    std::uint64_t latency_ms = 0,
    bool force = false
);

}  // namespace rpc::infra
