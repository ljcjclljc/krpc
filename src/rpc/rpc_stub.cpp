#include "rpc/rpc/client.h"

// 文件用途：
// 提供 W13 阶段 RPC 客户端实现：
// - 基于实时状态的自适应负载均衡（CPU/内存/QPS/延迟加权）
// - 集成熔断状态机（closed/open/half-open）与自动恢复探测
// - 集成降级策略（缓存兜底/静态兜底）与流量灰度染色能力

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/functional/hash.hpp>
#include <boost/lexical_cast/try_lexical_convert.hpp>
#include <boost/json/object.hpp>

#include "rpc/infra/infra.h"
#include "rpc/infra/memory_pool.h"
#include "rpc/infra/structured_log.h"
#include "rpc/net/net.h"
#include "rpc/rpc/krpc_channel.h"
#include "rpc/runtime/retry_budget.h"

namespace rpc::client {

namespace {

using Clock = std::chrono::steady_clock;

constexpr double kDefaultCpuWeight = 0.30;
constexpr double kDefaultMemWeight = 0.20;
constexpr double kDefaultQpsWeight = 0.20;
constexpr double kDefaultLatencyWeight = 0.30;

std::optional<std::string> config_value_if_any(const char* key) {
    if (key == nullptr) {
        return std::nullopt;
    }
    try {
        const ::rpc::infra::ConfigSnapshot snapshot = ::rpc::infra::config_repository().snapshot();
        const auto it = snapshot.values.find(key);
        if (it == snapshot.values.end() || it->second.empty()) {
            return std::nullopt;
        }
        return it->second;
    } catch (...) {
        return std::nullopt;
    }
}

std::size_t parse_config_size(const char* key, std::size_t default_value) {
    const std::optional<std::string> value = config_value_if_any(key);
    if (!value.has_value()) {
        return default_value;
    }

    unsigned long long parsed = 0;
    if (!boost::conversion::try_lexical_convert(*value, parsed)) {
        return default_value;
    }
    if (parsed > static_cast<unsigned long long>(std::numeric_limits<std::size_t>::max())) {
        return default_value;
    }
    return static_cast<std::size_t>(parsed);
}

std::uint64_t parse_config_u64(const char* key, std::uint64_t default_value) {
    const std::optional<std::string> value = config_value_if_any(key);
    if (!value.has_value()) {
        return default_value;
    }
    std::uint64_t parsed = 0;
    if (!boost::conversion::try_lexical_convert(*value, parsed)) {
        return default_value;
    }
    return parsed;
}

double parse_config_double(const char* key, double default_value) {
    const std::optional<std::string> value = config_value_if_any(key);
    if (!value.has_value()) {
        return default_value;
    }
    double parsed = 0.0;
    if (!boost::conversion::try_lexical_convert(*value, parsed)) {
        return default_value;
    }
    return parsed;
}

std::size_t parse_metadata_size(
    const RpcRequest& request,
    const char* key,
    std::size_t default_value
) {
    const auto it = request.metadata.find(key);
    if (it == request.metadata.end() || it->second.empty()) {
        return default_value;
    }

    unsigned long long parsed = 0;
    if (!boost::conversion::try_lexical_convert(it->second, parsed)) {
        return default_value;
    }
    if (parsed > static_cast<unsigned long long>(std::numeric_limits<std::size_t>::max())) {
        return default_value;
    }
    return static_cast<std::size_t>(parsed);
}

std::uint64_t parse_metadata_u64(
    const RpcRequest& request,
    const char* key,
    std::uint64_t default_value
) {
    const auto it = request.metadata.find(key);
    if (it == request.metadata.end() || it->second.empty()) {
        return default_value;
    }

    std::uint64_t parsed = 0;
    if (!boost::conversion::try_lexical_convert(it->second, parsed)) {
        return default_value;
    }
    return parsed;
}

bool parse_metadata_bool(
    const RpcRequest& request,
    const char* key,
    bool default_value
) {
    const auto it = request.metadata.find(key);
    if (it == request.metadata.end() || it->second.empty()) {
        return default_value;
    }

    std::string lowered = it->second;
    boost::algorithm::trim(lowered);
    boost::algorithm::to_lower(lowered);
    if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on") {
        return true;
    }
    if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "off") {
        return false;
    }
    return default_value;
}

double parse_metadata_double(
    const RpcRequest& request,
    const char* key,
    double default_value
) {
    const auto it = request.metadata.find(key);
    if (it == request.metadata.end() || it->second.empty()) {
        return default_value;
    }

    double parsed = 0.0;
    if (!boost::conversion::try_lexical_convert(it->second, parsed)) {
        return default_value;
    }
    return parsed;
}

std::size_t parse_metadata_or_config_size(
    const RpcRequest& request,
    const char* metadata_key,
    const char* config_key,
    std::size_t default_value
) {
    return parse_metadata_size(
        request,
        metadata_key,
        parse_config_size(config_key, default_value)
    );
}

std::uint64_t parse_metadata_or_config_u64(
    const RpcRequest& request,
    const char* metadata_key,
    const char* config_key,
    std::uint64_t default_value
) {
    return parse_metadata_u64(
        request,
        metadata_key,
        parse_config_u64(config_key, default_value)
    );
}

double parse_metadata_or_config_double(
    const RpcRequest& request,
    const char* metadata_key,
    const char* config_key,
    double default_value
) {
    return parse_metadata_double(
        request,
        metadata_key,
        parse_config_double(config_key, default_value)
    );
}

std::string lower_ascii_copy(std::string value) {
    boost::algorithm::to_lower(value);
    return value;
}

std::string metadata_value(const RpcRequest& request, const char* key) {
    const auto it = request.metadata.find(key);
    if (it == request.metadata.end()) {
        return {};
    }
    return it->second;
}

double clamp01(double value) {
    if (value < 0.0) {
        return 0.0;
    }
    if (value > 1.0) {
        return 1.0;
    }
    return value;
}

double ewma(double current, double sample, double alpha) {
    if (current < 0.0) {
        return sample;
    }
    return alpha * sample + (1.0 - alpha) * current;
}

std::string node_key(const ServiceNode& node) {
    if (!node.id.empty()) {
        return node.id;
    }
    return node.host + ":" + std::to_string(node.port);
}

std::string node_endpoint(const ServiceNode& node) {
    return node.host + ":" + std::to_string(node.port);
}

std::string node_lane(const ServiceNode& node) {
    const auto lane_it = node.labels.find("lane");
    if (lane_it != node.labels.end() && !lane_it->second.empty()) {
        return lane_it->second;
    }
    const auto tag_it = node.labels.find("tag");
    if (tag_it != node.labels.end() && !tag_it->second.empty()) {
        return tag_it->second;
    }
    return "stable";
}

std::string circuit_state_to_string(CircuitBreakerState state) {
    switch (state) {
        case CircuitBreakerState::Closed:
            return "closed";
        case CircuitBreakerState::Open:
            return "open";
        case CircuitBreakerState::HalfOpen:
            return "half-open";
        default:
            return "closed";
    }
}

bool is_retryable_failure_code(int code) {
    return code == 429 || code == 503 || code == 504 || (code >= 500 && code < 600);
}

::rpc::rpc::KrpcCodec parse_codec(const RpcRequest& request) {
    const auto it = request.metadata.find("x-krpc-codec");
    if (it == request.metadata.end() || it->second.empty()) {
        return ::rpc::rpc::KrpcCodec::Raw;
    }

    const std::string lowered = lower_ascii_copy(it->second);
    if (lowered == "protobuf" || lowered == "proto" || lowered == "pb") {
        return ::rpc::rpc::KrpcCodec::Protobuf;
    }
    return ::rpc::rpc::KrpcCodec::Raw;
}

std::string find_trace_id(const RpcRequest& request) {
    const auto x_trace_it = request.metadata.find("x-trace-id");
    if (x_trace_it != request.metadata.end() && !x_trace_it->second.empty()) {
        return x_trace_it->second;
    }

    const auto trace_it = request.metadata.find("trace_id");
    if (trace_it != request.metadata.end() && !trace_it->second.empty()) {
        return trace_it->second;
    }
    return {};
}

std::string find_span_id(const RpcRequest& request) {
    const auto x_span_it = request.metadata.find("x-span-id");
    if (x_span_it != request.metadata.end() && !x_span_it->second.empty()) {
        return x_span_it->second;
    }

    const auto span_it = request.metadata.find("span_id");
    if (span_it != request.metadata.end() && !span_it->second.empty()) {
        return span_it->second;
    }
    return {};
}

std::mutex g_client_mutex;
std::shared_ptr<IRpcClient> g_default_client;
std::mutex g_audit_mutex;
std::mutex g_lb_snapshot_mutex;
std::mutex g_lb_runtime_mutex;
RpcInvokeAuditSnapshot g_last_invoke_audit;
LoadBalancerDecisionSnapshot g_last_lb_snapshot;
LoadBalancerRuntimeStats g_last_lb_runtime_stats;

class InMemoryServiceDiscovery final : public IServiceDiscovery {
public:
    InMemoryServiceDiscovery() {
        services_.emplace(
            "gateway.backend",
            std::vector<ServiceNode>{
                ServiceNode{
                    "node-a",
                    "127.0.0.1",
                    9000,
                    {{"lane", "stable"}, {"az", "a"}},
                    0.28,
                    0.34,
                    120.0,
                    24.0,
                },
                ServiceNode{
                    "node-b",
                    "127.0.0.1",
                    9001,
                    {{"lane", "gray"}, {"az", "b"}},
                    0.72,
                    0.75,
                    280.0,
                    110.0,
                },
            }
        );

        services_.emplace(
            "svc.echo",
            std::vector<ServiceNode>{
                ServiceNode{"a", "10.0.0.1", 8080, {{"lane", "stable"}}, 0.25, 0.23, 90.0, 18.0},
                ServiceNode{"b", "10.0.0.2", 8081, {{"lane", "gray"}}, 0.63, 0.60, 210.0, 84.0},
            }
        );
    }

    std::vector<ServiceNode> list_nodes(const std::string& service_name) override {
        const auto it = services_.find(service_name);
        if (it == services_.end()) {
            return {};
        }
        return it->second;
    }

private:
    std::unordered_map<std::string, std::vector<ServiceNode>> services_;
};

class AdaptiveLoadBalancer final : public ILoadBalancer, public ILoadBalancerRuntimeFeedback {
public:
    std::size_t select_node(
        const std::vector<ServiceNode>& nodes,
        const RpcRequest& request
    ) override {
        if (nodes.empty()) {
            return 0;
        }

        const Clock::time_point lock_wait_started = Clock::now();
        std::lock_guard<std::mutex> lock(mutex_);
        const std::uint64_t lock_wait_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - lock_wait_started).count()
        );
        runtime_stats_.select_lock_wait_ns_total += lock_wait_ns;
        ++runtime_stats_.select_calls;
        const Clock::time_point now = Clock::now();

        LoadBalancerDecisionSnapshot decision;
        decision.has_selection = false;

        const std::string gray_tag = metadata_value(request, "x-gray-tag").empty()
            ? std::string("gray")
            : metadata_value(request, "x-gray-tag");
        const std::string stable_tag = metadata_value(request, "x-stable-tag").empty()
            ? std::string("stable")
            : metadata_value(request, "x-stable-tag");

        std::string preferred_lane = metadata_value(request, "x-traffic-tag");
        bool gray_routed = false;
        if (preferred_lane.empty()) {
            const double gray_percent = std::clamp(
                parse_metadata_or_config_double(
                    request,
                    "x-gray-percent",
                    "rpc.gray.percent",
                    0.0
                ),
                0.0,
                100.0
            );
            if (gray_percent > 0.0) {
                std::string hash_source = metadata_value(request, "x-user-id");
                if (hash_source.empty()) {
                    hash_source = metadata_value(request, "x-request-id");
                }
                if (hash_source.empty()) {
                    hash_source = metadata_value(request, "x-trace-id");
                }
                if (hash_source.empty()) {
                    hash_source = request.payload;
                }
                const std::size_t bucket = boost::hash<std::string>{}(hash_source) % 100U;
                if (static_cast<double>(bucket) < gray_percent) {
                    preferred_lane = gray_tag;
                    gray_routed = true;
                } else {
                    preferred_lane = stable_tag;
                }
            }
        } else {
            gray_routed = (preferred_lane == gray_tag);
        }

        boost::container::small_vector<std::size_t, 16> candidate_indices;
        candidate_indices.reserve(nodes.size());

        if (!preferred_lane.empty()) {
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                if (node_lane(nodes[i]) == preferred_lane) {
                    candidate_indices.push_back(i);
                }
            }
        }
        if (candidate_indices.empty()) {
            candidate_indices.resize(nodes.size());
            for (std::size_t i = 0; i < nodes.size(); ++i) {
                candidate_indices[i] = i;
            }
            preferred_lane = "mixed";
        }

        double cpu_weight = parse_metadata_or_config_double(
            request,
            "x-lb-weight-cpu",
            "rpc.lb.weight.cpu",
            kDefaultCpuWeight
        );
        double mem_weight = parse_metadata_or_config_double(
            request,
            "x-lb-weight-mem",
            "rpc.lb.weight.mem",
            kDefaultMemWeight
        );
        double qps_weight = parse_metadata_or_config_double(
            request,
            "x-lb-weight-qps",
            "rpc.lb.weight.qps",
            kDefaultQpsWeight
        );
        double latency_weight = parse_metadata_or_config_double(
            request,
            "x-lb-weight-latency",
            "rpc.lb.weight.latency",
            kDefaultLatencyWeight
        );
        cpu_weight = std::max(0.0, cpu_weight);
        mem_weight = std::max(0.0, mem_weight);
        qps_weight = std::max(0.0, qps_weight);
        latency_weight = std::max(0.0, latency_weight);
        const double total_weight = cpu_weight + mem_weight + qps_weight + latency_weight;
        if (total_weight <= 0.0) {
            cpu_weight = kDefaultCpuWeight;
            mem_weight = kDefaultMemWeight;
            qps_weight = kDefaultQpsWeight;
            latency_weight = kDefaultLatencyWeight;
        } else {
            cpu_weight /= total_weight;
            mem_weight /= total_weight;
            qps_weight /= total_weight;
            latency_weight /= total_weight;
        }

        const double qps_target = std::max(1.0, parse_metadata_or_config_double(
            request,
            "x-lb-target-qps",
            "rpc.lb.target_qps",
            220.0
        ));
        const double latency_target_ms = std::max(1.0, parse_metadata_or_config_double(
            request,
            "x-lb-target-latency-ms",
            "rpc.lb.target_latency_ms",
            80.0
        ));

        double best_score = -1.0;
        std::optional<std::size_t> selected_index;
        CircuitBreakerState selected_circuit = CircuitBreakerState::Closed;

        for (const std::size_t idx : candidate_indices) {
            const ServiceNode& node = nodes[idx];
            NodeRuntimeState& state = states_[node_key(node)];
            merge_external_metrics(node, state);

            if (state.circuit_state == CircuitBreakerState::Open) {
                if (now >= state.open_until) {
                    state.circuit_state = CircuitBreakerState::HalfOpen;
                    state.half_open_successes = 0;
                    state.half_open_inflight = 0;
                } else {
                    continue;
                }
            }
            if (state.circuit_state == CircuitBreakerState::HalfOpen
                && state.half_open_inflight >= max_half_open_probes(request)) {
                continue;
            }

            const double cpu = metric_or_default(node.cpu_utilization, state.cpu_ewma, 0.45);
            const double mem = metric_or_default(node.memory_utilization, state.memory_ewma, 0.45);
            const double qps = metric_or_default(node.qps, state.qps_ewma, 0.0);
            const double latency = metric_or_default(node.latency_ms, state.latency_ewma, 0.0);

            const double cpu_penalty = clamp01(cpu);
            const double mem_penalty = clamp01(mem);
            const double qps_penalty = qps <= 0.0 ? 0.0 : qps / (qps + qps_target);
            const double latency_penalty = latency <= 0.0 ? 0.0 : latency / (latency + latency_target_ms);

            const double saturation_penalty = std::min(1.0, static_cast<double>(state.inflight) / 32.0);
            const double risk = cpu_weight * cpu_penalty
                + mem_weight * mem_penalty
                + qps_weight * qps_penalty
                + latency_weight * latency_penalty
                + 0.35 * clamp01(state.failure_ewma)
                + 0.10 * saturation_penalty;
            const double score = std::max(0.0, 1.0 - risk);

            if (!selected_index || score > best_score) {
                selected_index = idx;
                best_score = score;
                selected_circuit = state.circuit_state;
            }
        }

        if (!selected_index.has_value()) {
            decision.blocked_by_circuit_breaker = true;
            decision.gray_routed = gray_routed;
            decision.traffic_lane = preferred_lane;
            decision.circuit_state = CircuitBreakerState::Open;
            last_decision_ = decision;
            ++runtime_stats_.blocked_by_circuit_breaker;
            if (gray_routed) {
                ++runtime_stats_.gray_routed;
            }
            publish_runtime_stats_locked();
            return nodes.size();
        }

        NodeRuntimeState& selected_state = states_[node_key(nodes[*selected_index])];
        if (selected_state.circuit_state == CircuitBreakerState::HalfOpen) {
            ++selected_state.half_open_inflight;
        }
        ++selected_state.inflight;

        decision.has_selection = true;
        decision.gray_routed = gray_routed;
        decision.selected_node_id = nodes[*selected_index].id;
        decision.selected_endpoint = node_endpoint(nodes[*selected_index]);
        decision.traffic_lane = preferred_lane;
        decision.circuit_state = selected_circuit;
        decision.score = best_score;
        last_decision_ = decision;
        if (gray_routed) {
            ++runtime_stats_.gray_routed;
        }
        ++runtime_stats_.selected_endpoint_counts[decision.selected_endpoint];
        publish_runtime_stats_locked();
        return *selected_index;
    }

    void on_invoke_result(
        const ServiceNode& node,
        const RpcRequest& request,
        const RpcResponse& response,
        std::uint64_t observed_latency_ms
    ) override {
        const Clock::time_point lock_wait_started = Clock::now();
        std::lock_guard<std::mutex> lock(mutex_);
        const std::uint64_t lock_wait_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - lock_wait_started).count()
        );
        runtime_stats_.feedback_lock_wait_ns_total += lock_wait_ns;
        ++runtime_stats_.feedback_calls;
        const Clock::time_point now = Clock::now();
        NodeRuntimeState& state = states_[node_key(node)];
        merge_external_metrics(node, state);

        if (state.inflight > 0) {
            --state.inflight;
        }

        if (observed_latency_ms > 0) {
            const double latency_sample = static_cast<double>(observed_latency_ms);
            state.latency_ewma = ewma(state.latency_ewma, latency_sample, 0.20);
            state.latency_window.push_back(latency_sample);
        }

        if (state.last_invoke_time != Clock::time_point{}) {
            const auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - state.last_invoke_time
            ).count();
            if (delta > 0) {
                const double instant_qps = 1000.0 / static_cast<double>(delta);
                state.qps_ewma = ewma(state.qps_ewma, instant_qps, 0.18);
            }
        }
        state.last_invoke_time = now;

        const bool success = (response.code == 0);
        const double failure_sample = success ? 0.0 : 1.0;
        state.failure_ewma = ewma(state.failure_ewma, failure_sample, 0.28);

        const std::size_t failure_threshold = std::max<std::size_t>(1, parse_metadata_or_config_size(
            request,
            "x-cb-failure-threshold",
            "rpc.circuit.failure_threshold",
            3
        ));
        const std::uint64_t open_window_ms = std::max<std::uint64_t>(50, parse_metadata_or_config_u64(
            request,
            "x-cb-open-ms",
            "rpc.circuit.open_ms",
            280
        ));
        const std::size_t half_open_success_threshold = std::max<std::size_t>(1, parse_metadata_or_config_size(
            request,
            "x-cb-half-open-success",
            "rpc.circuit.half_open_success",
            2
        ));

        if (success) {
            state.consecutive_failures = 0;
            if (state.circuit_state == CircuitBreakerState::HalfOpen) {
                if (state.half_open_inflight > 0) {
                    --state.half_open_inflight;
                }
                ++state.half_open_successes;
                if (state.half_open_successes >= half_open_success_threshold) {
                    state.circuit_state = CircuitBreakerState::Closed;
                    state.half_open_successes = 0;
                    state.half_open_inflight = 0;
                }
            }
            publish_runtime_stats_locked();
            return;
        }

        if (state.circuit_state == CircuitBreakerState::HalfOpen) {
            if (state.half_open_inflight > 0) {
                --state.half_open_inflight;
            }
            state.circuit_state = CircuitBreakerState::Open;
            state.open_until = now + std::chrono::milliseconds(open_window_ms);
            state.half_open_successes = 0;
            state.consecutive_failures = failure_threshold;
            publish_runtime_stats_locked();
            return;
        }

        if (is_retryable_failure_code(response.code)) {
            ++state.consecutive_failures;
        }
        if (state.consecutive_failures >= failure_threshold) {
            state.circuit_state = CircuitBreakerState::Open;
            state.open_until = now + std::chrono::milliseconds(open_window_ms);
            state.half_open_successes = 0;
            state.half_open_inflight = 0;
        }
        publish_runtime_stats_locked();
    }

    LoadBalancerDecisionSnapshot last_decision_snapshot() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_decision_;
    }

private:
    void publish_runtime_stats_locked() {
        std::lock_guard<std::mutex> runtime_lock(g_lb_runtime_mutex);
        g_last_lb_runtime_stats = runtime_stats_;
    }

    struct NodeRuntimeState {
        double cpu_ewma{0.45};
        double memory_ewma{0.45};
        double qps_ewma{0.0};
        double latency_ewma{0.0};
        double failure_ewma{0.0};
        std::size_t inflight{0};
        std::size_t consecutive_failures{0};
        std::size_t half_open_successes{0};
        std::size_t half_open_inflight{0};
        CircuitBreakerState circuit_state{CircuitBreakerState::Closed};
        Clock::time_point open_until{};
        Clock::time_point last_invoke_time{};
        boost::circular_buffer<double> latency_window{32};
    };

    static std::size_t max_half_open_probes(const RpcRequest& request) {
        return std::max<std::size_t>(1, parse_metadata_or_config_size(
            request,
            "x-cb-half-open-max-probes",
            "rpc.circuit.half_open_max_probes",
            1
        ));
    }

    static double metric_or_default(double external, double internal, double fallback) {
        if (external >= 0.0) {
            return external;
        }
        if (internal >= 0.0) {
            return internal;
        }
        return fallback;
    }

    static void merge_external_metrics(const ServiceNode& node, NodeRuntimeState& state) {
        if (node.cpu_utilization >= 0.0) {
            state.cpu_ewma = ewma(state.cpu_ewma, clamp01(node.cpu_utilization), 0.35);
        }
        if (node.memory_utilization >= 0.0) {
            state.memory_ewma = ewma(state.memory_ewma, clamp01(node.memory_utilization), 0.35);
        }
        if (node.qps >= 0.0) {
            state.qps_ewma = ewma(state.qps_ewma, node.qps, 0.25);
        }
        if (node.latency_ms >= 0.0) {
            state.latency_ewma = ewma(state.latency_ewma, node.latency_ms, 0.25);
            state.latency_window.push_back(node.latency_ms);
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, NodeRuntimeState> states_;
    LoadBalancerDecisionSnapshot last_decision_;
    LoadBalancerRuntimeStats runtime_stats_;
};

class DefaultRpcClient final : public IRpcClient {
public:
    DefaultRpcClient(
        std::shared_ptr<IServiceDiscovery> discovery,
        std::shared_ptr<ILoadBalancer> load_balancer,
        ::rpc::runtime::RetryBudgetOptions retry_budget_options
    )
        : discovery_(std::move(discovery)),
          load_balancer_(std::move(load_balancer)),
          retry_budget_(retry_budget_options),
          channel_() {
        if (!discovery_ || !load_balancer_) {
            throw std::invalid_argument("rpc dependencies cannot be null");
        }
    }

    RpcResponse invoke(const RpcRequest& request) override {
        const Clock::time_point invoke_started = Clock::now();
        const std::string trace_id = find_trace_id(request);
        const std::string span_id = find_span_id(request);
        const ::rpc::rpc::KrpcCodec codec = parse_codec(request);
        auto* runtime_feedback = dynamic_cast<ILoadBalancerRuntimeFeedback*>(load_balancer_.get());

        const auto finalize_and_log = [&](RpcResponse response, const std::string& endpoint) -> RpcResponse {
            const ::rpc::runtime::RetryBudgetSnapshot budget_snapshot = retry_budget_.snapshot();
            response.retry_budget_request_count = budget_snapshot.request_count;
            response.retry_budget_retry_count = budget_snapshot.retry_count;
            response.retry_budget_max_tokens = budget_snapshot.max_retry_tokens;
            response.retry_budget_available_tokens = budget_snapshot.available_retry_tokens;

            if (response.cancelled && response.cancel_reason.empty()) {
                response.cancel_reason = "cancelled";
            }
            if (response.selected_endpoint.empty()) {
                response.selected_endpoint = endpoint;
            }

            if (runtime_feedback != nullptr) {
                const LoadBalancerDecisionSnapshot decision = runtime_feedback->last_decision_snapshot();
                {
                    std::lock_guard<std::mutex> lb_lock(g_lb_snapshot_mutex);
                    g_last_lb_snapshot = decision;
                }
                if (response.traffic_lane.empty()) {
                    response.traffic_lane = decision.traffic_lane;
                }
                if (response.circuit_state.empty()) {
                    response.circuit_state = circuit_state_to_string(decision.circuit_state);
                }
            }

            RpcInvokeAuditSnapshot snapshot;
            snapshot.trace_id = trace_id;
            snapshot.span_id = span_id;
            snapshot.service = request.service;
            snapshot.method = request.method;
            snapshot.endpoint = response.selected_endpoint;
            snapshot.code = response.code;
            snapshot.message = response.message;
            snapshot.attempts = response.attempts;
            snapshot.degraded = response.degraded;
            snapshot.degrade_strategy = response.degrade_strategy;
            snapshot.traffic_lane = response.traffic_lane;
            snapshot.circuit_state = response.circuit_state;

            {
                std::lock_guard<std::mutex> lock(g_audit_mutex);
                g_last_invoke_audit = snapshot;
            }

            const std::uint64_t total_latency_ms = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - invoke_started).count()
            );
            const ::rpc::infra::MemoryPoolStats memory_stats = ::rpc::infra::AdaptiveMemoryPool::instance().stats();

            boost::json::object log_fields;
            log_fields["span_id"] = snapshot.span_id;
            log_fields["service"] = snapshot.service;
            log_fields["method"] = snapshot.method;
            log_fields["endpoint"] = snapshot.endpoint;
            log_fields["code"] = snapshot.code;
            log_fields["message"] = snapshot.message;
            log_fields["attempts"] = snapshot.attempts;
            log_fields["degraded"] = snapshot.degraded;
            log_fields["degrade_strategy"] = snapshot.degrade_strategy;
            log_fields["traffic_lane"] = snapshot.traffic_lane;
            log_fields["circuit_state"] = snapshot.circuit_state;
            log_fields["mem_short_hit_ratio"] = memory_stats.short_lived.hit_ratio;
            log_fields["mem_long_hit_ratio"] = memory_stats.long_lived.hit_ratio;
            log_fields["mem_active_bytes_high_watermark"] = memory_stats.active_bytes_high_watermark;
            log_fields["slow_stage"] = "rpc.invoke";

            ::rpc::infra::structured_log(
                snapshot.code == 0
                    ? ::rpc::infra::LogLevel::Info
                    : (snapshot.code >= 500 ? ::rpc::infra::LogLevel::Error : ::rpc::infra::LogLevel::Warn),
                "rpc.invoke",
                std::move(log_fields),
                snapshot.trace_id,
                total_latency_ms
            );
            return response;
        };

        if (request.service.empty()) {
            return finalize_and_log(
                RpcResponse{400, "service name is empty", {}, 0, 0},
                ""
            );
        }

        const std::vector<ServiceNode> nodes = discovery_->list_nodes(request.service);
        if (nodes.empty()) {
            RpcResponse unavailable{503, "no available service nodes", {}, 0, 0};
            unavailable = apply_degrade_if_needed(request, unavailable);
            return finalize_and_log(unavailable, "");
        }

        std::size_t max_retries = request.max_retries;
        if (max_retries == 0) {
            max_retries = parse_metadata_or_config_size(
                request,
                "x-max-retries",
                "rpc.retry.max_retries",
                1
            );
        }

        retry_budget_.record_request();

        RpcResponse last_response{503, "upstream_error", {}, 0, 0};
        std::string last_endpoint;

        for (std::size_t attempt = 1;; ++attempt) {
            if (attempt > 1) {
                if (attempt - 1 > max_retries) {
                    if (last_response.code == 0) {
                        last_response.code = 503;
                        last_response.message = "retry_exhausted";
                    }
                    last_response = apply_degrade_if_needed(request, last_response);
                    return finalize_and_log(last_response, last_endpoint);
                }
                if (!retry_budget_.try_acquire_retry_token()) {
                    RpcResponse exhausted{
                        429,
                        "retry_budget_exhausted",
                        {},
                        attempt - 1,
                        last_response.effective_timeout_ms,
                    };
                    exhausted = apply_degrade_if_needed(request, exhausted);
                    return finalize_and_log(exhausted, last_endpoint);
                }
            }

            const std::size_t selected = load_balancer_->select_node(nodes, request);
            if (selected >= nodes.size()) {
                if (runtime_feedback == nullptr) {
                    const ServiceNode& fallback_node = nodes.front();
                    return invoke_single_node(
                        request,
                        codec,
                        fallback_node,
                        attempt,
                        &last_response,
                        &last_endpoint,
                        runtime_feedback,
                        finalize_and_log
                    );
                }

                RpcResponse unavailable{503, "no_healthy_service_nodes", {}, attempt - 1, 0};
                unavailable = apply_degrade_if_needed(request, unavailable);
                return finalize_and_log(unavailable, last_endpoint);
            }

            const ServiceNode& node = nodes[selected];
            const std::string endpoint = node_endpoint(node);
            last_endpoint = endpoint;

            ::rpc::rpc::KrpcRequestFrame channel_request;
            channel_request.service = request.service;
            channel_request.method = request.method;
            channel_request.payload = request.payload;
            channel_request.metadata = request.metadata;
            channel_request.timeout_ms = request.timeout_ms;
            channel_request.max_retries = max_retries;

            ::rpc::rpc::KrpcTransportRequest transport_request;
            transport_request.endpoint = endpoint;
            transport_request.downstream_timeout_ms = request.timeout_ms;
            transport_request.attempt = attempt;

            const Clock::time_point invoke_started = Clock::now();
            const ::rpc::rpc::KrpcTransportResponse channel_response = channel_.invoke(
                transport_request,
                channel_request,
                codec
            );
            const std::uint64_t observed_latency_ms = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - invoke_started).count()
            );

            RpcResponse current_response{
                channel_response.code,
                channel_response.message,
                channel_response.payload,
                attempt,
                channel_response.effective_timeout_ms,
                channel_response.cancelled,
                channel_response.cancel_reason,
            };

            if (runtime_feedback != nullptr) {
                runtime_feedback->on_invoke_result(node, request, current_response, observed_latency_ms);
                const LoadBalancerDecisionSnapshot decision = runtime_feedback->last_decision_snapshot();
                current_response.traffic_lane = decision.traffic_lane;
                current_response.circuit_state = circuit_state_to_string(decision.circuit_state);
            }
            current_response.selected_endpoint = endpoint;

            if (channel_response.code == 0) {
                remember_cache(request, current_response.payload);
                current_response.message = current_response.message.empty() ? "ok" : current_response.message;
                return finalize_and_log(current_response, endpoint);
            }

            last_response = std::move(current_response);
            if (!channel_response.retryable) {
                last_response = apply_degrade_if_needed(request, last_response);
                return finalize_and_log(last_response, endpoint);
            }
        }
    }

private:
    template <typename FinalizeFn>
    RpcResponse invoke_single_node(
        const RpcRequest& request,
        ::rpc::rpc::KrpcCodec codec,
        const ServiceNode& node,
        std::size_t attempt,
        RpcResponse* last_response,
        std::string* last_endpoint,
        ILoadBalancerRuntimeFeedback* runtime_feedback,
        FinalizeFn finalize_and_log
    ) {
        const std::string endpoint = node_endpoint(node);
        if (last_endpoint != nullptr) {
            *last_endpoint = endpoint;
        }

        ::rpc::rpc::KrpcRequestFrame channel_request;
        channel_request.service = request.service;
        channel_request.method = request.method;
        channel_request.payload = request.payload;
        channel_request.metadata = request.metadata;
        channel_request.timeout_ms = request.timeout_ms;
        channel_request.max_retries = request.max_retries;

        ::rpc::rpc::KrpcTransportRequest transport_request;
        transport_request.endpoint = endpoint;
        transport_request.downstream_timeout_ms = request.timeout_ms;
        transport_request.attempt = attempt;

        const Clock::time_point invoke_started = Clock::now();
        const ::rpc::rpc::KrpcTransportResponse channel_response = channel_.invoke(
            transport_request,
            channel_request,
            codec
        );
        const std::uint64_t observed_latency_ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - invoke_started).count()
        );

        RpcResponse response{
            channel_response.code,
            channel_response.message,
            channel_response.payload,
            attempt,
            channel_response.effective_timeout_ms,
            channel_response.cancelled,
            channel_response.cancel_reason,
        };
        response.selected_endpoint = endpoint;

        if (runtime_feedback != nullptr) {
            runtime_feedback->on_invoke_result(node, request, response, observed_latency_ms);
            const LoadBalancerDecisionSnapshot decision = runtime_feedback->last_decision_snapshot();
            response.traffic_lane = decision.traffic_lane;
            response.circuit_state = circuit_state_to_string(decision.circuit_state);
        }

        if (response.code == 0) {
            remember_cache(request, response.payload);
            return finalize_and_log(response, endpoint);
        }

        response = apply_degrade_if_needed(request, response);
        if (last_response != nullptr) {
            *last_response = response;
        }
        return finalize_and_log(response, endpoint);
    }

    std::string cache_key(const RpcRequest& request) const {
        const auto explicit_key = request.metadata.find("x-fallback-cache-key");
        if (explicit_key != request.metadata.end() && !explicit_key->second.empty()) {
            return explicit_key->second;
        }
        return request.service + "|" + request.method;
    }

    void remember_cache(const RpcRequest& request, const std::string& payload) {
        if (!parse_metadata_bool(request, "x-fallback-cache", true)) {
            return;
        }

        CachedPayload cached;
        cached.payload = payload;
        cached.updated_at = Clock::now();

        std::lock_guard<std::mutex> lock(cache_mutex_);
        fallback_cache_[cache_key(request)] = std::move(cached);
    }

    std::optional<std::string> lookup_cached_payload(const RpcRequest& request) const {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        const auto it = fallback_cache_.find(cache_key(request));
        if (it == fallback_cache_.end()) {
            return std::nullopt;
        }

        const std::uint64_t ttl_ms = parse_metadata_u64(request, "x-fallback-cache-ttl-ms", 10'000);
        if (ttl_ms > 0) {
            const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                Clock::now() - it->second.updated_at
            ).count();
            if (age_ms > static_cast<long long>(ttl_ms)) {
                return std::nullopt;
            }
        }
        return it->second.payload;
    }

    RpcResponse apply_degrade_if_needed(const RpcRequest& request, RpcResponse failure) const {
        if (failure.code == 0) {
            return failure;
        }

        if (!is_retryable_failure_code(failure.code)
            && failure.code != 429
            && failure.code != 503
            && failure.code != 504) {
            return failure;
        }

        if (parse_metadata_bool(request, "x-fallback-cache", true)) {
            const std::optional<std::string> cached = lookup_cached_payload(request);
            if (cached.has_value()) {
                failure.code = 0;
                failure.message = "degraded_cache_fallback";
                failure.payload = *cached;
                failure.degraded = true;
                failure.degrade_strategy = "cache";
                return failure;
            }
        }

        const auto static_it = request.metadata.find("x-fallback-static");
        if (static_it != request.metadata.end() && !static_it->second.empty()) {
            failure.code = 0;
            failure.message = "degraded_static_fallback";
            failure.payload = static_it->second;
            failure.degraded = true;
            failure.degrade_strategy = "static";
            return failure;
        }

        return failure;
    }

    struct CachedPayload {
        std::string payload;
        Clock::time_point updated_at{};
    };

    std::shared_ptr<IServiceDiscovery> discovery_;
    std::shared_ptr<ILoadBalancer> load_balancer_;
    ::rpc::runtime::RetryBudget retry_budget_;
    ::rpc::rpc::KrpcChannel channel_;

    mutable std::mutex cache_mutex_;
    mutable std::unordered_map<std::string, CachedPayload> fallback_cache_;
};

}  // namespace

void init_client(ClientInitOptions options) {
    std::lock_guard<std::mutex> lock(g_client_mutex);

    if (!options.discovery) {
        options.discovery = std::make_shared<InMemoryServiceDiscovery>();
    }
    if (!options.load_balancer) {
        options.load_balancer = std::make_shared<AdaptiveLoadBalancer>();
    }

    ::rpc::runtime::RetryBudgetOptions retry_options = options.retry_budget_options;
    const ::rpc::runtime::RetryBudgetOptions defaults{};
    if (retry_options.window == defaults.window) {
        const std::uint64_t window_ms = parse_config_u64("rpc.retry.window_ms", static_cast<std::uint64_t>(
            defaults.window.count()
        ));
        retry_options.window = std::chrono::milliseconds(window_ms == 0 ? 1 : window_ms);
    }
    if (retry_options.retry_ratio == defaults.retry_ratio) {
        retry_options.retry_ratio = parse_config_double("rpc.retry.ratio", defaults.retry_ratio);
    }
    if (retry_options.min_retry_tokens == defaults.min_retry_tokens) {
        retry_options.min_retry_tokens = parse_config_size("rpc.retry.min_tokens", defaults.min_retry_tokens);
    }

    g_default_client = std::make_shared<DefaultRpcClient>(
        std::move(options.discovery),
        std::move(options.load_balancer),
        retry_options
    );
    {
        std::lock_guard<std::mutex> runtime_lock(g_lb_runtime_mutex);
        g_last_lb_runtime_stats = LoadBalancerRuntimeStats{};
    }
}

std::shared_ptr<IRpcClient> default_client() {
    {
        std::lock_guard<std::mutex> lock(g_client_mutex);
        if (g_default_client) {
            return g_default_client;
        }
    }

    init_client();

    std::lock_guard<std::mutex> lock(g_client_mutex);
    return g_default_client;
}

RpcInvokeAuditSnapshot last_invoke_audit_snapshot() {
    std::lock_guard<std::mutex> lock(g_audit_mutex);
    return g_last_invoke_audit;
}

LoadBalancerDecisionSnapshot last_load_balancer_decision_snapshot() {
    std::lock_guard<std::mutex> lock(g_lb_snapshot_mutex);
    return g_last_lb_snapshot;
}

LoadBalancerRuntimeStats load_balancer_runtime_stats() {
    std::lock_guard<std::mutex> lock(g_lb_runtime_mutex);
    return g_last_lb_runtime_stats;
}

}  // namespace rpc::client
