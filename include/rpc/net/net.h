#pragma once

// 文件用途：
// 声明网络模块初始化入口，用于启动期装配 Socket/Epoll/连接管理相关能力。

#include <cstddef>
#include <cstdint>
#include <string>

namespace rpc::net {

// 初始化网络模块。
// 当前为占位实现，后续可在此初始化监听器、连接池、事件循环线程等组件。
void init_network();

// 跨层超时预算结果：
// - effective_timeout_ms: 当前应应用到 net 层的有效超时
// - deadline_exceeded: 上游预算已耗尽（无需继续下游调用）
struct EffectiveTimeout {
    std::uint64_t effective_timeout_ms{0};
    bool deadline_exceeded{false};
};

// 结合上游 deadline 与下游 timeout 配置，计算 net 层有效超时。
// 规则：effective = min(上游剩余预算, 下游配置)，其中 0 表示“未配置该层限制”。
EffectiveTimeout derive_effective_timeout(std::uint64_t downstream_timeout_ms);

// net 层模拟请求模型（用于 W7 超时/重试链路验证）。
struct NetCallRequest {
    std::string endpoint;
    std::string payload;
    std::uint64_t downstream_timeout_ms{0};
    std::size_t attempt{1}; // 1-based
    std::size_t fail_before_success{0};
};

// net 层模拟请求结果。
struct NetCallResponse {
    int code{0};
    std::string message;
    std::string payload;
    bool retryable{false};
    std::uint64_t effective_timeout_ms{0};
};

// 模拟一次 net 调用，内部会应用超时分层结果。
NetCallResponse invoke_stub(const NetCallRequest& request);

// 观测上一次 net 调用使用的有效超时（用于测试验收）。
std::uint64_t last_effective_timeout_ms() noexcept;

}  // namespace rpc::net
