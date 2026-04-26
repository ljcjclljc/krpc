cmake_minimum_required(VERSION 3.20)

# 文件用途：
# 统一定义项目构建目标：核心库、应用可执行文件与单元测试。

project(rpc_gateway
    VERSION 0.1.0
    DESCRIPTION "High-performance distributed RPC gateway"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(RPC_ENABLE_ASAN "Enable AddressSanitizer for runtime memory checks" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(ProjectOptions)
rpc_apply_project_options()

if(RPC_ENABLE_ASAN AND NOT MSVC)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

find_package(Boost REQUIRED COMPONENTS context)
find_package(Protobuf REQUIRED)
find_package(OpenSSL REQUIRED)

set(RPC_PROTO_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/proto/krpc.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto"
)
protobuf_generate_cpp(RPC_PROTO_SRCS RPC_PROTO_HDRS ${RPC_PROTO_FILES})

set(PROTOBUF_LINK_TARGET "")
if (TARGET protobuf::libprotobuf)
    set(PROTOBUF_LINK_TARGET protobuf::libprotobuf)
elseif(TARGET Protobuf::protobuf)
    set(PROTOBUF_LINK_TARGET Protobuf::protobuf)
endif()

add_library(rpc_core
    ${RPC_PROTO_SRCS}
    src/common/version.cpp
    src/common/boost_json_url_src.cpp
    src/gateway/http_gateway.cpp
    src/gateway/http_parser.cpp
    src/runtime/coroutine.cpp
    src/runtime/hook.cpp
    src/runtime/io_manager.cpp
    src/runtime/request_context.cpp
    src/runtime/retry_budget.cpp
    src/runtime/scheduler.cpp
    src/runtime/timer.cpp
    src/runtime/runtime_stub.cpp
    src/net/net_stub.cpp
    src/net/reactor_http_server.cpp
    src/gateway/gateway_stub.cpp
    src/rpc/krpc_channel.cpp
    src/rpc/rpc_stub.cpp
    src/infra/infra_stub.cpp
    src/infra/memory_pool.cpp
    src/infra/structured_log.cpp
    src/infra/raft/raft_cluster.cpp
    src/infra/raft/raft_storage.cpp
    src/infra/raft/raft_rpc.cpp
)

target_include_directories(rpc_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(rpc_core
    PRIVATE
        Boost::context
        OpenSSL::SSL
        OpenSSL::Crypto
        ${CMAKE_DL_LIBS}
        ${PROTOBUF_LINK_TARGET}
        ${Protobuf_LIBRARIES}
)

add_executable(rpc_gateway_app
    apps/gateway/main.cpp
)

target_link_libraries(rpc_gateway_app
    PRIVATE
        rpc_core
)

enable_testing()

add_executable(coroutine_switch_test
    tests/unit/coroutine_switch_test.cpp
)

target_link_libraries(coroutine_switch_test
    PRIVATE
        rpc_core
)

add_test(
    NAME coroutine_switch_test
    COMMAND coroutine_switch_test
)

add_executable(config_snapshot_test
    tests/unit/config_snapshot_test.cpp
)

target_link_libraries(config_snapshot_test
    PRIVATE
        rpc_core
)

add_test(
    NAME config_snapshot_test
    COMMAND config_snapshot_test
)

add_executable(rpc_abstraction_test
    tests/unit/rpc_abstraction_test.cpp
)

target_link_libraries(rpc_abstraction_test
    PRIVATE
        rpc_core
)

add_test(
    NAME rpc_abstraction_test
    COMMAND rpc_abstraction_test
)

add_executable(scheduler_mn_test
    tests/unit/scheduler_mn_test.cpp
)

target_link_libraries(scheduler_mn_test
    PRIVATE
        rpc_core
)

add_test(
    NAME scheduler_mn_test
    COMMAND scheduler_mn_test
)

add_executable(io_epoll_integration_test
    tests/unit/io_epoll_integration_test.cpp
)

target_link_libraries(io_epoll_integration_test
    PRIVATE
        rpc_core
)

add_test(
    NAME io_epoll_integration_test
    COMMAND io_epoll_integration_test
)

add_executable(timer_heap_test
    tests/unit/timer_heap_test.cpp
)

target_link_libraries(timer_heap_test
    PRIVATE
        rpc_core
)

add_test(
    NAME timer_heap_test
    COMMAND timer_heap_test
)

add_executable(hook_syscall_test
    tests/unit/hook_syscall_test.cpp
)

target_link_libraries(hook_syscall_test
    PRIVATE
        rpc_core
)

add_test(
    NAME hook_syscall_test
    COMMAND hook_syscall_test
)

add_executable(w7_timeout_retry_test
    tests/unit/w7_timeout_retry_test.cpp
)

target_link_libraries(w7_timeout_retry_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w7_timeout_retry_test
    COMMAND w7_timeout_retry_test
)

add_executable(w8_reactor_http_test
    tests/unit/w8_reactor_http_test.cpp
)

target_link_libraries(w8_reactor_http_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w8_reactor_http_test
    COMMAND w8_reactor_http_test
)

add_executable(w9_gateway_router_auth_test
    tests/unit/w9_gateway_router_auth_test.cpp
)

target_link_libraries(w9_gateway_router_auth_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w9_gateway_router_auth_test
    COMMAND w9_gateway_router_auth_test
)

add_executable(w10_gateway_rpc_trace_metrics_test
    tests/unit/w10_gateway_rpc_trace_metrics_test.cpp
)

target_link_libraries(w10_gateway_rpc_trace_metrics_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w10_gateway_rpc_trace_metrics_test
    COMMAND w10_gateway_rpc_trace_metrics_test
)

add_executable(w12_raft_consensus_test
    tests/unit/w12_raft_consensus_test.cpp
)

target_link_libraries(w12_raft_consensus_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w12_raft_consensus_test
    COMMAND w12_raft_consensus_test
)

add_executable(w12_raft_storage_recovery_test
    tests/unit/w12_raft_storage_recovery_test.cpp
)

target_link_libraries(w12_raft_storage_recovery_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w12_raft_storage_recovery_test
    COMMAND w12_raft_storage_recovery_test
)

add_executable(w12_raft_rpc_transport_test
    tests/unit/w12_raft_rpc_transport_test.cpp
)

target_link_libraries(w12_raft_rpc_transport_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w12_raft_rpc_transport_test
    COMMAND w12_raft_rpc_transport_test
)

add_executable(w12_raft_fault_injection_test
    tests/unit/w12_raft_fault_injection_test.cpp
)

target_link_libraries(w12_raft_fault_injection_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w12_raft_fault_injection_test
    COMMAND w12_raft_fault_injection_test
)

add_executable(w13_adaptive_lb_circuit_breaker_gray_test
    tests/unit/w13_adaptive_lb_circuit_breaker_gray_test.cpp
)

target_link_libraries(w13_adaptive_lb_circuit_breaker_gray_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w13_adaptive_lb_circuit_breaker_gray_test
    COMMAND w13_adaptive_lb_circuit_breaker_gray_test
)

add_executable(w14_memory_pool_observability_test
    tests/unit/w14_memory_pool_observability_test.cpp
)

target_link_libraries(w14_memory_pool_observability_test
    PRIVATE
        rpc_core
)

add_test(
    NAME w14_memory_pool_observability_test
    COMMAND w14_memory_pool_observability_test
)

add_executable(w15_fullchain_perf_test
    tests/unit/w15_fullchain_perf_test.cpp
)

target_link_libraries(w15_fullchain_perf_test
    PRIVATE
        rpc_core
)

target_include_directories(w15_fullchain_perf_test
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
)

add_test(
    NAME w15_fullchain_perf_test
    COMMAND w15_fullchain_perf_test
)

add_executable(w16_stability_chaos_tuning_test
    tests/unit/w16_stability_chaos_tuning_test.cpp
)

target_link_libraries(w16_stability_chaos_tuning_test
    PRIVATE
        rpc_core
)

target_include_directories(w16_stability_chaos_tuning_test
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
)

add_test(
    NAME w16_stability_chaos_tuning_test
    COMMAND w16_stability_chaos_tuning_test
)

add_executable(w17_hot_reload_tls_rollback_test
    tests/unit/w17_hot_reload_tls_rollback_test.cpp
)

target_link_libraries(w17_hot_reload_tls_rollback_test
    PRIVATE
        rpc_core
)

target_compile_definitions(w17_hot_reload_tls_rollback_test
    PRIVATE
        RPC_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
)

add_test(
    NAME w17_hot_reload_tls_rollback_test
    COMMAND w17_hot_reload_tls_rollback_test
)
