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

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(ProjectOptions)
rpc_apply_project_options()

add_library(rpc_core
    src/common/version.cpp
    src/runtime/coroutine.cpp
    src/runtime/scheduler.cpp
    src/runtime/runtime_stub.cpp
    src/net/net_stub.cpp
    src/gateway/gateway_stub.cpp
    src/rpc/rpc_stub.cpp
    src/infra/infra_stub.cpp
)

target_include_directories(rpc_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
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
