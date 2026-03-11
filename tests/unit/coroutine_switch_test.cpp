#include <atomic>
#include <cstdint>
#include <iostream>
#include <stdexcept>

// 文件用途：
// 验证 W2 协程核心能力：
// 1) 单线程高频 resume/yield 切换稳定性
// 2) 协程生命周期回收是否正确
// 3) 回收后协程 ID 是否可复用

#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/scheduler.h"

int main() {
    // 非协程上下文调用 yield_current 应抛出逻辑异常。
    bool rejected_outside_coroutine = false;
    try {
        rpc::runtime::Coroutine::yield_current();
    } catch (const std::logic_error&) {
        rejected_outside_coroutine = true;
    }
    if (!rejected_outside_coroutine) {
        std::cerr << "yield_current should fail outside coroutine context\n";
        return 1;
    }

    if (rpc::runtime::Coroutine::current() != nullptr) {
        std::cerr << "current() should be nullptr before scheduler runs\n";
        return 1;
    }

    // 验收目标：执行 10w 次协程切换。
    constexpr std::int32_t kSwitchCount = 100000;
    std::int32_t counter = 0;
    std::atomic<bool> current_visible{true};

    rpc::runtime::CoroutineScheduler scheduler;
    scheduler.start(1);

    // 创建一个会频繁 yield 的协程：
    // 每次循环递增计数后让出执行权，触发调度器反复 resume/yield。
    const rpc::runtime::CoroutineId first_id = scheduler.schedule([&counter, &current_visible]() {
        for (std::int32_t i = 0; i < kSwitchCount; ++i) {
            if (rpc::runtime::Coroutine::current() == nullptr) {
                current_visible.store(false, std::memory_order_relaxed);
            }
            ++counter;
            rpc::runtime::Coroutine::yield_current();
        }
    });

    scheduler.wait_idle();

    if (!current_visible.load(std::memory_order_relaxed)) {
        std::cerr << "current() should be visible inside coroutine callback\n";
        return 1;
    }

    if (rpc::runtime::Coroutine::current() != nullptr) {
        std::cerr << "current() should be nullptr after scheduler wait_idle\n";
        return 1;
    }

    // 校验切换次数是否准确。
    if (counter != kSwitchCount) {
        std::cerr << "switch count mismatch, expected=" << kSwitchCount << ", actual=" << counter << '\n';
        return 1;
    }

    // 首次执行完成后，协程应全部被回收。
    if (scheduler.alive_count() != 0) {
        std::cerr << "lifecycle recycle failed, alive coroutines=" << scheduler.alive_count() << '\n';
        return 1;
    }

    if (scheduler.completed_count() != 1) {
        std::cerr << "unexpected completed count=" << scheduler.completed_count() << '\n';
        return 1;
    }

    // 创建第二个协程，验证调度器会复用先前回收的 ID。
    const rpc::runtime::CoroutineId second_id = scheduler.schedule([]() {});
    if (second_id != first_id) {
        std::cerr << "coroutine id should be recycled, first=" << first_id << ", second=" << second_id << '\n';
        return 1;
    }

    scheduler.wait_idle();

    // 第二次执行后应继续保持回收计数正确。
    if (scheduler.alive_count() != 0 || scheduler.completed_count() != 2) {
        std::cerr << "recycle after second run failed, alive=" << scheduler.alive_count()
                  << ", completed=" << scheduler.completed_count() << '\n';
        return 1;
    }

    scheduler.stop();

    if (scheduler.running() || scheduler.worker_count() != 0) {
        std::cerr << "scheduler stop failed, running=" << scheduler.running()
                  << ", workers=" << scheduler.worker_count() << '\n';
        return 1;
    }

    std::cout << "coroutine_switch_test passed, switch_count=" << counter << '\n';
    return 0;
}
