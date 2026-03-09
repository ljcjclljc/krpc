#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#include "rpc/runtime/coroutine.h"
#include "rpc/runtime/scheduler.h"

namespace {

bool wait_idle_with_timeout(
    rpc::runtime::CoroutineScheduler& scheduler,
    std::chrono::milliseconds timeout
) {
    auto waiter = std::async(std::launch::async, [&scheduler]() {
        scheduler.wait_idle();
    });

    if (waiter.wait_for(timeout) != std::future_status::ready) {
        return false;
    }

    waiter.get();
    return true;
}

}  // namespace

int main() {
    using namespace std::chrono_literals;

    constexpr std::size_t kWorkerThreads = 4;
    constexpr std::size_t kProducerThreads = 4;
    constexpr std::size_t kCoroutinesPerProducer = 200;
    constexpr std::size_t kYieldPerCoroutine = 8;
    constexpr std::size_t kRestartCoroutines = 64;

    rpc::runtime::CoroutineScheduler scheduler;

    // 线程池未启动时禁止提交任务，避免进入兼容分支。
    bool rejected_before_start = false;
    try {
        scheduler.schedule([]() {});
    } catch (const std::logic_error&) {
        rejected_before_start = true;
    }
    if (!rejected_before_start) {
        std::cerr << "schedule should be rejected before start()\n";
        return 1;
    }

    scheduler.start(kWorkerThreads);

    // 先留一小段空闲窗口，验证 idle 协程确实被调度执行。
    std::this_thread::sleep_for(20ms);

    std::atomic<std::size_t> finished{0};
    std::atomic<std::size_t> yields{0};

    std::vector<std::thread> producers;
    producers.reserve(kProducerThreads);

    for (std::size_t producer = 0; producer < kProducerThreads; ++producer) {
        producers.emplace_back([&scheduler, &finished, &yields, per_coroutine = kCoroutinesPerProducer, yield_turns = kYieldPerCoroutine]() {
            for (std::size_t i = 0; i < per_coroutine; ++i) {
                scheduler.schedule([&finished, &yields, yield_turns]() {
                    for (std::size_t turn = 0; turn < yield_turns; ++turn) {
                        yields.fetch_add(1, std::memory_order_relaxed);
                        rpc::runtime::Coroutine::yield_current();
                    }
                    finished.fetch_add(1, std::memory_order_relaxed);
                });
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    if (!wait_idle_with_timeout(scheduler, 15s)) {
        std::cerr << "wait_idle timeout in stress phase, potential deadlock\n";
        scheduler.stop();
        return 1;
    }

    scheduler.stop();

    const std::size_t expected_coroutines = kProducerThreads * kCoroutinesPerProducer;
    const std::size_t expected_yields = expected_coroutines * kYieldPerCoroutine;

    if (finished.load(std::memory_order_relaxed) != expected_coroutines) {
        std::cerr << "finished mismatch, expected=" << expected_coroutines
                  << ", actual=" << finished.load(std::memory_order_relaxed) << '\n';
        return 1;
    }

    if (yields.load(std::memory_order_relaxed) != expected_yields) {
        std::cerr << "yield count mismatch, expected=" << expected_yields
                  << ", actual=" << yields.load(std::memory_order_relaxed) << '\n';
        return 1;
    }

    if (scheduler.running()) {
        std::cerr << "scheduler should be stopped after stop()\n";
        return 1;
    }

    if (scheduler.worker_count() != 0) {
        std::cerr << "worker threads should be reclaimed, worker_count=" << scheduler.worker_count() << '\n';
        return 1;
    }

    if (scheduler.pending_count() != 0 || scheduler.alive_count() != 0) {
        std::cerr << "scheduler not fully drained, pending=" << scheduler.pending_count()
                  << ", alive=" << scheduler.alive_count() << '\n';
        return 1;
    }

    if (scheduler.idle_switch_count() == 0) {
        std::cerr << "idle coroutine was not exercised\n";
        return 1;
    }

    // 停止后同样不允许再提交任务。
    bool rejected_after_stop = false;
    try {
        scheduler.schedule([]() {});
    } catch (const std::logic_error&) {
        rejected_after_stop = true;
    }
    if (!rejected_after_stop) {
        std::cerr << "schedule should be rejected after stop()\n";
        return 1;
    }

    // 再次启动 worker，验证 stop/start 生命周期可恢复。
    scheduler.start(2);
    std::atomic<std::size_t> restarted_finished{0};

    for (std::size_t i = 0; i < kRestartCoroutines; ++i) {
        scheduler.schedule([&restarted_finished]() {
            rpc::runtime::Coroutine::yield_current();
            restarted_finished.fetch_add(1, std::memory_order_relaxed);
        });
    }

    if (!wait_idle_with_timeout(scheduler, 10s)) {
        std::cerr << "wait_idle timeout in restart phase\n";
        scheduler.stop();
        return 1;
    }

    scheduler.stop();

    if (restarted_finished.load(std::memory_order_relaxed) != kRestartCoroutines) {
        std::cerr << "restart phase finished mismatch, expected=" << kRestartCoroutines
                  << ", actual=" << restarted_finished.load(std::memory_order_relaxed) << '\n';
        return 1;
    }

    const std::size_t expected_completed = expected_coroutines + kRestartCoroutines;
    if (scheduler.completed_count() != expected_completed) {
        std::cerr << "completed_count mismatch, expected=" << expected_completed
                  << ", actual=" << scheduler.completed_count() << '\n';
        return 1;
    }

    if (scheduler.pending_count() != 0 || scheduler.alive_count() != 0) {
        std::cerr << "scheduler not clean after restart phase, pending=" << scheduler.pending_count()
                  << ", alive=" << scheduler.alive_count() << '\n';
        return 1;
    }

    std::cout << "scheduler_mn_test passed"
              << ", completed=" << scheduler.completed_count()
              << ", idle_switches=" << scheduler.idle_switch_count()
              << ", yields=" << yields.load(std::memory_order_relaxed)
              << '\n';
    return 0;
}
