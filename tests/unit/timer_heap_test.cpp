#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rpc/runtime/timer.h"

// 文件用途：
// 单元验证最小堆定时器：到期顺序、取消语义、超时计算精度。

namespace {

using Clock = rpc::runtime::TimerManager::Clock;

long long elapsed_ms(const Clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();
}

}  // namespace

int main() {
    using namespace std::chrono_literals;

    rpc::runtime::TimerManager timer_manager;
    const auto start = Clock::now();

    std::vector<std::pair<std::string, long long>> fired;
    fired.reserve(3);

    const rpc::runtime::TimerId timer_a = timer_manager.add_timer(60ms, [&]() {
        fired.emplace_back("A", elapsed_ms(start));
    });
    const rpc::runtime::TimerId timer_b = timer_manager.add_timer(20ms, [&]() {
        fired.emplace_back("B", elapsed_ms(start));
    });
    const rpc::runtime::TimerId timer_c = timer_manager.add_timer(40ms, [&]() {
        fired.emplace_back("C", elapsed_ms(start));
    });

    if (timer_a == 0 || timer_b == 0 || timer_c == 0) {
        std::cerr << "timer id should not be zero\n";
        return 1;
    }

    const int next_timeout = timer_manager.next_timeout_ms();
    if (next_timeout < 0 || next_timeout > 30) {
        std::cerr << "unexpected next_timeout_ms before firing, value=" << next_timeout << '\n';
        return 1;
    }

    if (!timer_manager.cancel_timer(timer_c)) {
        std::cerr << "cancel timer_c failed\n";
        return 1;
    }

    const auto wait_deadline = Clock::now() + 400ms;
    while (Clock::now() < wait_deadline && fired.size() < 2) {
        const int timeout_ms = timer_manager.next_timeout_ms();
        if (timeout_ms < 0) {
            break;
        }

        if (timeout_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
        } else {
            std::this_thread::sleep_for(1ms);
        }

        auto callbacks = timer_manager.collect_expired_callbacks();
        for (auto& callback : callbacks) {
            callback();
        }
    }

    if (fired.size() != 2) {
        std::cerr << "expected 2 timers fired, actual=" << fired.size() << '\n';
        return 1;
    }

    if (fired[0].first != "B" || fired[1].first != "A") {
        std::cerr << "unexpected callback order: " << fired[0].first << " -> " << fired[1].first << '\n';
        return 1;
    }

    // 精度验收使用宽松阈值：避免不同环境调度抖动引起偶发失败。
    if (fired[0].second < 15 || fired[1].second < 55) {
        std::cerr << "timer fired too early, B=" << fired[0].second
                  << "ms, A=" << fired[1].second << "ms\n";
        return 1;
    }

    if (fired[0].second > 200 || fired[1].second > 250) {
        std::cerr << "timer fired too late, B=" << fired[0].second
                  << "ms, A=" << fired[1].second << "ms\n";
        return 1;
    }

    if (timer_manager.cancel_timer(timer_c)) {
        std::cerr << "cancelled timer_c should already be cleaned\n";
        return 1;
    }

    const int rest_timeout = timer_manager.next_timeout_ms();
    if (rest_timeout != -1) {
        std::cerr << "expected no timers left, timeout=" << rest_timeout << '\n';
        return 1;
    }

    std::cout << "timer_heap_test passed"
              << ", fired_order=" << fired[0].first << "->" << fired[1].first
              << ", fired_ms=" << fired[0].second << "," << fired[1].second
              << '\n';
    return 0;
}
