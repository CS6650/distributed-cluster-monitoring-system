#ifndef TIMER_HPP
#define TIMER_HPP

#include <chrono>
#include <functional>
#include <thread>
#include <atomic>

class Timer {
    std::atomic<bool> running {false};
    std::thread th;

public:
    template<typename Fn>
    void start(int ms, Fn callback) {
        running = true;
        th = std::thread([=]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            if (running) callback();
        });
        th.detach();
    }

    void cancel() { running = false; }
};

#endif
