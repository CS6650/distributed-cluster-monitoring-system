#ifndef TIMER_HPP
#define TIMER_HPP

#include <chrono>
#include <functional>
#include <thread>
#include <atomic>
#include <memory>

class Timer
{
    // Use shared_ptr so all threads see the same atomic
    std::shared_ptr<std::atomic<bool>> running;
    std::thread th;

public:
    Timer() : running(std::make_shared<std::atomic<bool>>(false)) {}

    ~Timer()
    {
        cancel();
        // Don't join detached threads
    }

    template <typename Fn>
    void start(int ms, Fn callback)
    {
        // Cancel any existing timer
        cancel();

        // Create new running flag
        running = std::make_shared<std::atomic<bool>>(true);

        // Capture running by value (but it's a shared_ptr, so all copies point to same atomic)
        auto runningCopy = running;

        th = std::thread([runningCopy, ms, callback]()
                         {
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            
            // Check if timer was cancelled
            if (runningCopy->load()) {
                callback();
            } });
        th.detach();
    }

    void cancel()
    {
        if (running)
        {
            running->store(false);
        }
    }
};

#endif