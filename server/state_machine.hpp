#ifndef STATE_MACHINE_HPP
#define STATE_MACHINE_HPP

#include <string>
#include <mutex>
#include <unordered_map>
#include "../common/logger.hpp"

class StateMachine {
    std::unordered_map<std::string,std::string> data;
    std::mutex mtx;
    Logger logger;

public:
    StateMachine();
    void apply(const std::string &command);
    std::string get(const std::string &key);
};

#endif
