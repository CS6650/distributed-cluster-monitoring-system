#include "state_machine.hpp"
#include <sstream>

StateMachine::StateMachine() : logger("state_machine.log") {}

void StateMachine::apply(const std::string &command) {
    std::lock_guard<std::mutex> lock(mtx);

    // Simple command parser: "SET key value"
    std::istringstream iss(command);
    std::string cmd, key, value;
    iss >> cmd >> key >> value;
    if (cmd == "SET") {
        data[key] = value;
        logger.info("Applied command: " + command);
    } else {
        logger.warn("Unknown command: " + command);
    }
}

std::string StateMachine::get(const std::string &key) {
    std::lock_guard<std::mutex> lock(mtx);
    if (data.count(key)) return data[key];
    return "";
}
