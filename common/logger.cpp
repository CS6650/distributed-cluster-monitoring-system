#include "logger.hpp"
#include <ctime>

static std::string now() {
    time_t t = time(0);
    char buf[32];
    strftime(buf, sizeof(buf), "%F %T", localtime(&t));
    return buf;
}

Logger::Logger(const std::string &name) {
    file.open(name, std::ios::app);
}

Logger::~Logger() {
    if (file.is_open()) file.close();
}

void Logger::info(const std::string &msg) {
    std::lock_guard<std::mutex> lock(mtx);
    file << "[" << now() << "] [INFO] " << msg << "\n";
    std::cout << "[INFO] " << msg << std::endl;
}

void Logger::warn(const std::string &msg) {
    std::lock_guard<std::mutex> lock(mtx);
    file << "[" << now() << "] [WARN] " << msg << "\n";
    std::cout << "[WARN] " << msg << std::endl;
}

void Logger::debug(const std::string &msg) {
    std::lock_guard<std::mutex> lock(mtx);
    file << "[" << now() << "] [DEBUG] " << msg << "\n";
    std::cout << "[DEBUG] " << msg << std::endl;
}

void Logger::error(const std::string &msg) {
    std::lock_guard<std::mutex> lock(mtx);
    file << "[" << now() << "] [ERROR] " << msg << "\n";
    std::cerr << "[ERROR] " << msg << std::endl;
}
