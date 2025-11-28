#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <string>
#include <fstream>
#include <mutex>
#include <iostream>

class Logger {
    std::mutex mtx;
    std::ofstream file;

public:
    Logger(const std::string &name);
    ~Logger();
    void info(const std::string &msg);
    void warn(const std::string &msg);
    void debug(const std::string &msg);
    void error(const std::string &msg);
};

#endif
