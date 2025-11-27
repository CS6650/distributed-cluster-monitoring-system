#ifndef RPC_HPP
#define RPC_HPP

#include <string>
#include <functional>
#include <thread>
#include <map>
#include <mutex>
#include "json.hpp"

using json = nlohmann::json;

class JsonRpcServer {
    int port;
    int server_fd;
    std::map<std::string, std::function<json(json)>> handlers;
    std::mutex mtx;

public:
    JsonRpcServer(int port);
    void start();
    void registerMethod(const std::string &name,
                        std::function<json(json)> fn);

private:
    void handleClient(int client_fd);
};

class JsonRpcClient {
public:
    static json call(const std::string &host, int port,
                     const std::string &method,
                     const json &params);
};

#endif
