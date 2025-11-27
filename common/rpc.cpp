#include "rpc.hpp"
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

JsonRpcServer::JsonRpcServer(int port) : port(port) {}

void JsonRpcServer::registerMethod(const std::string &name,
                                   std::function<json(json)> fn) {
    std::lock_guard<std::mutex> lock(mtx);
    handlers[name] = fn;
}

void JsonRpcServer::start() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    bind(server_fd, (sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 16);

    std::thread([this]() {
        while (true) {
            sockaddr_in client;
            socklen_t len = sizeof(client);
            int client_fd = accept(server_fd, (sockaddr*)&client, &len);
            std::thread(&JsonRpcServer::handleClient, this, client_fd).detach();
        }
    }).detach();
}

void JsonRpcServer::handleClient(int client_fd) {
    char buffer[4096];
    memset(buffer, 0, sizeof(buffer));

    int n = read(client_fd, buffer, sizeof(buffer)-1);
    if (n <= 0) { close(client_fd); return; }

    json req = json::parse(buffer);
    std::string method = req["method"];
    json params = req["params"];

    json resp;

    if (handlers.count(method)) {
        resp = handlers[method](params);
    } else {
        resp = {{"error", "unknown method"}};
    }

    std::string out = resp.dump();
    send(client_fd, out.c_str(), out.size(), 0);
    close(client_fd);
}

json JsonRpcClient::call(const std::string &host, int port,
                         const std::string &method, const json &params) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    addr.sin_port = htons(port);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        return {{"error", "connect_failed"}};
    }

    json packet = { {"method", method}, {"params", params} };
    std::string data = packet.dump();
    send(sock, data.c_str(), data.size(), 0);

    char buffer[4096];
    memset(buffer, 0, sizeof(buffer));
    int n = read(sock, buffer, sizeof(buffer)-1);

    close(sock);

    if (n <= 0) return {{"error", "no_response"}};
    return json::parse(buffer);
}
