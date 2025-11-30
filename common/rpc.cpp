// #include "rpc.hpp"
// #include <arpa/inet.h>
// #include <unistd.h>
// #include <cstring>
// #include <iostream>

// JsonRpcServer::JsonRpcServer(int port) : port(port) {}

// void JsonRpcServer::registerMethod(const std::string &name,
//                                    std::function<json(json)> fn) {
//     std::lock_guard<std::mutex> lock(mtx);
//     handlers[name] = fn;
// }

// void JsonRpcServer::start() {
//     server_fd = socket(AF_INET, SOCK_STREAM, 0);

//     sockaddr_in addr{};
//     addr.sin_family = AF_INET;
//     addr.sin_addr.s_addr = INADDR_ANY;
//     addr.sin_port = htons(port);

//     bind(server_fd, (sockaddr*)&addr, sizeof(addr));
//     listen(server_fd, 16);

//     std::thread([this]() {
//         while (true) {
//             sockaddr_in client;
//             socklen_t len = sizeof(client);
//             int client_fd = accept(server_fd, (sockaddr*)&client, &len);
//             std::thread(&JsonRpcServer::handleClient, this, client_fd).detach();
//         }
//     }).detach();
// }

// void JsonRpcServer::handleClient(int client_fd) {
//     char buffer[4096];
//     memset(buffer, 0, sizeof(buffer));

//     int n = read(client_fd, buffer, sizeof(buffer)-1);
//     if (n <= 0) { close(client_fd); return; }

//     json req = json::parse(buffer);
//     std::string method = req["method"];
//     json params = req["params"];

//     json resp;

//     if (handlers.count(method)) {
//         resp = handlers[method](params);
//     } else {
//         resp = {{"error", "unknown method"}};
//     }

//     std::string out = resp.dump();
//     send(client_fd, out.c_str(), out.size(), 0);
//     close(client_fd);
// }

// json JsonRpcClient::call(const std::string &host, int port,
//                          const std::string &method, const json &params) {
//     int sock = socket(AF_INET, SOCK_STREAM, 0);

//     sockaddr_in addr{};
//     addr.sin_family = AF_INET;
//     inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
//     addr.sin_port = htons(port);

//     if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
//         return {{"error", "connect_failed"}};
//     }

//     json packet = { {"method", method}, {"params", params} };
//     std::string data = packet.dump();
//     send(sock, data.c_str(), data.size(), 0);

//     char buffer[4096];
//     memset(buffer, 0, sizeof(buffer));
//     int n = read(sock, buffer, sizeof(buffer)-1);

//     close(sock);

//     if (n <= 0) return {{"error", "no_response"}};
//     return json::parse(buffer);
// }

#include "rpc.hpp"
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <stdexcept>

JsonRpcServer::JsonRpcServer(int port) : port(port), server_fd(-1) {}

JsonRpcServer::~JsonRpcServer()
{
    if (server_fd >= 0)
    {
        close(server_fd);
    }
}

void JsonRpcServer::registerMethod(const std::string &name,
                                   std::function<json(json)> fn)
{
    std::lock_guard<std::mutex> lock(mtx);
    handlers[name] = fn;
}

void JsonRpcServer::start()
{
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        throw std::runtime_error("Failed to create socket");
    }

    // CRITICAL FIX: Enable SO_REUSEADDR to prevent "Address already in use" after crash
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        close(server_fd);
        throw std::runtime_error("Failed to set SO_REUSEADDR");
    }

#ifdef SO_REUSEPORT
    // Allow multiple sockets to bind to the same port (Linux-specific optimization)
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0)
    {
        std::cerr << "Warning: SO_REUSEPORT not supported" << std::endl;
    }
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(server_fd, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(server_fd);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port) +
                                 ": " + std::string(strerror(errno)));
    }

    if (listen(server_fd, 16) < 0)
    {
        close(server_fd);
        throw std::runtime_error("Failed to listen on port " + std::to_string(port));
    }

    std::cout << "JsonRpcServer listening on port " << port << std::endl;

    // Accept loop in detached thread
    std::thread([this]()
                {
        while (true) {
            sockaddr_in client;
            socklen_t len = sizeof(client);
            int client_fd = accept(server_fd, (sockaddr*)&client, &len);
            
            if (client_fd < 0) {
                // Server likely shutting down
                break;
            }
            
            std::thread(&JsonRpcServer::handleClient, this, client_fd).detach();
        } })
        .detach();
}

void JsonRpcServer::handleClient(int client_fd)
{
    char buffer[8192]; // Increased buffer size
    memset(buffer, 0, sizeof(buffer));

    // Set timeout on client socket
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    int n = read(client_fd, buffer, sizeof(buffer) - 1);
    if (n <= 0)
    {
        close(client_fd);
        return;
    }

    try
    {
        json req = json::parse(buffer);
        std::string method = req["method"];
        json params = req["params"];

        json resp;
        {
            std::lock_guard<std::mutex> lock(mtx);
            if (handlers.count(method))
            {
                resp = handlers[method](params);
            }
            else
            {
                resp = {{"error", "unknown method"}};
            }
        }

        std::string out = resp.dump();
        send(client_fd, out.c_str(), out.size(), 0);
    }
    catch (const std::exception &e)
    {
        json error_resp = {{"error", std::string("parse_error: ") + e.what()}};
        std::string out = error_resp.dump();
        send(client_fd, out.c_str(), out.size(), 0);
    }

    close(client_fd);
}

json JsonRpcClient::call(const std::string &host, int port,
                         const std::string &method, const json &params)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        return {{"error", "socket_creation_failed"}};
    }

    // Set connection timeout
    struct timeval timeout;
    timeout.tv_sec = 2; // 2 second timeout for connect
    timeout.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    addr.sin_port = htons(port);

    if (connect(sock, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        close(sock);
        return {{"error", "connect_failed"}};
    }

    json packet = {{"method", method}, {"params", params}};
    std::string data = packet.dump();

    int sent = send(sock, data.c_str(), data.size(), 0);
    if (sent < 0)
    {
        close(sock);
        return {{"error", "send_failed"}};
    }

    char buffer[8192]; // Increased buffer size
    memset(buffer, 0, sizeof(buffer));
    int n = read(sock, buffer, sizeof(buffer) - 1);
    close(sock);

    if (n <= 0)
    {
        return {{"error", "no_response"}};
    }

    try
    {
        return json::parse(buffer);
    }
    catch (const std::exception &e)
    {
        return {{"error", std::string("parse_error: ") + e.what()}};
    }
}
