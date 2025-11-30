// #include "raft_rpc.hpp"
// #include <iostream>

// std::unordered_map<std::string,int> RaftRpc::registry;

// void RaftRpc::init(int port, RaftNode *node) {

//     registry[node->getId()] = port;

//     JsonRpcServer *srv = new JsonRpcServer(port);

//     srv->registerMethod("RequestVote",
//         [node](json req) { return node->onRequestVote(req); });

//     srv->registerMethod("AppendEntries",
//         [node](json req) { return node->onAppendEntries(req); });

//     srv->start();
// }

// int RaftRpc::portOf(const std::string &nodeId) {
//     return registry[nodeId];
// }

#include "raft_rpc.hpp"
#include <iostream>
#include <thread>
#include <chrono>

std::unordered_map<std::string, int> RaftRpc::registry;
std::unordered_map<std::string, JsonRpcServer *> RaftRpc::servers;

void RaftRpc::init(int port, RaftNode *node)
{
    registry[node->getId()] = port;

    std::cout << "Initializing RPC server for " << node->getId()
              << " on port " << port << std::endl;

    // Create and configure server
    JsonRpcServer *srv = new JsonRpcServer(port);

    // Register RAFT RPC methods
    srv->registerMethod("RequestVote",
                        [node](json req)
                        { return node->onRequestVote(req); });
    srv->registerMethod("AppendEntries",
                        [node](json req)
                        { return node->onAppendEntries(req); });

    // Start the server (spawns accept thread)
    srv->start();

    // CRITICAL FIX: Wait for server to fully initialize
    // This prevents "connect_failed" errors when other nodes try to reach us
    // immediately after we restart. The delay ensures:
    // 1. The server thread has spawned
    // 2. bind() has completed (important after crash - port may be in TIME_WAIT)
    // 3. listen() has completed
    // 4. The accept loop is running and ready to receive connections
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Store server pointer to prevent memory leak and allow cleanup
    servers[node->getId()] = srv;

    std::cout << "RPC server for " << node->getId()
              << " is ready and accepting connections" << std::endl;
}

int RaftRpc::portOf(const std::string &nodeId)
{
    auto it = registry.find(nodeId);
    if (it != registry.end())
    {
        return it->second;
    }
    return -1; // Not found
}

void RaftRpc::cleanup()
{
    // Optional: Add cleanup method to properly shut down servers
    for (auto &pair : servers)
    {
        delete pair.second;
    }
    servers.clear();
    registry.clear();
}
