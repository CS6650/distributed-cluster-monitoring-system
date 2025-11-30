
// #ifndef RAFT_RPC_HPP
// #define RAFT_RPC_HPP

// #include "../common/rpc.hpp"
// #include "raft_node.hpp"
// #include <unordered_map>

// class RaftRpc {
// public:
//     static void init(int port, RaftNode *node);
//     static int portOf(const std::string &nodeId);

// private:
//     static std::unordered_map<std::string,int> registry;
// };

// #endif

#ifndef RAFT_RPC_HPP
#define RAFT_RPC_HPP

#include "raft_node.hpp"
#include "../common/rpc.hpp"
#include <unordered_map>
#include <string>

class RaftRpc
{
public:
    static void init(int port, RaftNode *node);
    static int portOf(const std::string &nodeId);
    static void cleanup(); // Optional: for graceful shutdown

private:
    static std::unordered_map<std::string, int> registry;
    static std::unordered_map<std::string, JsonRpcServer *> servers;
};

#endif