#include "raft_rpc.hpp"
#include <iostream>

std::unordered_map<std::string,int> RaftRpc::registry;

void RaftRpc::init(int port, RaftNode *node) {

    registry[node->getId()] = port;

    JsonRpcServer *srv = new JsonRpcServer(port);

    srv->registerMethod("RequestVote",
        [node](json req) { return node->onRequestVote(req); });

    srv->registerMethod("AppendEntries",
        [node](json req) { return node->onAppendEntries(req); });

    srv->start();
}

int RaftRpc::portOf(const std::string &nodeId) {
    return registry[nodeId];
}
