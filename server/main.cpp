#include "raft_node.hpp"
#include "state_machine.hpp"
#include <iostream>
#include <thread>

int main(int argc, char* argv[]) {

    if (argc < 3) {
        std::cerr << "Usage: ./manager <node_id> <port> [peer1,peer2,...]\n";
        return 1;
    }

    std::string nodeId = argv[1];
    int port = std::stoi(argv[2]);

    std::vector<std::string> peers;
    if (argc >= 4) {
        std::string peerStr = argv[3];
        size_t pos = 0;
        while ((pos = peerStr.find(',')) != std::string::npos) {
            peers.push_back(peerStr.substr(0, pos));
            peerStr.erase(0, pos + 1);
        }
        if (!peerStr.empty()) peers.push_back(peerStr);
    }

    RaftNode node(nodeId, port, peers);
    node.start();

    // Dummy state machine thread (for demonstration)
    StateMachine sm;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    return 0;
}
