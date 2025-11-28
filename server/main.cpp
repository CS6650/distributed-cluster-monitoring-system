#include "raft_node.hpp"
#include "state_machine.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <set>
#include <mutex>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

// Global state for tracking worker nodes
std::set<std::string> activeWorkers;
std::mutex workerMutex;

// Leader discovery service - listens on port 6000
void runLeaderDiscoveryService(RaftNode* node) {
    const int DISCOVERY_PORT = 6000;
    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(DISCOVERY_PORT);
    
    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Failed to bind discovery port 6000\n";
        return;
    }
    
    listen(server_fd, 16);
    std::cout << "[INFO] Leader discovery service listening on port 6000\n";
    
    while (true) {
        sockaddr_in client;
        socklen_t len = sizeof(client);
        int client_fd = accept(server_fd, (sockaddr*)&client, &len);
        
        // Handle each worker connection in a separate thread
        std::thread([client_fd, node]() {
            char buffer[1024];
            memset(buffer, 0, sizeof(buffer));
            int n = read(client_fd, buffer, sizeof(buffer)-1);
            
            if (n > 0) {
                std::string msg(buffer);
                
                // Parse "HEARTBEAT node_id" message
                if (msg.find("HEARTBEAT") == 0) {
                    size_t spacePos = msg.find(' ');
                    if (spacePos != std::string::npos) {
                        std::string workerId = msg.substr(spacePos + 1);
                        // Remove trailing newline
                        workerId.erase(workerId.find_last_not_of(" \n\r\t") + 1);
                        
                        // Only accept if we're the leader
                        if (node->isLeader()) {
                            std::lock_guard<std::mutex> lock(workerMutex);
                            activeWorkers.insert(workerId);
                            
                            std::cout << "[INFO] Received heartbeat from worker: " 
                                     << workerId << " (total workers: " 
                                     << activeWorkers.size() << ")\n";
                            
                            // Send acknowledgment
                            std::string response = "ACK\n";
                            send(client_fd, response.c_str(), response.size(), 0);
                        } else {
                            // Not the leader - send rejection
                            std::string response = "NOT_LEADER\n";
                            send(client_fd, response.c_str(), response.size(), 0);
                        }
                    }
                }
            }
            
            close(client_fd);
        }).detach();
    }
}

// Periodic cleanup of stale workers
void cleanupStaleWorkers() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        
        std::lock_guard<std::mutex> lock(workerMutex);
        // In production, track last heartbeat time and remove stale workers
        std::cout << "[INFO] Active workers: " << activeWorkers.size() << "\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: ./manager <node_id> <raft_port> [peer1:port1,peer2:port2,...]\n";
        std::cerr << "Example: ./manager node1 5000 node2:5001,node3:5002\n";
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
    
    std::cout << "[INFO] Starting manager node: " << nodeId 
              << " on Raft port: " << port << "\n";
    if (!peers.empty()) {
        std::cout << "[INFO] Peers: ";
        for (const auto& p : peers) std::cout << p << " ";
        std::cout << "\n";
    }
    
    // Start Raft node
    RaftNode node(nodeId, port, peers);
    node.start();
    
    // Start leader discovery service (runs on all nodes, but only leader accepts workers)
    std::thread(runLeaderDiscoveryService, &node).detach();
    
    // Start worker cleanup thread
    std::thread(cleanupStaleWorkers).detach();
    
    // Dummy state machine thread
    StateMachine sm;
    
    // Keep main thread alive
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    
    return 0;
}