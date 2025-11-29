// #include "raft_node.hpp"
// #include "state_machine.hpp"
// #include <iostream>
// #include <thread>
// #include <vector>
// #include <set>
// #include <mutex>
// #include <arpa/inet.h>
// #include <unistd.h>
// #include <cstring>
// #include <fstream>

// // ============================================================================
// // WORKER STATUS TRACKING
// // ============================================================================

// /**
//  * @brief Tracks the health status of a worker node
//  *
//  * Contains timestamp of last heartbeat and online/offline status.
//  */
// struct WorkerStatus
// {
//     std::chrono::steady_clock::time_point lastSeen; // Last heartbeat timestamp
//     bool online;                                    // Is worker currently online?
// };

// // Global state for tracking worker nodes across the system
// // Protected by workerMutex for thread-safe access
// std::unordered_map<std::string, WorkerStatus> workerMap;
// std::mutex workerMutex;

// // ============================================================================
// // LEADER DISCOVERY SERVICE
// // ============================================================================

// /**
//  * @brief Runs the leader discovery service on port 6000
//  *
//  * This service listens for heartbeat messages from worker nodes. Only the
//  * current RAFT leader will accept and process these heartbeats. If a non-leader
//  * receives a heartbeat, it responds with NOT_LEADER so workers can find the
//  * actual leader.
//  *
//  * @param node Pointer to the RaftNode instance to check leadership status
//  *
//  * Protocol:
//  *   Worker sends: "HEARTBEAT <node_id>"
//  *   Leader responds: "ACK"
//  *   Non-leader responds: "NOT_LEADER"
//  */
// void runLeaderDiscoveryService(RaftNode *node)
// {
//     const int DISCOVERY_PORT = 6000;

//     // Create TCP socket for listening
//     int server_fd = socket(AF_INET, SOCK_STREAM, 0);
//     int opt = 1;

//     // Allow reuse of address to avoid "Address already in use" errors
//     setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

//     // Bind to all interfaces on port 6000
//     sockaddr_in addr{};
//     addr.sin_family = AF_INET;
//     addr.sin_addr.s_addr = INADDR_ANY;
//     addr.sin_port = htons(DISCOVERY_PORT);

//     if (bind(server_fd, (sockaddr *)&addr, sizeof(addr)) < 0)
//     {
//         std::cerr << "Failed to bind discovery port 6000\n";
//         return;
//     }

//     // Start listening with a backlog of 16 connections
//     listen(server_fd, 16);
//     std::cout << "[INFO] Leader discovery service listening on port 6000\n";

//     // Accept loop - runs forever
//     while (true)
//     {
//         sockaddr_in client;
//         socklen_t len = sizeof(client);

//         // Block until a worker connects
//         int client_fd = accept(server_fd, (sockaddr *)&client, &len);

//         // Handle each worker connection in a separate thread to avoid blocking
//         // Thread is detached so it cleans up automatically when done
//         std::thread([client_fd, node]()
//                     {
//             char buffer[1024];
//             memset(buffer, 0, sizeof(buffer));
            
//             // Read the heartbeat message from worker
//             int n = read(client_fd, buffer, sizeof(buffer)-1);
            
//             if (n > 0) {
//                 std::string msg(buffer);
                
//                 // Parse "HEARTBEAT node_id" message format
//                 if (msg.find("HEARTBEAT") == 0) {
//                     size_t spacePos = msg.find(' ');
//                     if (spacePos != std::string::npos) {
//                         // Extract worker ID from message
//                         std::string workerId = msg.substr(spacePos + 1);
                        
//                         // Remove trailing whitespace/newlines
//                         workerId.erase(workerId.find_last_not_of(" \n\r\t") + 1);
                        
//                         // Only process heartbeat if this node is the RAFT leader
//                         if (node->isLeader()) {
//                             // Thread-safe update of worker status
//                             std::lock_guard<std::mutex> lock(workerMutex);
                            
//                             // Update worker's last seen timestamp and online status
//                             WorkerStatus &ws = workerMap[workerId];
//                             ws.lastSeen = std::chrono::steady_clock::now();
//                             ws.online = true;

//                             std::cout << "[INFO] Received heartbeat from worker: " 
//                                      << workerId << " (total workers: " 
//                                      << workerMap.size() << ")\n";
                            
//                             // Send positive acknowledgment to worker
//                             std::string response = "ACK\n";
//                             send(client_fd, response.c_str(), response.size(), 0);
//                         } else {
//                             // This node is not the leader - reject the heartbeat
//                             // Worker should try connecting to other manager nodes
//                             std::string response = "NOT_LEADER\n";
//                             send(client_fd, response.c_str(), response.size(), 0);
//                         }
//                     }
//                 }
//             }
            
//             // Close the connection to this worker
//             close(client_fd); })
//             .detach();
//     }
// }

// // ============================================================================
// // WORKER HEALTH MONITORING
// // ============================================================================

// /**
//  * @brief Monitors worker health and generates status reports
//  *
//  * Runs in a background thread, checking every 5 seconds whether workers are
//  * still online based on their last heartbeat timestamp. Generates a JSON file
//  * (workers.json) with the current status of all tracked workers.
//  *
//  * Timeout: Workers are considered offline if no heartbeat received for 10 seconds
//  */
// void cleanupStaleWorkers()
// {
//     const auto TIMEOUT = std::chrono::seconds(10);

//     while (true)
//     {
//         // Wait 5 seconds between status updates
//         std::this_thread::sleep_for(std::chrono::seconds(5));

//         // Lock for thread-safe access to workerMap
//         std::lock_guard<std::mutex> lock(workerMutex);

//         auto now = std::chrono::steady_clock::now();

//         // Create/overwrite workers.json file in current directory
//         std::ofstream jsonFile("workers.json");
//         if (!jsonFile.is_open())
//         {
//             std::cerr << "Error: Cannot open workers.json for writing\n";
//             continue;
//         }

//         // Write JSON structure
//         jsonFile << "{\n  \"workers\": [\n";

//         bool first = true;
//         for (auto &entry : workerMap)
//         {
//             const std::string &id = entry.first;
//             WorkerStatus &ws = entry.second;

//             // Check if worker is still online based on timeout
//             bool isOnline = (now - ws.lastSeen <= TIMEOUT);
//             ws.online = isOnline;

//             // Calculate milliseconds since last heartbeat
//             long msAgo = std::chrono::duration_cast<std::chrono::milliseconds>(
//                              now - ws.lastSeen)
//                              .count();

//             // Add comma separator between JSON objects (except first)
//             if (!first)
//                 jsonFile << ",\n";
//             first = false;

//             // Write worker status as JSON object
//             jsonFile << "    {\n"
//                      << "      \"id\": \"" << id << "\",\n"
//                      << "      \"online\": " << (isOnline ? "true" : "false") << ",\n"
//                      << "      \"last_seen_ms_ago\": " << msAgo << "\n"
//                      << "    }";
//         }

//         // Close JSON structure
//         jsonFile << "\n  ]\n}\n";
//         jsonFile.close();
//     }
// }

// // ============================================================================
// // MAIN FUNCTION
// // ============================================================================

// /**
//  * @brief Entry point for the manager node process
//  *
//  * Initializes a RAFT node, starts the consensus protocol, and launches
//  * background services for worker monitoring and leader discovery.
//  *
//  * @param argc Argument count
//  * @param argv Arguments: <node_id> <raft_port> [peer1:port1,peer2:port2,...]
//  *
//  * Example usage:
//  *   ./manager node1 5000 node2:5001,node3:5002
//  *
//  * This starts:
//  *   - RAFT consensus on port 5000
//  *   - Leader discovery service on port 6000
//  *   - Peer connections to node2:5001 and node3:5002
//  */
// int main(int argc, char *argv[])
// {
//     // Validate command-line arguments
//     if (argc < 3)
//     {
//         std::cerr << "Usage: ./manager <node_id> <raft_port> [peer1:port1,peer2:port2,...]\n";
//         std::cerr << "Example: ./manager node1 5000 node2:5001,node3:5002\n";
//         return 1;
//     }

//     // Parse arguments
//     std::string nodeId = argv[1];  // Unique identifier for this node
//     int port = std::stoi(argv[2]); // Port for RAFT RPC communication

//     // Parse comma-separated peer list (optional)
//     std::vector<std::string> peers;
//     if (argc >= 4)
//     {
//         std::string peerStr = argv[3];
//         size_t pos = 0;

//         // Split by comma delimiter
//         while ((pos = peerStr.find(',')) != std::string::npos)
//         {
//             peers.push_back(peerStr.substr(0, pos));
//             peerStr.erase(0, pos + 1);
//         }

//         // Add last peer (or only peer if no commas)
//         if (!peerStr.empty())
//             peers.push_back(peerStr);
//     }

//     // Log startup information
//     std::cout << "[INFO] Starting manager node: " << nodeId
//               << " on Raft port: " << port << "\n";
//     if (!peers.empty())
//     {
//         std::cout << "[INFO] Peers: ";
//         for (const auto &p : peers)
//             std::cout << p << " ";
//         std::cout << "\n";
//     }

//     // Initialize and start the RAFT node
//     RaftNode node(nodeId, port, peers);
//     node.start();

//     // Start leader discovery service in background thread
//     // This service runs on ALL nodes but only the leader accepts heartbeats
//     std::thread(runLeaderDiscoveryService, &node).detach();

//     // Start worker health monitoring thread
//     // Continuously checks worker status and generates JSON reports
//     std::thread(cleanupStaleWorkers).detach();

//     // Initialize state machine (currently unused but available for queries)
//     StateMachine sm;

//     // Keep main thread alive - all work happens in background threads
//     while (true)
//     {
//         std::this_thread::sleep_for(std::chrono::seconds(5));
//     }

//     return 0;
// }

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
#include <fstream>

// ============================================================================
// LEADER DISCOVERY SERVICE (UPDATED TO USE RAFT LOG)
// ============================================================================

/**
 * @brief Runs the leader discovery service on port 6000
 *
 * NOW INTEGRATES WITH RAFT: Instead of directly updating workerMap,
 * heartbeats are replicated through the RAFT log to ensure all nodes
 * have consistent worker state.
 */
void runLeaderDiscoveryService(RaftNode *node)
{
    const int DISCOVERY_PORT = 6000;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(DISCOVERY_PORT);

    if (bind(server_fd, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        std::cerr << "Failed to bind discovery port 6000\n";
        return;
    }

    listen(server_fd, 16);
    std::cout << "[INFO] Leader discovery service listening on port 6000\n";

    while (true)
    {
        sockaddr_in client;
        socklen_t len = sizeof(client);
        int client_fd = accept(server_fd, (sockaddr *)&client, &len);

        std::thread([client_fd, node]()
                    {
            char buffer[1024];
            memset(buffer, 0, sizeof(buffer));
            
            int n = read(client_fd, buffer, sizeof(buffer)-1);
            
            if (n > 0) {
                std::string msg(buffer);
                
                if (msg.find("HEARTBEAT") == 0) {
                    size_t spacePos = msg.find(' ');
                    if (spacePos != std::string::npos) {
                        std::string workerId = msg.substr(spacePos + 1);
                        workerId.erase(workerId.find_last_not_of(" \n\r\t") + 1);
                        
                        if (node->isLeader()) {
                            // ============================================
                            // CRITICAL CHANGE: Replicate through RAFT log
                            // ============================================
                            
                            // Create a HEARTBEAT command as JSON
                            json command = {
                                {"action", "HEARTBEAT"},
                                {"node_id", workerId},
                                {"timestamp", std::time(nullptr)}
                            };
                            
                            // Submit to RAFT log for replication
                            bool success = node->submitCommand(command.dump());
                            
                            if (success) {
                                std::cout << "[INFO] Heartbeat from " << workerId 
                                         << " submitted to RAFT log\n";
                                std::string response = "ACK\n";
                                send(client_fd, response.c_str(), response.size(), 0);
                            } else {
                                std::cout << "[WARN] Failed to submit heartbeat to RAFT\n";
                                std::string response = "ERROR\n";
                                send(client_fd, response.c_str(), response.size(), 0);
                            }
                        } else {
                            std::string response = "NOT_LEADER\n";
                            send(client_fd, response.c_str(), response.size(), 0);
                        }
                    }
                }
            }
            
            close(client_fd); })
            .detach();
    }
}

// ============================================================================
// WORKER HEALTH MONITORING (UPDATED TO READ FROM STATE MACHINE)
// ============================================================================

/**
 * @brief Monitors worker health from replicated state machine
 *
 * NOW READS FROM STATE MACHINE: Instead of reading from local workerMap,
 * this reads from the RAFT-replicated state machine, ensuring consistent
 * views across all nodes.
 */
void generateWorkerStatusReport(RaftNode *node)
{
    const int TIMEOUT_SECONDS = 10;

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // Get all nodes from the replicated state machine
        json allNodes = node->queryAll();

        std::ofstream jsonFile("workers.json");
        if (!jsonFile.is_open())
        {
            std::cerr << "Error: Cannot open workers.json for writing\n";
            continue;
        }

        jsonFile << "{\n  \"workers\": [\n";

        bool first = true;
        time_t now = std::time(nullptr);

        for (auto it = allNodes.begin(); it != allNodes.end(); ++it)
        {
            const std::string &id = it.key();
            json nodeState = it.value();

            if (!nodeState.contains("last_seen") || !nodeState.contains("status"))
                continue;

            time_t lastSeen = nodeState["last_seen"];
            std::string status = nodeState["status"];

            // Calculate if node is online based on timeout
            long secondsAgo = now - lastSeen;
            bool isOnline = (secondsAgo <= TIMEOUT_SECONDS) && (status == "active");

            if (!first)
                jsonFile << ",\n";
            first = false;

            jsonFile << "    {\n"
                     << "      \"id\": \"" << id << "\",\n"
                     << "      \"online\": " << (isOnline ? "true" : "false") << ",\n"
                     << "      \"last_seen_seconds_ago\": " << secondsAgo << ",\n"
                     << "      \"status\": \"" << status << "\"\n"
                     << "    }";
        }

        jsonFile << "\n  ]\n}\n";
        jsonFile.close();
    }
}

// ============================================================================
// MAIN FUNCTION
// ============================================================================

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cerr << "Usage: ./manager <node_id> <raft_port> [peer1:port1,peer2:port2,...]\n";
        std::cerr << "Example: ./manager node1 5000 node2:5001,node3:5002\n";
        return 1;
    }

    std::string nodeId = argv[1];
    int port = std::stoi(argv[2]);

    std::vector<std::string> peers;
    if (argc >= 4)
    {
        std::string peerStr = argv[3];
        size_t pos = 0;
        while ((pos = peerStr.find(',')) != std::string::npos)
        {
            peers.push_back(peerStr.substr(0, pos));
            peerStr.erase(0, pos + 1);
        }
        if (!peerStr.empty())
            peers.push_back(peerStr);
    }

    std::cout << "[INFO] Starting manager node: " << nodeId
              << " on Raft port: " << port << "\n";
    if (!peers.empty())
    {
        std::cout << "[INFO] Peers: ";
        for (const auto &p : peers)
            std::cout << p << " ";
        std::cout << "\n";
    }

    // Initialize and start the RAFT node
    RaftNode node(nodeId, port, peers);
    node.start();

    // Start leader discovery service (now integrated with RAFT)
    std::thread(runLeaderDiscoveryService, &node).detach();

    // Start worker status report generator (reads from state machine)
    std::thread(generateWorkerStatusReport, &node).detach();

    // Keep main thread alive
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    return 0;
}