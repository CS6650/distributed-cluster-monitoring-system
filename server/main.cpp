#include "raft_node.hpp"
#include "state_machine.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <fstream>

void generateWorkerStatusReport(RaftNode *node)
{
    const int TIMEOUT_SECONDS = 10;

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));

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

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cerr << "Usage: ./manager <node_id> <raft_port> [peer1:port1,peer2:port2,...]\n";
        std::cerr << "Example: ./manager node1 5001 node2:5002,node3:5003\n";
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

    std::cout << "[STARTUP] Manager " << nodeId << " starting on port " << port << "\n";
    if (!peers.empty())
    {
        std::cout << "[STARTUP] Peers configured: " << peers.size() << " nodes\n";
    }

    RaftNode node(nodeId, port, peers);
    node.start();

    std::thread(generateWorkerStatusReport, &node).detach();

    std::cout << "[READY] Monitoring system running. Check " << nodeId 
              << ".log for details.\n";

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    return 0;
}


