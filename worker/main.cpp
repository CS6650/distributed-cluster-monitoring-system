#include <iostream>
#include <thread>
#include <chrono>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include "../common/logger.hpp"

const char *LEADER_HOST = "127.0.0.1";
const int LEADER_PORT = 6000; // leader discovery port

void sendHeartbeat(const std::string &nodeId, Logger &logger)
{
    int failureCount = 0;
    bool wasConnected = false;

    while (true)
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0)
        {
            logger.warn("Failed to create socket");
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }

        // Set socket timeout to avoid hanging
        struct timeval timeout;
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        sockaddr_in serv_addr{};
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(LEADER_PORT);
        inet_pton(AF_INET, LEADER_HOST, &serv_addr.sin_addr);

        if (connect(sock, (sockaddr *)&serv_addr, sizeof(serv_addr)) == 0)
        {
            // Connected successfully
            std::string msg = "HEARTBEAT " + nodeId + "\n";
            ssize_t sent = send(sock, msg.c_str(), msg.size(), 0);

            if (sent > 0)
            {
                // Wait for response
                char buffer[256];
                memset(buffer, 0, sizeof(buffer));
                ssize_t received = recv(sock, buffer, sizeof(buffer) - 1, 0);

                if (received > 0)
                {
                    std::string response(buffer);

                    if (response.find("ACK") != std::string::npos)
                    {
                        logger.info("Heartbeat sent from " + nodeId + " - ACK received");
                        // failureCount = 0; // Reset failure counter
                        if (!wasConnected) {
                            std::cout << " [CONNECTED] " << nodeId 
                                      << " connected to leader\n" << std::flush;
                            wasConnected = true;
                        }
                        
                        failureCount = 0;
                    }
                    else if (response.find("NOT_LEADER") != std::string::npos)
                    {
                        logger.warn("Connected node is not the leader - waiting for leader election");
                        failureCount++;
                        wasConnected = false; 
                    }
                }
                else
                {
                    logger.warn("No response from leader");
                    failureCount++;
                    wasConnected = false; 
                }
            }
            else
            {
                logger.warn("Failed to send heartbeat");
                failureCount++;
                wasConnected = false; 
            }
        }
        else
        {
            logger.warn("Cannot reach leader at " + std::string(LEADER_HOST) + ":" +
                        std::to_string(LEADER_PORT) + " (attempt " +
                        std::to_string(failureCount + 1) + ")");
            failureCount++;
            if (wasConnected) {
                std::cout << "[DISCONNECTED] " << nodeId 
                          << " lost connection to leader\n" << std::flush;
                wasConnected = false;
            }
        }

        close(sock);

        // Exponential backoff on repeated failures (max 30 seconds)
        int sleepTime = std::min(5 + (failureCount * 2), 30);
        std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
    }
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: ./worker <node_id>\n";
        std::cerr << "Example: ./worker worker1\n";
        return 1;
    }

    std::string nodeId = argv[1];
    Logger logger(nodeId + ".log");
    
    std::cout << "[STARTUP] Worker " << nodeId << " starting\n";
    std::cout << "[READY] Sending heartbeats to port 6000. Check " 
              << nodeId << ".log for details.\n";

    std::thread(sendHeartbeat, nodeId, std::ref(logger)).detach();

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }

    return 0;

    // std::string nodeId = argv[1];
    // Logger logger(nodeId + ".log");
    // logger.info("Worker started with node ID: " + nodeId);
    // std::cout << "[INFO] Worker " << nodeId << " starting - will connect to leader at "
    //           << LEADER_HOST << ":" << LEADER_PORT << "\n";

    // std::thread(sendHeartbeat, nodeId, std::ref(logger)).detach();

    // // Keep main thread alive
    // while (true)
    // {
    //     std::this_thread::sleep_for(std::chrono::seconds(60));
    // }

    // return 0;
}