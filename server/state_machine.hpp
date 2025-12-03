#ifndef STATE_MACHINE_HPP
#define STATE_MACHINE_HPP

#include <string>
#include <mutex>
#include <unordered_map>
#include <ctime>
#include "../common/logger.hpp"
#include "../common/json.hpp"

using json = nlohmann::json;

struct NodeState
{
    time_t last_seen;
    std::string status; // "active", "inactive", "dead", "unknown"

    std::string ip_address;
    int cpu_usage;
    int memory_usage;
    std::string metadata;

    NodeState()
        : last_seen(0), status("unknown"), cpu_usage(0), memory_usage(0) {}

    json toJson() const
    {
        return json{
            {"last_seen", last_seen},
            {"status", status},
            {"ip_address", ip_address},
            {"cpu_usage", cpu_usage},
            {"memory_usage", memory_usage},
            {"metadata", metadata}};
    }
};

class StateMachine
{
    std::unordered_map<std::string, NodeState> nodes;
    std::mutex mtx;
    Logger logger;

public:
    StateMachine();

    // Apply a command to the state machine
    void apply(const std::string &command);

    // Query methods
    std::string get(const std::string &nodeId);
    json getAll();
    json getNodeState(const std::string &nodeId);
    std::vector<std::string> getActiveNodes();
    std::vector<std::string> getInactiveNodes();

    // Check if a node exists
    bool hasNode(const std::string &nodeId);

    // Get count of nodes by status
    int countByStatus(const std::string &status);

private:
    void handleUpdateNode(const json &params);
    void handleRemoveNode(const std::string &nodeId);
    void handleSetStatus(const std::string &nodeId, const std::string &status);
};

#endif