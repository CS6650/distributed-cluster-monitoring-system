// #include "state_machine.hpp"
// #include <sstream>

// StateMachine::StateMachine() : logger("state_machine.log") {}

// void StateMachine::apply(const std::string &command) {
//     std::lock_guard<std::mutex> lock(mtx);

//     // Simple command parser: "SET key value"
//     std::istringstream iss(command);
//     std::string cmd, key, value;
//     iss >> cmd >> key >> value;
//     if (cmd == "SET") {
//         data[key] = value;
//         logger.info("Applied command: " + command);
//     } else {
//         logger.warn("Unknown command: " + command);
//     }
// }

// std::string StateMachine::get(const std::string &key) {
//     std::lock_guard<std::mutex> lock(mtx);
//     if (data.count(key)) return data[key];
//     return "";
// }


// ============================================================================
// state_machine.cpp
// ============================================================================
#include "state_machine.hpp"
#include <sstream>
#include <algorithm>

StateMachine::StateMachine() : logger("state_machine.log") {}

void StateMachine::apply(const std::string &command)
{
    std::lock_guard<std::mutex> lock(mtx);

    try
    {
        // Parse command as JSON
        json cmd = json::parse(command);

        if (!cmd.contains("action"))
        {
            logger.warn("Command missing 'action' field: " + command);
            return;
        }

        std::string action = cmd["action"];

        if (action == "UPDATE_NODE")
        {
            // Update node state
            // Format: {"action": "UPDATE_NODE", "node_id": "node1", "status": "active", "last_seen": 1234567890, ...}
            handleUpdateNode(cmd);
        }
        else if (action == "REMOVE_NODE")
        {
            // Remove a node from tracking
            // Format: {"action": "REMOVE_NODE", "node_id": "node1"}
            if (cmd.contains("node_id"))
            {
                handleRemoveNode(cmd["node_id"]);
            }
        }
        else if (action == "SET_STATUS")
        {
            // Update just the status
            // Format: {"action": "SET_STATUS", "node_id": "node1", "status": "inactive"}
            if (cmd.contains("node_id") && cmd.contains("status"))
            {
                handleSetStatus(cmd["node_id"], cmd["status"]);
            }
        }
        else if (action == "HEARTBEAT")
        {
            // Simple heartbeat update
            // Format: {"action": "HEARTBEAT", "node_id": "node1"}
            if (cmd.contains("node_id"))
            {
                std::string nodeId = cmd["node_id"];
                nodes[nodeId].last_seen = std::time(nullptr);
                nodes[nodeId].status = "active";
                logger.info("Heartbeat from " + nodeId);
            }
        }
        else
        {
            logger.warn("Unknown action: " + action);
        }
    }
    catch (const json::exception &e)
    {
        // Fallback: try to parse as simple key-value command for backwards compatibility
        // Format: "SET key value"
        std::istringstream iss(command);
        std::string cmd, key, value;
        iss >> cmd >> key >> value;

        if (cmd == "SET")
        {
            NodeState state;
            state.last_seen = std::time(nullptr);
            state.status = value;
            nodes[key] = state;
            logger.info("Applied legacy command: " + command);
        }
        else
        {
            logger.warn("Failed to parse command: " + command + " - " + e.what());
        }
    }
}

void StateMachine::handleUpdateNode(const json &params)
{
    if (!params.contains("node_id"))
    {
        logger.warn("UPDATE_NODE missing node_id");
        return;
    }

    std::string nodeId = params["node_id"];
    NodeState &state = nodes[nodeId];

    // Update fields if present
    if (params.contains("status"))
    {
        state.status = params["status"];
    }

    if (params.contains("last_seen"))
    {
        state.last_seen = params["last_seen"];
    }
    else
    {
        state.last_seen = std::time(nullptr);
    }

    if (params.contains("ip_address"))
    {
        state.ip_address = params["ip_address"];
    }

    if (params.contains("cpu_usage"))
    {
        state.cpu_usage = params["cpu_usage"];
    }

    if (params.contains("memory_usage"))
    {
        state.memory_usage = params["memory_usage"];
    }

    if (params.contains("metadata"))
    {
        if (params["metadata"].is_string())
        {
            state.metadata = params["metadata"];
        }
        else
        {
            state.metadata = params["metadata"].dump();
        }
    }

    logger.info("Updated node: " + nodeId + " -> " + state.toJson().dump());
}

void StateMachine::handleRemoveNode(const std::string &nodeId)
{
    auto it = nodes.find(nodeId);
    if (it != nodes.end())
    {
        nodes.erase(it);
        logger.info("Removed node: " + nodeId);
    }
    else
    {
        logger.warn("Cannot remove non-existent node: " + nodeId);
    }
}

void StateMachine::handleSetStatus(const std::string &nodeId, const std::string &status)
{
    nodes[nodeId].status = status;
    nodes[nodeId].last_seen = std::time(nullptr);
    logger.info("Set status for " + nodeId + ": " + status);
}

std::string StateMachine::get(const std::string &nodeId)
{
    std::lock_guard<std::mutex> lock(mtx);

    auto it = nodes.find(nodeId);
    if (it != nodes.end())
    {
        return it->second.status;
    }
    return "";
}

json StateMachine::getAll()
{
    std::lock_guard<std::mutex> lock(mtx);

    json result = json::object();
    for (const auto &pair : nodes)
    {
        result[pair.first] = pair.second.toJson();
    }
    return result;
}

json StateMachine::getNodeState(const std::string &nodeId)
{
    std::lock_guard<std::mutex> lock(mtx);

    auto it = nodes.find(nodeId);
    if (it != nodes.end())
    {
        return it->second.toJson();
    }
    return json::object();
}

std::vector<std::string> StateMachine::getActiveNodes()
{
    std::lock_guard<std::mutex> lock(mtx);

    std::vector<std::string> activeNodes;
    time_t now = std::time(nullptr);

    for (const auto &pair : nodes)
    {
        // Consider a node active if last_seen within last 30 seconds
        if (pair.second.status == "active" &&
            (now - pair.second.last_seen) < 30)
        {
            activeNodes.push_back(pair.first);
        }
    }
    return activeNodes;
}

std::vector<std::string> StateMachine::getInactiveNodes()
{
    std::lock_guard<std::mutex> lock(mtx);

    std::vector<std::string> inactiveNodes;
    time_t now = std::time(nullptr);

    for (const auto &pair : nodes)
    {
        // Consider a node inactive if status != active OR last_seen > 30 seconds ago
        if (pair.second.status != "active" ||
            (now - pair.second.last_seen) >= 30)
        {
            inactiveNodes.push_back(pair.first);
        }
    }
    return inactiveNodes;
}

bool StateMachine::hasNode(const std::string &nodeId)
{
    std::lock_guard<std::mutex> lock(mtx);
    return nodes.find(nodeId) != nodes.end();
}

int StateMachine::countByStatus(const std::string &status)
{
    std::lock_guard<std::mutex> lock(mtx);

    int count = 0;
    for (const auto &pair : nodes)
    {
        if (pair.second.status == status)
        {
            count++;
        }
    }
    return count;
}