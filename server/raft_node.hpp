// #ifndef RAFT_NODE_HPP
// #define RAFT_NODE_HPP
// #include <string>
// #include <vector>
// #include <mutex>
// #include <atomic>
// #include <thread>
// #include <unordered_map>
// #include "../common/json.hpp"
// #include "../common/logger.hpp"
// #include "../common/timer.hpp"
// #include "../common/rpc.hpp"
// #include "../common/thread_pool.hpp"

// using json = nlohmann::json;

// enum class Role {
//     FOLLOWER,
//     CANDIDATE,
//     LEADER
// };

// struct LogEntry {
//     int term;
//     std::string command;
// };

// class RaftNode {
// public:
//     RaftNode(std::string nodeId,
//              int rpcPort,
//              std::vector<std::string> peers);
    
//     std::string getId() const { return id; }
//     void start();
    
//     ThreadPool rpcPool;
//     // -------------------------------------------------
//     // RPC handlers
//     // -------------------------------------------------
//     json onRequestVote(const json &req);
//     json onAppendEntries(const json &req);
    
//     // -------------------------------------------------
//     // Leader functions
//     // -------------------------------------------------
//     void sendHeartbeats();
//     void replicateLog();
//     // Add this method to check if node is the leader
//     bool isLeader() {
//         std::lock_guard<std::mutex> lock(mtx);
//         return role == Role::LEADER;
//     }

// private:
//     // Identity
//     std::string id;
//     int port;
//     std::vector<std::string> peerAddrs;
    
//     // Persistent state (on stable storage)
//     int currentTerm = 0;
//     std::string votedFor = "";
//     std::vector<LogEntry> log;
    
//     // Volatile state
//     int commitIndex = -1;
//     int lastApplied = -1;
    
//     // Leader volatile state
//     std::unordered_map<std::string,int> nextIndex;
//     std::unordered_map<std::string,int> matchIndex;
    
//     // Role
//     std::atomic<Role> role {Role::FOLLOWER};
    
//     // Concurrency
//     std::mutex mtx;
    
//     Timer electionTimer;
//     Timer heartbeatTimer;
//     Logger logger;
    
//     // Internal methods - ALL called with lock held
//     void becomeFollower(int term);           // Transition to follower
//     void becomeCandidate();                   // Wrapper that acquires lock
//     void becomeLeader();                      // Wrapper that acquires lock
//     void resetElectionTimer();                // Reset election timeout
//     void applyStateMachine();                 // Apply committed entries
    
//     void startElection();                     // Start election (lock held)
//     void becomeLeaderInternal();              // Become leader (lock held)
//     void sendHeartbeatsInternal();            // Send heartbeats (lock held)
// };

// #endif

#ifndef RAFT_NODE_HPP
#define RAFT_NODE_HPP

#include <string>
#include <vector>
#include <mutex>
#include <atomic>
#include <thread>
#include <unordered_map>
#include "../common/json.hpp"
#include "../common/logger.hpp"
#include "../common/timer.hpp"
#include "../common/rpc.hpp"
#include "../common/thread_pool.hpp"
#include "state_machine.hpp"

using json = nlohmann::json;

enum class Role
{
    FOLLOWER,
    CANDIDATE,
    LEADER
};

struct LogEntry
{
    int term;
    std::string command;
};

class RaftNode
{
public:
    RaftNode(std::string nodeId,
             int rpcPort,
             std::vector<std::string> peers);

    std::string getId() const { return id; }
    void start();

    ThreadPool rpcPool;

    // -------------------------------------------------
    // RPC handlers
    // -------------------------------------------------
    json onRequestVote(const json &req);
    json onAppendEntries(const json &req);

    // -------------------------------------------------
    // Leader functions
    // -------------------------------------------------
    void sendHeartbeats();
    void replicateLog();

    // Check if node is the leader
    bool isLeader()
    {
        std::lock_guard<std::mutex> lock(mtx);
        return role == Role::LEADER;
    }

    // -------------------------------------------------
    // NEW: Client command submission
    // -------------------------------------------------

    /**
     * @brief Submit a command to be replicated through RAFT
     *
     * This is the entry point for clients to submit commands.
     * Only the leader can accept commands. Commands are:
     * 1. Appended to the leader's log
     * 2. Replicated to followers
     * 3. Applied to state machine once committed
     *
     * @param command The command string (typically JSON)
     * @return true if command was accepted (node is leader), false otherwise
     */
    bool submitCommand(const std::string &command)
    {
        std::lock_guard<std::mutex> lock(mtx);

        if (role != Role::LEADER)
        {
            logger.warn("submitCommand rejected: not leader");
            return false;
        }

        // Append to local log
        LogEntry entry;
        entry.term = currentTerm;
        entry.command = command;
        log.push_back(entry);

        logger.info("Command submitted to log at index " +
                    std::to_string(log.size() - 1) +
                    ": " + command);

        // Trigger replication to followers
        // Note: We call replicateLogInternal which assumes lock is held
        replicateLogInternal();

        return true;
    }

    // Public method to access state machine (for client queries)
    std::string query(const std::string &key)
    {
        return stateMachine.get(key);
    }

    // Query all nodes from state machine
    json queryAll()
    {
        return stateMachine.getAll();
    }

    // Query specific node state
    json queryNode(const std::string &nodeId)
    {
        return stateMachine.getNodeState(nodeId);
    }

private:
    // Identity
    std::string id;
    int port;
    std::vector<std::string> peerAddrs;

    // Persistent state (on stable storage)
    int currentTerm = 0;
    std::string votedFor = "";
    std::vector<LogEntry> log;

    // Volatile state
    int commitIndex = -1;
    int lastApplied = -1;

    // Leader volatile state
    std::unordered_map<std::string, int> nextIndex;
    std::unordered_map<std::string, int> matchIndex;

    // Role
    std::atomic<Role> role{Role::FOLLOWER};

    // State machine
    StateMachine stateMachine;

    // Concurrency
    std::mutex mtx;
    Timer electionTimer;
    Timer heartbeatTimer;
    Logger logger;

    // Internal methods - ALL called with lock held
    void becomeFollower(int term);
    void becomeCandidate();
    void becomeLeader();
    void resetElectionTimer();
    void applyStateMachine();
    void startElection();
    void becomeLeaderInternal();
    void sendHeartbeatsInternal();

    // NEW: Internal replication (called with lock held)
    void replicateLogInternal();
};

#endif