#include "raft_node.hpp"
#include "raft_rpc.hpp"
#include <chrono>
#include <random>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>

// Maximum entries to send per AppendEntries RPC to avoid buffer overflow
static constexpr int MAX_ENTRIES_PER_RPC = 5;

static int randomTimeout()
{
    // Random election timeout between 300–500 ms
    static std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(300, 500);
    return dist(rng);
}

RaftNode::RaftNode(std::string nodeId,
                   int rpcPort,
                   std::vector<std::string> peers)
    : rpcPool(8),
      id(nodeId),
      port(rpcPort),
      peerAddrs(peers),
      persistentState(nodeId),
      logger("node_" + nodeId + ".log")
{
    // Load persisted state into memory cache
    currentTerm = persistentState.getCurrentTerm();
    votedFor = persistentState.getVotedFor();
    log = persistentState.getLog();

    logger.info("Loaded persistent state: term=" + std::to_string(currentTerm) +
                ", votedFor=" + votedFor +
                ", log entries=" + std::to_string(log.size()));
}

void RaftNode::start()
{
    // Register RPC methods
    RaftRpc::init(port, this);

    logger.info("Node starting as follower.");

    std::lock_guard<std::mutex> lock(mtx);
    role = Role::FOLLOWER;
    resetElectionTimer();
}

RaftNode::~RaftNode()
{
    stopDiscoveryService();

    // Wait for discovery thread to finish
    if (discoveryThread.joinable())
    {
        discoveryThread.join();
    }
}

// ============================================================================
// PERSISTENT STATE HELPER METHODS
// ============================================================================

void RaftNode::updateTerm(int newTerm)
{
    // Must be called WITH lock held
    currentTerm = newTerm;
    persistentState.setCurrentTerm(newTerm);
}

void RaftNode::updateVotedFor(const std::string &candidate)
{
    // Must be called WITH lock held
    votedFor = candidate;
    persistentState.setVotedFor(candidate);
}

void RaftNode::appendToLog(const LogEntry &entry)
{
    // Must be called WITH lock held
    log.push_back(entry);
    persistentState.appendLog(entry);
}

void RaftNode::truncateLogFrom(int index)
{
    // Must be called WITH lock held
    log.erase(log.begin() + index, log.end());
    persistentState.truncateLog(index);
}

// ============================================================================
// DISCOVERY SERVICE
// ============================================================================

void RaftNode::startDiscoveryService()
{
    std::lock_guard<std::mutex> lock(discoveryMutex);

    if (runningDiscoveryService)
    {
        logger.info("Discovery service already running");
        return;
    }

    const int DISCOVERY_PORT = 6000;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        logger.warn("Failed to create discovery socket");
        return;
    }

    // Enable address reuse to avoid TIME_WAIT issues
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

#ifdef SO_REUSEPORT
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(DISCOVERY_PORT);

    if (bind(server_fd, (sockaddr *)&addr, sizeof(addr)) < 0)
    {
        logger.warn("Failed to bind discovery port 6000 (another leader may be active)");
        close(server_fd);
        return;
    }

    if (listen(server_fd, 16) < 0)
    {
        logger.warn("Failed to listen on discovery port");
        close(server_fd);
        return;
    }

    discoverySocket = server_fd;
    runningDiscoveryService = true;

    logger.info("✓ Leader discovery service started on port 6000");
    std::cout << "✓ [DISCOVERY] " << id << " now accepting worker connections on port 6000\n"
              << std::flush;

    if (discoveryThread.joinable())
    {
        discoveryThread.join();
    }

    discoveryThread = std::thread(&RaftNode::runDiscoveryService, this);
}

void RaftNode::stopDiscoveryService()
{
    std::lock_guard<std::mutex> lock(discoveryMutex);

    if (!runningDiscoveryService)
    {
        return;
    }

    logger.info("✗ Stopping discovery service (no longer leader)");
    std::cout << "✗ [DISCOVERY] " << id << " stopped accepting worker connections\n"
              << std::flush;

    runningDiscoveryService = false;

    int sock = discoverySocket.exchange(-1);
    if (sock >= 0)
    {
        shutdown(sock, SHUT_RDWR);
        close(sock);
    }
}

void RaftNode::runDiscoveryService()
{
    int serverSocket = discoverySocket;

    while (runningDiscoveryService && serverSocket >= 0)
    {
        sockaddr_in client;
        socklen_t len = sizeof(client);

        int client_fd = accept(serverSocket, (sockaddr *)&client, &len);

        if (client_fd < 0)
        {
            if (!runningDiscoveryService)
            {
                break;
            }
            if (discoverySocket == -1)
            {
                break;
            }
            continue;
        }

        struct timeval timeout;
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;
        setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

        std::thread([this, client_fd]()
                    {
            char buffer[1024];
            memset(buffer, 0, sizeof(buffer));
            
            ssize_t n = read(client_fd, buffer, sizeof(buffer) - 1);
            
            if (n > 0) {
                std::string msg(buffer);
                
                if (msg.find("HEARTBEAT") == 0) {
                    size_t spacePos = msg.find(' ');
                    if (spacePos != std::string::npos) {
                        std::string workerId = msg.substr(spacePos + 1);
                        workerId.erase(workerId.find_last_not_of(" \n\r\t") + 1);
                        
                        bool isLeader = (role.load() == Role::LEADER);
                        
                        if (isLeader) {
                            json command = {
                                {"action", "HEARTBEAT"},
                                {"node_id", workerId},
                                {"timestamp", std::time(nullptr)}
                            };
                            
                            bool success = submitCommand(command.dump());
                            
                            if (success) {
                                logger.info("Heartbeat from " + workerId + " submitted to RAFT log");
                                std::string response = "ACK\n";
                                send(client_fd, response.c_str(), response.size(), 0);
                            } else {
                                logger.warn("Failed to submit heartbeat to RAFT");
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

    logger.info("Discovery service thread exiting");
}

// ============================================================================
// RAFT CORE - ELECTIONS AND ROLE TRANSITIONS
// ============================================================================

void RaftNode::resetElectionTimer()
{
    // Must be called WITH lock held
    int t = randomTimeout();
    electionTimer.cancel();

    electionTimer.start(t, [this]()
                        {
        std::lock_guard<std::mutex> lock(mtx);
        
        if (role == Role::FOLLOWER || role == Role::CANDIDATE) {
            logger.info("Election timeout triggered (role=" + 
                       std::to_string((int)role.load()) + ")");
            startElection();
        } });
}

void RaftNode::becomeFollower(int term)
{
    // Must be called WITH lock held
    bool wasLeader = (role == Role::LEADER);

    role = Role::FOLLOWER;

    // ✓ PERSIST: Update term and clear vote BEFORE any response
    updateTerm(term);
    updateVotedFor("");

    heartbeatTimer.cancel();
    logger.info("Transition -> FOLLOWER (term " + std::to_string(term) + ")");

    resetElectionTimer();

    if (wasLeader)
    {
        mtx.unlock();
        stopDiscoveryService();
        mtx.lock();
    }
}

void RaftNode::becomeCandidate()
{
    std::lock_guard<std::mutex> lock(mtx);
    startElection();
}

void RaftNode::becomeLeader()
{
    std::lock_guard<std::mutex> lock(mtx);
    becomeLeaderInternal();
}

void RaftNode::startElection()
{
    // Called WITH lock held
    role = Role::CANDIDATE;

    // ✓ PERSIST: Increment term and vote for self BEFORE sending RPCs
    updateTerm(currentTerm + 1);
    updateVotedFor(id);

    int term = currentTerm;
    logger.info("Transition -> CANDIDATE (term " + std::to_string(term) + ")");

    resetElectionTimer();

    auto voteCount = std::make_shared<std::atomic<int>>(1);
    int needed = (peerAddrs.size() + 1) / 2 + 1;

    if (*voteCount >= needed)
    {
        logger.info("Single-node cluster detected, becoming leader immediately");
        becomeLeaderInternal();
        return;
    }

    int lastLogIdx = (int)log.size() - 1;
    int lastLogTerm = log.empty() ? 0 : log.back().term;

    auto electionWon = std::make_shared<std::atomic<bool>>(false);

    for (const auto &peerSpec : peerAddrs)
    {
        size_t colon = peerSpec.find(':');
        if (colon == std::string::npos)
            continue;

        std::string peerId = peerSpec.substr(0, colon);
        int peerPort = std::stoi(peerSpec.substr(colon + 1));

        json params = {
            {"term", term},
            {"candidateId", id},
            {"lastLogIndex", lastLogIdx},
            {"lastLogTerm", lastLogTerm}};

        rpcPool.enqueue([this, peerId, peerPort, params,
                         term, needed, voteCount, electionWon]()
                        {
            try {
                logger.info("Sending RequestVote to " + peerId);

                json resp = JsonRpcClient::call("127.0.0.1",
                                                peerPort,
                                                "RequestVote",
                                                params);

                logger.info("RequestVote response from " + peerId + ":\n" + resp.dump(4));

                if (!resp.contains("term") || !resp.contains("voteGranted"))
                    return;

                int respTerm = resp["term"];
                bool granted = resp["voteGranted"];

                std::lock_guard<std::mutex> lock(mtx);

                if (respTerm > currentTerm) {
                    becomeFollower(respTerm);
                    return;
                }

                if (role != Role::CANDIDATE || currentTerm != term)
                    return;

                if (granted) {
                    int votes = ++(*voteCount);
                    logger.info("Vote from " + peerId + " (" +
                                std::to_string(votes) + "/" +
                                std::to_string(needed) + ")");

                    if (votes >= needed && !electionWon->exchange(true)) {
                        becomeLeaderInternal();
                    }
                }

            } catch (const std::exception &e) {
                logger.warn("RequestVote FAILED to " + peerId +
                            ": " + e.what());
            } });
    }
}

void RaftNode::becomeLeaderInternal()
{
    // Must be called WITH lock held
    if (role == Role::LEADER)
        return;

    electionTimer.cancel();

    role = Role::LEADER;

    logger.info("Transition -> LEADER (term " + std::to_string(currentTerm) + ")");
    std::cout << "✓ [LEADER] " << id << " elected as leader (term "
              << currentTerm << ")\n"
              << std::flush;

    for (const auto &peerSpec : peerAddrs)
    {
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos)
            continue;
        std::string peerId = peerSpec.substr(0, colonPos);
        nextIndex[peerId] = log.size();
        matchIndex[peerId] = -1;
    }

    sendHeartbeatsInternal();

    mtx.unlock();
    startDiscoveryService();
    mtx.lock();
}

// ============================================================================
// RAFT RPC HANDLERS
// ============================================================================

json RaftNode::onRequestVote(const json &req)
{
    std::lock_guard<std::mutex> lock(mtx);

    int term = req["term"];
    std::string candidate = req["candidateId"];
    int lastLogIndex = req["lastLogIndex"];
    int lastLogTerm = req["lastLogTerm"];

    json resp;
    resp["term"] = currentTerm;
    resp["voteGranted"] = false;

    if (term < currentTerm)
    {
        logger.info("Rejecting vote for " + candidate + " (stale term " +
                    std::to_string(term) + " < " + std::to_string(currentTerm) + ")");
        return resp;
    }

    if (term > currentTerm)
    {
        becomeFollower(term);
        resp["term"] = currentTerm;
    }

    int ourLastLogTerm = log.empty() ? 0 : log.back().term;
    int ourLastLogIndex = (int)log.size() - 1;

    bool logUpToDate =
        (lastLogTerm > ourLastLogTerm) ||
        (lastLogTerm == ourLastLogTerm && lastLogIndex >= ourLastLogIndex);

    if ((votedFor == "" || votedFor == candidate) && logUpToDate)
    {
        // ✓ PERSIST: Vote BEFORE sending response
        updateVotedFor(candidate);
        resp["voteGranted"] = true;

        resetElectionTimer();

        logger.info("Granting vote to " + candidate + " for term " + std::to_string(term));
    }
    else
    {
        logger.info("Denying vote to " + candidate +
                    " (already voted for '" + votedFor + "' or log outdated)");
    }
    return resp;
}

json RaftNode::onAppendEntries(const json &req)
{
    std::lock_guard<std::mutex> lock(mtx);

    int term = req["term"];
    std::string leaderId = req["leaderId"];

    json resp;
    resp["term"] = currentTerm;
    resp["success"] = false;

    if (term < currentTerm)
    {
        logger.info("Rejecting AppendEntries from " + leaderId + " (stale term " +
                    std::to_string(term) + " < " + std::to_string(currentTerm) + ")");
        return resp;
    }

    if (term > currentTerm)
    {
        if (role == Role::CANDIDATE || role == Role::LEADER)
        {
            logger.info("Stepping down: received heartbeat from " + leaderId +
                        " (term " + std::to_string(term) + ")");
        }
        becomeFollower(term);
    }
    else if (term == currentTerm)
    {
        if (role == Role::CANDIDATE)
        {
            logger.info("Stepping down: recognized " + leaderId + " as leader");
            becomeFollower(term);
        }
        else if (role == Role::LEADER)
        {
            logger.warn("SPLIT BRAIN DETECTED: Received AppendEntries from " + leaderId +
                        " while also being leader in term " + std::to_string(term));

            if (leaderId < id)
            {
                logger.warn("Stepping down: " + leaderId + " < " + id + " (lexicographic)");
                becomeFollower(term);
            }
            else
            {
                logger.warn("Rejecting: maintaining leadership (" + id + " >= " + leaderId + ")");
                resp["success"] = false;
                return resp;
            }
        }
        else
        {
            resetElectionTimer();
        }
    }

    if (req.contains("prevLogIndex") && req.contains("prevLogTerm") &&
        req.contains("entries"))
    {
        int prevLogIndex = req["prevLogIndex"];
        int prevLogTerm = req["prevLogTerm"];
        json entries = req["entries"];

        if (prevLogIndex >= 0)
        {
            if (prevLogIndex >= (int)log.size() ||
                log[prevLogIndex].term != prevLogTerm)
            {
                resetElectionTimer();
                resp["success"] = false;
                resp["conflictIndex"] = std::min(prevLogIndex, (int)log.size());
                logger.info("Log inconsistency at index " + std::to_string(prevLogIndex) +
                            " (our log size: " + std::to_string(log.size()) + ")");
                return resp;
            }
        }

        // ✓ PERSIST: Log modifications are persisted via helper methods
        int index = prevLogIndex + 1;
        for (const auto &entry : entries)
        {
            if (index < (int)log.size())
            {
                if (log[index].term != entry["term"])
                {
                    truncateLogFrom(index);
                    appendToLog({entry["term"], entry["command"]});
                    logger.info("Replaced conflicting entry at index " + std::to_string(index));
                }
                // else: entry already exists with same term, skip it
            }
            else
            {
                appendToLog({entry["term"], entry["command"]});
                logger.info("Appended entry at index " + std::to_string(index) +
                            ": " + std::string(entry["command"]));
            }
            index++;
        }

        if (req.contains("leaderCommit"))
        {
            int leaderCommit = req["leaderCommit"];
            if (leaderCommit > commitIndex)
            {
                int oldCommit = commitIndex;
                commitIndex = std::min(leaderCommit, (int)log.size() - 1);
                logger.info("Updated commitIndex from " + std::to_string(oldCommit) +
                            " to " + std::to_string(commitIndex));

                applyStateMachine();
            }
        }
    }

    resp["success"] = true;
    resp["term"] = currentTerm;
    return resp;
}

// ============================================================================
// LEADER OPERATIONS
// ============================================================================

bool RaftNode::submitCommand(const std::string &command)
{
    std::lock_guard<std::mutex> lock(mtx);

    if (role != Role::LEADER)
    {
        logger.warn("submitCommand rejected: not leader");
        return false;
    }

    // ✓ PERSIST: Append to log BEFORE replication
    LogEntry entry;
    entry.term = currentTerm;
    entry.command = command;
    appendToLog(entry);

    int newEntryIndex = log.size() - 1;
    logger.info("Command submitted to log at index " +
                std::to_string(newEntryIndex) +
                ": " + command);

    replicateLogInternal();

    if (peerAddrs.empty())
    {
        logger.info("Single-node cluster: immediately committing entry " +
                    std::to_string(newEntryIndex));
        applyStateMachine();
    }

    return true;
}

void RaftNode::sendHeartbeats()
{
    std::lock_guard<std::mutex> lock(mtx);
    sendHeartbeatsInternal();
}

void RaftNode::sendHeartbeatsInternal()
{
    // Must be called WITH lock held
    if (role != Role::LEADER)
        return;

    int term = currentTerm;
    std::string leaderId = id;

    if (peerAddrs.empty())
    {
        applyStateMachine();

        heartbeatTimer.cancel();
        heartbeatTimer.start(50, [this]()
                             {
            std::lock_guard<std::mutex> lock(mtx);
            sendHeartbeatsInternal(); });
        return;
    }

    logger.info("Sending heartbeats (term " + std::to_string(term) + ")");

    for (const auto &peerSpec : peerAddrs)
    {
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos)
        {
            continue;
        }
        std::string peerId = peerSpec.substr(0, colonPos);
        int peerPort = std::stoi(peerSpec.substr(colonPos + 1));

        int nextIdx = nextIndex[peerId];
        int prevLogIndex = nextIdx - 1;
        int prevLogTerm = (prevLogIndex >= 0 && prevLogIndex < (int)log.size())
                              ? log[prevLogIndex].term
                              : 0;

        json entries = json::array();
        int entriesCount = 0;
        for (int i = nextIdx; i < (int)log.size() && entriesCount < MAX_ENTRIES_PER_RPC; i++)
        {
            entries.push_back({{"term", log[i].term},
                               {"command", log[i].command}});
            entriesCount++;
        }

        json params = {
            {"term", term},
            {"leaderId", leaderId},
            {"prevLogIndex", prevLogIndex},
            {"prevLogTerm", prevLogTerm},
            {"entries", entries},
            {"leaderCommit", commitIndex}};

        rpcPool.enqueue([this, peerId, peerPort, params, term, nextIdx, entriesCount]()
                        {
            try {
                json resp = JsonRpcClient::call("127.0.0.1", peerPort,
                                                "AppendEntries", params);
                
                if (entriesCount > 0) {
                    logger.info("Sent " + std::to_string(entriesCount) + 
                               " entries to " + peerId + " (nextIndex=" + 
                               std::to_string(nextIdx) + ")");
                }
                
                if (resp.contains("term")) {
                    int respTerm = resp["term"];
                    bool success = resp.value("success", false);
                    
                    std::lock_guard<std::mutex> lock(mtx);
                    
                    if (respTerm > currentTerm) {
                        logger.info("Stepping down: received higher term " + 
                                   std::to_string(respTerm) + " from " + peerId);
                        becomeFollower(respTerm);
                        return;
                    }
                    
                    if (role != Role::LEADER || currentTerm != term)
                        return;
                    
                    if (success && entriesCount > 0) {
                        matchIndex[peerId] = nextIdx + entriesCount - 1;
                        nextIndex[peerId] = matchIndex[peerId] + 1;
                        
                        logger.info("Log replication success to " + peerId + 
                                   " (matchIndex=" + std::to_string(matchIndex[peerId]) + 
                                   ", remaining=" + std::to_string((int)log.size() - nextIndex[peerId]) + ")");
                        
                        applyStateMachine();
                        
                        if (nextIndex[peerId] < (int)log.size()) {
                            logger.info("More entries to replicate to " + peerId + 
                                       ", will send in next heartbeat");
                        }
                    } else if (!success) {
                        if (nextIndex[peerId] > 0) {
                            if (resp.contains("conflictIndex")) {
                                int conflictIdx = resp["conflictIndex"];
                                nextIndex[peerId] = std::max(0, conflictIdx);
                                logger.info("Fast backtrack to index " + 
                                           std::to_string(nextIndex[peerId]) + " for " + peerId);
                            } else {
                                nextIndex[peerId]--;
                                logger.info("Decrementing nextIndex to " + 
                                           std::to_string(nextIndex[peerId]) + " for " + peerId);
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                logger.info("FAILED: Heartbeat to " + peerId + " - " + 
                       std::string(e.what()));
            } });
    }

    heartbeatTimer.cancel();
    heartbeatTimer.start(50, [this]()
                         {
        std::lock_guard<std::mutex> lock(mtx);
        sendHeartbeatsInternal(); });
}

void RaftNode::replicateLog()
{
    std::lock_guard<std::mutex> lock(mtx);

    if (role != Role::LEADER)
    {
        logger.warn("replicateLog called but not leader");
        return;
    }

    if (peerAddrs.empty())
    {
        applyStateMachine();
        return;
    }

    int term = currentTerm;
    std::string leaderId = id;

    logger.info("Replicating log entries to followers");

    for (const auto &peerSpec : peerAddrs)
    {
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos)
            continue;

        std::string peerId = peerSpec.substr(0, colonPos);
        int peerPort = std::stoi(peerSpec.substr(colonPos + 1));

        int nextIdx = nextIndex[peerId];
        int prevLogIndex = nextIdx - 1;
        int prevLogTerm = (prevLogIndex >= 0 && prevLogIndex < (int)log.size())
                              ? log[prevLogIndex].term
                              : 0;

        json entries = json::array();
        int entriesCount = 0;
        for (int i = nextIdx; i < (int)log.size() && entriesCount < MAX_ENTRIES_PER_RPC; i++)
        {
            entries.push_back({{"term", log[i].term},
                               {"command", log[i].command}});
            entriesCount++;
        }

        json params = {
            {"term", term},
            {"leaderId", leaderId},
            {"prevLogIndex", prevLogIndex},
            {"prevLogTerm", prevLogTerm},
            {"entries", entries},
            {"leaderCommit", commitIndex}};

        rpcPool.enqueue([this, peerId, peerPort, params, term, nextIdx, entriesCount]()
                        {
            try {
                json resp = JsonRpcClient::call("127.0.0.1", peerPort,
                                                "AppendEntries", params);
                
                if (!resp.contains("term") || !resp.contains("success"))
                    return;

                int respTerm = resp["term"];
                bool success = resp["success"];

                std::lock_guard<std::mutex> lock(mtx);

                if (respTerm > currentTerm) {
                    becomeFollower(respTerm);
                    return;
                }

                if (role != Role::LEADER || currentTerm != term)
                    return;

                if (success) {
                    if (entriesCount > 0) {
                        matchIndex[peerId] = nextIdx + entriesCount - 1;
                        nextIndex[peerId] = matchIndex[peerId] + 1;
                        
                        logger.info("Log replication success to " + peerId + 
                                   " (matchIndex=" + std::to_string(matchIndex[peerId]) + ")");
                        
                        applyStateMachine();
                    }
                } else {
                    if (nextIndex[peerId] > 0) {
                        if (resp.contains("conflictIndex")) {
                            int conflictIdx = resp["conflictIndex"];
                            nextIndex[peerId] = std::max(0, conflictIdx);
                            logger.info("Fast backtrack to index " + 
                                       std::to_string(nextIndex[peerId]) + " for " + peerId);
                        } else {
                            nextIndex[peerId]--;
                            logger.info("Log replication failed to " + peerId + 
                                       ", decrementing nextIndex to " + 
                                       std::to_string(nextIndex[peerId]));
                        }
                    }
                }

            } catch (const std::exception& e) {
                logger.warn("Log replication to " + peerId + " failed: " + e.what());
            } });
    }
}

void RaftNode::replicateLogInternal()
{
    // Must be called WITH lock held
    if (role != Role::LEADER)
    {
        logger.warn("replicateLogInternal called but not leader");
        return;
    }

    if (peerAddrs.empty())
    {
        applyStateMachine();
        return;
    }

    int term = currentTerm;
    std::string leaderId = id;

    logger.info("Replicating log entries to followers");

    for (const auto &peerSpec : peerAddrs)
    {
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos)
            continue;

        std::string peerId = peerSpec.substr(0, colonPos);
        int peerPort = std::stoi(peerSpec.substr(colonPos + 1));

        int nextIdx = nextIndex[peerId];
        int prevLogIndex = nextIdx - 1;
        int prevLogTerm = (prevLogIndex >= 0 && prevLogIndex < (int)log.size())
                              ? log[prevLogIndex].term
                              : 0;

        json entries = json::array();
        int entriesCount = 0;
        for (int i = nextIdx; i < (int)log.size() && entriesCount < MAX_ENTRIES_PER_RPC; i++)
        {
            entries.push_back({{"term", log[i].term},
                               {"command", log[i].command}});
            entriesCount++;
        }

        json params = {
            {"term", term},
            {"leaderId", leaderId},
            {"prevLogIndex", prevLogIndex},
            {"prevLogTerm", prevLogTerm},
            {"entries", entries},
            {"leaderCommit", commitIndex}};

        rpcPool.enqueue([this, peerId, peerPort, params, term, nextIdx, entriesCount]()
                        {
            try {
                json resp = JsonRpcClient::call("127.0.0.1", peerPort,
                                                "AppendEntries", params);
                
                if (!resp.contains("term") || !resp.contains("success"))
                    return;

                int respTerm = resp["term"];
                bool success = resp["success"];

                std::lock_guard<std::mutex> lock(mtx);

                if (respTerm > currentTerm) {
                    becomeFollower(respTerm);
                    return;
                }

                if (role != Role::LEADER || currentTerm != term)
                    return;

                if (success) {
                    if (entriesCount > 0) {
                        matchIndex[peerId] = nextIdx + entriesCount - 1;
                        nextIndex[peerId] = matchIndex[peerId] + 1;
                        
                        logger.info("Log replication success to " + peerId + 
                                   " (matchIndex=" + std::to_string(matchIndex[peerId]) + ")");
                        
                        applyStateMachine();
                    }
                } else {
                    if (nextIndex[peerId] > 0) {
                        if (resp.contains("conflictIndex")) {
                            int conflictIdx = resp["conflictIndex"];
                            nextIndex[peerId] = std::max(0, conflictIdx);
                            logger.info("Fast backtrack to index " + 
                                       std::to_string(nextIndex[peerId]) + " for " + peerId);
                        } else {
                            nextIndex[peerId]--;
                            logger.info("Log replication failed to " + peerId + 
                                       ", decrementing nextIndex to " + 
                                       std::to_string(nextIndex[peerId]));
                        }
                    }
                }

            } catch (const std::exception& e) {
                logger.warn("Log replication to " + peerId + " failed: " + e.what());
            } });
    }
}

void RaftNode::applyStateMachine()
{
    // Must be called WITH lock held

    for (int n = (int)log.size() - 1; n > commitIndex; n--)
    {
        if (log[n].term != currentTerm)
            continue;

        int replicationCount = 1;
        for (const auto &pair : matchIndex)
        {
            if (pair.second >= n)
            {
                replicationCount++;
            }
        }

        int majority = (peerAddrs.size() + 1) / 2 + 1;
        if (replicationCount >= majority)
        {
            int oldCommit = commitIndex;
            commitIndex = n;
            logger.info("Advanced commitIndex from " + std::to_string(oldCommit) +
                        " to " + std::to_string(commitIndex));
            break;
        }
    }

    while (lastApplied < commitIndex)
    {
        lastApplied++;
        const LogEntry &entry = log[lastApplied];

        logger.info("Applying to state machine: " + entry.command +
                    " (index=" + std::to_string(lastApplied) +
                    ", term=" + std::to_string(entry.term) + ")");

        stateMachine.apply(entry.command);
    }
}