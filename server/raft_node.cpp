#include "raft_node.hpp"
#include "raft_rpc.hpp"
#include <chrono>
#include <random>
#include <algorithm>

static int randomTimeout() {
    // Random election timeout between 300–500 ms
    static std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(300, 500);
    return dist(rng);
}

RaftNode::RaftNode(std::string nodeId,
                   int rpcPort,
                   std::vector<std::string> peers)
    : id(nodeId),
      port(rpcPort),
      peerAddrs(peers),
      logger("node_" + nodeId + ".log")
{
}

void RaftNode::start() {
    // Register RPC methods
    RaftRpc::init(port, this);

    logger.info("Node starting as follower.");
    
    std::lock_guard<std::mutex> lock(mtx);
    role = Role::FOLLOWER;
    resetElectionTimer();
}

void RaftNode::resetElectionTimer() {
    // Must be called WITH lock held
    int t = randomTimeout();
    electionTimer.cancel();
    electionTimer.start(t, [this]() {
        std::lock_guard<std::mutex> lock(mtx);
        if (role == Role::FOLLOWER) {
            startElection();
        }
    });
}

void RaftNode::becomeFollower(int term) {
    // Must be called WITH lock held
    role = Role::FOLLOWER;
    currentTerm = term;
    votedFor = "";
    heartbeatTimer.cancel(); // Stop sending heartbeats if we were leader
    logger.info("Transition -> FOLLOWER (term " + std::to_string(term) + ")");
    resetElectionTimer();
}

void RaftNode::startElection() {
    // Must be called WITH lock held
    role = Role::CANDIDATE;
    currentTerm++;
    votedFor = id;

    int term = currentTerm;
    logger.info("Transition -> CANDIDATE (term " + std::to_string(term) + ")");

    resetElectionTimer();

    // Vote tracking with thread-safe shared state
    auto voteCount = std::make_shared<std::atomic<int>>(1); // Vote for self
    int needed = (peerAddrs.size() + 1) / 2 + 1;

    // Check if already have majority (single node case)
    if (*voteCount >= needed) {
        logger.info("Already have majority (" + std::to_string(*voteCount) + 
                   "/" + std::to_string(needed) + "), becoming leader immediately");
        becomeLeaderInternal();
        return;
    }

    int lastLogIdx = (int)log.size() - 1;
    int lastLogTrm = log.empty() ? 0 : log.back().term;

    // Send RequestVote RPCs - Release lock before network I/O
    for (const auto &peerSpec : peerAddrs) {
        // Parse "nodeX:port" format
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos) continue;
        
        std::string peerId = peerSpec.substr(0, colonPos);
        int peerPort = std::stoi(peerSpec.substr(colonPos + 1));

        json params = {
            {"term", term},
            {"candidateId", id},
            {"lastLogIndex", lastLogIdx},
            {"lastLogTerm", lastLogTrm}
        };

        // Use a thread pool or limit concurrent threads in production
        std::thread([this, peerId, peerPort, params, term, needed, voteCount]() {
            try {
                json resp = JsonRpcClient::call("127.0.0.1", peerPort,
                                                "RequestVote", params);

                if (resp.contains("voteGranted") && resp.contains("term")) {
                    std::lock_guard<std::mutex> lock(mtx);

                    int respTerm = resp["term"];
                    bool vote = resp["voteGranted"];

                    // If we see a higher term, step down
                    if (respTerm > currentTerm) {
                        becomeFollower(respTerm);
                        return;
                    }

                    // Only count votes for the current election
                    if (role != Role::CANDIDATE || currentTerm != term) return;

                    if (vote) {
                        int votes = ++(*voteCount);
                        logger.info("Received vote from " + peerId + " (" + 
                                  std::to_string(votes) + "/" + std::to_string(needed) + ")");
                        
                        if (votes >= needed && role == Role::CANDIDATE) {
                            becomeLeaderInternal();
                        }
                    }
                }
            } catch (...) {
                // Silently handle network errors
            }
        }).detach();
    }
}

void RaftNode::becomeCandidate() {
    std::lock_guard<std::mutex> lock(mtx);
    startElection();
}

void RaftNode::becomeLeaderInternal() {
    // Must be called WITH lock held
    if (role == Role::LEADER) return;
    role = Role::LEADER;

    logger.info("Transition -> LEADER (term " + std::to_string(currentTerm) + ")");

    electionTimer.cancel(); // Stop election timeout

    // Initialize leader state
    for (const auto &peerSpec : peerAddrs) {
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos) continue;
        std::string peerId = peerSpec.substr(0, colonPos);
        nextIndex[peerId] = log.size();
        matchIndex[peerId] = -1;
    }

    // Send immediate heartbeat, then start periodic timer
    sendHeartbeatsInternal();
}

void RaftNode::becomeLeader() {
    std::lock_guard<std::mutex> lock(mtx);
    becomeLeaderInternal();
}

json RaftNode::onRequestVote(const json &req) {
    std::lock_guard<std::mutex> lock(mtx);

    int term = req["term"];
    std::string candidate = req["candidateId"];
    int lastLogIndex = req["lastLogIndex"];
    int lastLogTerm = req["lastLogTerm"];

    json resp;
    resp["term"] = currentTerm;
    resp["voteGranted"] = false;

    // Reject if term is older
    if (term < currentTerm) {
        logger.info("Rejecting vote for " + candidate + " (stale term " + 
                   std::to_string(term) + " < " + std::to_string(currentTerm) + ")");
        return resp;
    }

    // Step down if we see a higher term
    if (term > currentTerm) {
        becomeFollower(term);
    }

    // Check if log is up-to-date
    int ourLastLogTerm = log.empty() ? 0 : log.back().term;
    int ourLastLogIndex = (int)log.size() - 1;
    
    bool logUpToDate =
        (lastLogTerm > ourLastLogTerm) ||
        (lastLogTerm == ourLastLogTerm && lastLogIndex >= ourLastLogIndex);

    // Grant vote if we haven't voted yet and candidate's log is up-to-date
    if ((votedFor == "" || votedFor == candidate) && logUpToDate) {
        votedFor = candidate;
        resp["voteGranted"] = true;
        resetElectionTimer();
        logger.info("Granting vote to " + candidate + " for term " + std::to_string(term));
    } else {
        logger.info("Denying vote to " + candidate + 
                   " (already voted for '" + votedFor + "' or log outdated)");
    }

    resp["term"] = currentTerm;
    return resp;
}

json RaftNode::onAppendEntries(const json &req) {
    std::lock_guard<std::mutex> lock(mtx);

    int term = req["term"];
    std::string leaderId = req["leaderId"];

    json resp;
    resp["term"] = currentTerm;
    resp["success"] = false;

    // Reject if term is older
    if (term < currentTerm) {
        logger.info("Rejecting AppendEntries from " + leaderId + " (stale term " + 
                   std::to_string(term) + " < " + std::to_string(currentTerm) + ")");
        return resp;
    }

    // Valid heartbeat from leader - step down and reset timer
    if (term >= currentTerm) {
        if (role == Role::CANDIDATE || role == Role::LEADER) {
            logger.info("Stepping down: received heartbeat from " + leaderId + 
                       " (term " + std::to_string(term) + ")");
        }
        becomeFollower(term);
    }

    resp["success"] = true;
    resp["term"] = currentTerm;
    return resp;
}

void RaftNode::sendHeartbeatsInternal() {
    // Must be called WITH lock held
    if (role != Role::LEADER) return;

    int term = currentTerm;
    std::string leaderId = id;

    logger.info("Sending heartbeats (term " + std::to_string(term) + ")");

    for (const auto &peerSpec : peerAddrs) {
        // Parse "nodeX:port" format
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos) continue;
        
        std::string peerId = peerSpec.substr(0, colonPos);
        int peerPort = std::stoi(peerSpec.substr(colonPos + 1));

        json params = {
            {"term", term},
            {"leaderId", leaderId}
        };

        // Spawn thread for non-blocking I/O
        std::thread([this, peerId, peerPort, params, term]() {
            try {
                json resp = JsonRpcClient::call("127.0.0.1", peerPort,
                                                "AppendEntries", params);
                
                if (resp.contains("term")) {
                    int respTerm = resp["term"];
                    if (respTerm > term) {
                        std::lock_guard<std::mutex> lock(mtx);
                        if (respTerm > currentTerm) {
                            logger.info("Stepping down: received higher term " + 
                                       std::to_string(respTerm) + " from " + peerId);
                            becomeFollower(respTerm);
                        }
                    }
                }
            } catch (...) {
                // Silently handle network errors
            }
        }).detach();
    }

    // Schedule next heartbeat - CRITICAL: Only schedule once
    heartbeatTimer.cancel(); // Cancel any previous timer
    heartbeatTimer.start(100, [this]() {
        std::lock_guard<std::mutex> lock(mtx);
        sendHeartbeatsInternal();
    });
}

void RaftNode::sendHeartbeats() {
    std::lock_guard<std::mutex> lock(mtx);
    sendHeartbeatsInternal();
}