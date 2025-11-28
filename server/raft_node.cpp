#include "raft_node.hpp"
#include "raft_rpc.hpp"
#include <chrono>
#include <random>
#include <algorithm>

static int randomTimeout() {
    // Random election timeout between 300–500 ms
    static std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(3000, 5000);
    return dist(rng);
}

RaftNode::RaftNode(std::string nodeId,
                   int rpcPort,
                   std::vector<std::string> peers)
    : rpcPool(8),
      id(nodeId),
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
    
    // FIX: The election timer should trigger an "election timeout" handler,
    // not directly call startElection(). This handler checks the current role
    // and decides what to do.
    electionTimer.start(t, [this]() {
        std::lock_guard<std::mutex> lock(mtx);
        
        // FIX: Only start an election if we're a FOLLOWER or CANDIDATE
        // If FOLLOWER -> become CANDIDATE and start election
        // If CANDIDATE -> election timed out, start new election
        // If LEADER -> do nothing (shouldn't happen, but be defensive)
        
        if (role == Role::FOLLOWER || role == Role::CANDIDATE) {
            logger.info("Election timeout triggered (role=" + 
                       std::to_string((int)role.load()) + ")");
            startElection();
        }
    });
}

void RaftNode::becomeFollower(int term) {
    // logger.debug("becomeFollower called by" + getId());
    // Must be called WITH lock held
    role = Role::FOLLOWER;
    currentTerm = term;
    votedFor = "";
    heartbeatTimer.cancel(); // Stop sending heartbeats if we were leader
    logger.info("Transition -> FOLLOWER (term " + std::to_string(term) + ")");
    
    // FIX: Reset election timer when becoming follower
    // resetElectionTimer();
}

void RaftNode::startElection() {
    // logger.debug("startElection called by " + getId());
    // Called WITH lock held

    // electionTimer.cancel();  // stop previous timers

    role = Role::CANDIDATE;
    currentTerm++;
    votedFor = id;

    int term = currentTerm;
    logger.info("Transition -> CANDIDATE (term " + std::to_string(term) + ")");

    resetElectionTimer();  // start timer for this election

    auto voteCount = std::make_shared<std::atomic<int>>(1);
    int needed = (peerAddrs.size() + 1) / 2 + 1;

    if (*voteCount >= needed) {
        becomeLeaderInternal();
        return;
    }

    int lastLogIdx = (int)log.size() - 1;
    int lastLogTerm = log.empty() ? 0 : log.back().term;

    auto electionWon = std::make_shared<std::atomic<bool>>(false);

    // Send RequestVote RPCs using thread pool
    for (const auto &peerSpec : peerAddrs) {
        size_t colon = peerSpec.find(':');
        if (colon == std::string::npos) continue;

        std::string peerId = peerSpec.substr(0, colon);
        int peerPort = std::stoi(peerSpec.substr(colon + 1));

        json params = {
            {"term", term},
            {"candidateId", id},
            {"lastLogIndex", lastLogIdx},
            {"lastLogTerm", lastLogTerm}
        };

        rpcPool.enqueue([this, peerId, peerPort, params, 
                         term, needed, voteCount, electionWon]() {
            try {
                logger.info("Sending RequestVote to " + peerId);

                json resp = JsonRpcClient::call("127.0.0.1",
                                                peerPort,
                                                "RequestVote",
                                                params);

                logger.info("RequestVote response from " + peerId + + ":\n" + resp.dump(4));

                if (!resp.contains("term") || !resp.contains("voteGranted"))
                    return;

                int respTerm = resp["term"];
                bool granted = resp["voteGranted"];

                std::lock_guard<std::mutex> lock(mtx);

                // Step down if higher term seen
                if (respTerm > currentTerm) {
                    becomeFollower(respTerm);
                    return;
                }

                // Outdated election
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
            }
        });
    }
}

void RaftNode::becomeLeaderInternal() {
    // logger.debug("becomeLeaderInternal called by" + getId());
    // Must be called WITH lock held
    if (role == Role::LEADER) return;
    
    // FIX: Cancel election timer when becoming leader
    electionTimer.cancel();
    
    role = Role::LEADER;

    logger.info("Transition -> LEADER (term " + std::to_string(currentTerm) + ")");

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

json RaftNode::onRequestVote(const json &req) {
    // logger.debug("onRequestVote called by" + getId());
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
        resp["term"] = currentTerm;
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
        
        // FIX: Reset election timer when granting a vote
        // This prevents us from starting our own election immediately
        resetElectionTimer();
        
        logger.info("Granting vote to " + candidate + " for term " + std::to_string(term));
    } else {
        logger.info("Denying vote to " + candidate + 
                   " (already voted for '" + votedFor + "' or log outdated)");
    }
    return resp;
}

json RaftNode::onAppendEntries(const json &req) {
    // logger.debug("onAppendEntries called by" + getId());
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
    if (term > currentTerm) {
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
    // logger.debug("sendHeartbeatsInternal called by" + getId());
    // Must be called WITH lock held
    if (role != Role::LEADER) return;

    int term = currentTerm;
    std::string leaderId = id;

    logger.info("Sending heartbeats (term " + std::to_string(term) + ")");

    for (const auto &peerSpec : peerAddrs) {
        
        // Parse "nodeX:port" format
        size_t colonPos = peerSpec.find(':');
        if (colonPos == std::string::npos) {
            continue;
        }
        std::string peerId = peerSpec.substr(0, colonPos);
        int peerPort = std::stoi(peerSpec.substr(colonPos + 1));

        json params = {
            {"term", term},
            {"leaderId", leaderId}
        };

        rpcPool.enqueue([this, peerId, peerPort, params, term]() {
            try {
                json resp = JsonRpcClient::call("127.0.0.1", peerPort,
                                                "AppendEntries", params);
                logger.info("SUCCESS: Heartbeat ACK from " + peerId);                                
                
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
            } catch (const std::exception& e) {
                // Silently handle network errors
                logger.info("FAILED: Heartbeat to " + peerId + " - " + 
                       std::string(e.what()));
            }
        });
    }

    // Schedule next heartbeat - CRITICAL: Cancel previous timer first
    heartbeatTimer.cancel();
    heartbeatTimer.start(50, [this]() {
        std::lock_guard<std::mutex> lock(mtx);
        sendHeartbeatsInternal();
    });
}