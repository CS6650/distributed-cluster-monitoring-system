#include "raft_node.hpp"
#include "raft_rpc.hpp"
#include <chrono>
#include <random>

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
    RaftRpc::init(port, this); // start RPC server
    logger.info("Node starting as follower.");
}

void RaftNode::start() {

    // Register RPC methods
    RaftRpc::init(port, this);

    logger.info("Node starting as follower.");
    becomeFollower(currentTerm);
}

void RaftNode::resetElectionTimer() {
    int t = randomTimeout();
    electionTimer.cancel();
    electionTimer.start(t, [this]() {
        if (role == Role::FOLLOWER || role == Role::CANDIDATE)
            becomeCandidate();
    });
}

void RaftNode::becomeFollower(int term) {
    std::lock_guard<std::mutex> lock(mtx);
    role = Role::FOLLOWER;
    currentTerm = term;
    votedFor = "";
    logger.info("Transition -> FOLLOWER (term " + std::to_string(term) + ")");
    resetElectionTimer();
}

void RaftNode::becomeCandidate() {
    std::lock_guard<std::mutex> lock(mtx);

    role = Role::CANDIDATE;
    currentTerm++;
    votedFor = id;

    logger.info("Transition -> CANDIDATE (term " + std::to_string(currentTerm) + ")");

    resetElectionTimer();

    // Vote for self
    int votes = 1;
    int needed = (peerAddrs.size() + 1) / 2 + 1;

    // Send RequestVote RPCs
    for (auto &p : peerAddrs) {
        json params = {
            {"term", currentTerm},
            {"candidateId", id},
            {"lastLogIndex", (int)log.size() - 1},
            {"lastLogTerm", log.empty() ? 0 : log.back().term}
        };

        std::thread([&, p, params]() {
            json resp = JsonRpcClient::call("127.0.0.1", RaftRpc::portOf(p),
                                            "RequestVote", params);

            if (resp.contains("voteGranted") &&
                resp.contains("term"))
            {
                std::lock_guard<std::mutex> lock(mtx);

                int respTerm = resp["term"];
                bool vote = resp["voteGranted"];

                if (respTerm > currentTerm) {
                    becomeFollower(respTerm);
                    return;
                }

                if (role != Role::CANDIDATE) return;

                if (vote) {
                    votes++;
                    if (votes >= needed) {
                        becomeLeader();
                    }
                }
            }
        }).detach();
    }
}

void RaftNode::becomeLeader() {
    std::lock_guard<std::mutex> lock(mtx);

    if (role == Role::LEADER) return;
    role = Role::LEADER;

    logger.info("Transition -> LEADER (term " + std::to_string(currentTerm) + ")");

    // Initialize leader state
    for (auto &p : peerAddrs) {
        nextIndex[p] = log.size();
        matchIndex[p] = -1;
    }

    // Start regular heartbeats
    heartbeatTimer.cancel();
    heartbeatTimer.start(100, [this]() { sendHeartbeats(); });
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

    if (term < currentTerm) return resp;

    if (term > currentTerm) {
        becomeFollower(term);
    }

    bool logUpToDate =
        (lastLogTerm > (log.empty() ? 0 : log.back().term)) ||
        (lastLogTerm == (log.empty() ? 0 : log.back().term) &&
         lastLogIndex >= (int)log.size() - 1);

    if ((votedFor == "" || votedFor == candidate) && logUpToDate) {
        votedFor = candidate;
        resp["voteGranted"] = true;
        resetElectionTimer();
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

    if (term < currentTerm) return resp;

    becomeFollower(term);
    resetElectionTimer();

    // For now: heartbeats only, log replication in Drop 3
    resp["success"] = true;
    return resp;
}

void RaftNode::sendHeartbeats() {
    if (role != Role::LEADER) return;

    json params = {
        {"term", currentTerm},
        {"leaderId", id}
    };

    for (auto &p : peerAddrs) {
        std::thread([&, p, params]() {
            JsonRpcClient::call("127.0.0.1", RaftRpc::portOf(p),
                                "AppendEntries", params);
        }).detach();
    }

    heartbeatTimer.start(100, [this]() { sendHeartbeats(); });
}
