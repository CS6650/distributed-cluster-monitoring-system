#ifndef PERSISTENT_STATE_HPP
#define PERSISTENT_STATE_HPP

#include <string>
#include <vector>
#include <fstream>
#include <mutex>
#include "../common/json.hpp"
#include "../common/log_entry.hpp"

using json = nlohmann::json;

class PersistentState
{
public:
    PersistentState(const std::string &nodeId)
        : stateFile("raft_state_" + nodeId + ".json")
    {
        load();
    }

    // Update and persist currentTerm
    void setCurrentTerm(int term)
    {
        std::lock_guard<std::mutex> lock(mtx);
        currentTerm = term;
        persist();
    }

    int getCurrentTerm() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return currentTerm;
    }

    // Update and persist votedFor
    void setVotedFor(const std::string &candidateId)
    {
        std::lock_guard<std::mutex> lock(mtx);
        votedFor = candidateId;
        persist();
    }

    std::string getVotedFor() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return votedFor;
    }

    // Log operations
    void appendLog(const LogEntry &entry)
    {
        std::lock_guard<std::mutex> lock(mtx);
        log.push_back(entry);
        persist();
    }

    void truncateLog(int fromIndex)
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (fromIndex >= 0 && fromIndex < (int)log.size())
        {
            log.erase(log.begin() + fromIndex, log.end());
            persist();
        }
    }

    std::vector<LogEntry> getLog() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return log;
    }

    int getLogSize() const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return log.size();
    }

    LogEntry getLogEntry(int index) const
    {
        std::lock_guard<std::mutex> lock(mtx);
        return log[index];
    }

private:
    std::string stateFile;
    mutable std::mutex mtx;

    // The three pieces of persistent state
    int currentTerm = 0;
    std::string votedFor = "";
    std::vector<LogEntry> log;

    void persist()
    {
        // Called with lock held
        json state;
        state["currentTerm"] = currentTerm;
        state["votedFor"] = votedFor;

        json logJson = json::array();
        for (const auto &entry : log)
        {
            logJson.push_back({{"term", entry.term},
                               {"command", entry.command}});
        }
        state["log"] = logJson;

        // Write to temporary file first, then rename (atomic on POSIX)
        std::string tempFile = stateFile + ".tmp";
        std::ofstream ofs(tempFile);
        if (!ofs)
        {
            throw std::runtime_error("Failed to open state file for writing");
        }

        ofs << state.dump(2);
        ofs.flush();
        ofs.close();

        // Atomic rename
        if (std::rename(tempFile.c_str(), stateFile.c_str()) != 0)
        {
            throw std::runtime_error("Failed to rename state file");
        }
    }

    void load()
    {
        std::lock_guard<std::mutex> lock(mtx);

        std::ifstream ifs(stateFile);
        if (!ifs)
        {
            // No state file exists - fresh start
            currentTerm = 0;
            votedFor = "";
            log.clear();
            return;
        }

        try
        {
            json state = json::parse(ifs);

            currentTerm = state.value("currentTerm", 0);
            votedFor = state.value("votedFor", "");

            log.clear();
            if (state.contains("log") && state["log"].is_array())
            {
                for (const auto &entry : state["log"])
                {
                    log.push_back({entry["term"],
                                   entry["command"]});
                }
            }
        }
        catch (const std::exception &e)
        {
            // Corrupted state file - start fresh
            currentTerm = 0;
            votedFor = "";
            log.clear();
        }
    }
};

#endif