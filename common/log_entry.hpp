#ifndef LOG_ENTRY_HPP
#define LOG_ENTRY_HPP

#include <string>

/**
 * LogEntry - Represents a single entry in the RAFT log.
 *
 * Each entry contains:
 * - term: The term when the entry was received by the leader
 * - command: The state machine command to apply
 */
struct LogEntry
{
    int term;
    std::string command;
};

#endif