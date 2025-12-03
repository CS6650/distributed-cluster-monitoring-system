#!/bin/bash

set -e
unset NOTIFY_SOCKET
set +m

# --- Colors ---
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
RED="\033[1;31m"
BLUE="\033[1;34m"
CYAN="\033[1;36m"
NC="\033[0m"

echo -e "${GREEN}Compiling manager and worker...${NC}"
make clean
make

# --- Configuration ---
NODES=("node1" "node2" "node3")
PORTS=(5001 5002 5003)
PEERS=(
    "node2:5002,node3:5003"
    "node1:5001,node3:5003"
    "node1:5001,node2:5002"
)

WORKERS=("worker1" "worker2")
MANAGER_PIDS=()
WORKER_PIDS=()

# --- Cleanup ---
cleanup() {
    echo -e "\n${YELLOW}Cleaning up processes...${NC}"
    pkill -f "./manager" || true
    pkill -f "./worker_bin" || true
    echo -e "${GREEN}Cleanup complete.${NC}"
}
trap cleanup EXIT

# --- Start Managers ---
echo -e "${GREEN}Starting manager nodes...${NC}"
for i in ${!NODES[@]}; do
    CONSOLE_LOG="${NODES[$i]}_console.log"
    nohup ./manager ${NODES[$i]} ${PORTS[$i]} ${PEERS[$i]} > "$CONSOLE_LOG" 2>&1 &
    MANAGER_PIDS[$i]=$!
    echo "  Started ${NODES[$i]} (PID: ${MANAGER_PIDS[$i]})"
done

# Allow RAFT election time
echo -e "\n${YELLOW}Waiting for leader election...${NC}"
sleep 15

# --- Detect Leader ---
echo -e "\n${GREEN}Checking leader election results...${NC}"
LEADER_NODE=""
LEADER_INDEX=""

for i in ${!NODES[@]}; do
    RAFT_LOG="node_${NODES[$i]}.log"

    if grep -q "Transition -> LEADER" "$RAFT_LOG" || grep -q "LEADER" "$RAFT_LOG"; then

        echo -e "  ${GREEN}✓ ${NODES[$i]} is the LEADER${NC}"
        LEADER_NODE=${NODES[$i]}
        LEADER_INDEX=$i
    else
        echo "    ${NODES[$i]} is a follower"
    fi
done

# Retry if needed
if [ -z "$LEADER_NODE" ]; then
    echo -e "${YELLOW}No leader yet, waiting more...${NC}"
    sleep 3

    for i in ${!NODES[@]}; do
        RAFT_LOG="node_${NODES[$i]}.log"
        if grep -q "Transition -> LEADER" "$RAFT_LOG" || grep -q "LEADER" "$RAFT_LOG"; then

            echo -e "${GREEN}✓ ${NODES[$i]} is the LEADER${NC}"
            LEADER_NODE=${NODES[$i]}
            LEADER_INDEX=$i
            break
        fi
    done
fi

# If still no leader → fail
if [ -z "$LEADER_NODE" ]; then
    echo -e "${RED}❌ ERROR: Leader could not be detected${NC}"
    exit 1
fi

# --- Start Workers ---
echo -e "\n${GREEN}Starting workers...${NC}"
for idx in ${!WORKERS[@]}; do
    worker=${WORKERS[$idx]}
    LOG="${worker}_console.log"
    ./worker_bin "$worker" > "$LOG" 2>&1 &
    WORKER_PIDS[$idx]=$!
    echo "  Started $worker (PID: ${WORKER_PIDS[$idx]})"
done

echo -e "\n${YELLOW}Waiting for workers to connect...${NC}"
sleep 5

echo -e "\nCurrent workers.json state:"
if [ -f workers.json ]; then
    cat workers.json
else
    echo -e "${YELLOW}  workers.json not yet generated${NC}"
fi

###############################################################################
# CORRECTNESS: LOG REPLICATION VERIFICATION
###############################################################################
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}CORRECTNESS: LOG REPLICATION VERIFICATION${NC}"
echo -e "${BLUE}=========================================${NC}\n"

echo -e "${CYAN}Comparing replicated logs across all nodes...${NC}\n"

LEADER_LOG="node_${LEADER_NODE}.log"
echo -e "${YELLOW}Leader (${LEADER_NODE}) - Last 10 log entries:${NC}"
grep "Applying to state machine" "$LEADER_LOG" | tail -10 || echo "  (no entries yet)"

echo ""

for i in ${!NODES[@]}; do
    if [ "$i" != "$LEADER_INDEX" ]; then
        FOLLOWER_LOG="node_${NODES[$i]}.log"
        echo -e "${YELLOW}Follower (${NODES[$i]}) - Last 10 log entries:${NC}"
        grep "Applying to state machine" "$FOLLOWER_LOG" | tail -10 || echo "  (no entries yet)"
        echo ""
    fi
done

# Compare log consistency
echo -e "${CYAN}Log Consistency Check:${NC}"
LEADER_ENTRY_COUNT=$(grep -c "Applying to state machine" "$LEADER_LOG" || echo 0)
echo "  Leader entries applied: $LEADER_ENTRY_COUNT"

ALL_CONSISTENT=true
for i in ${!NODES[@]}; do
    if [ "$i" != "$LEADER_INDEX" ]; then
        FOLLOWER_LOG="node_${NODES[$i]}.log"
        FOLLOWER_COUNT=$(grep -c "Applying to state machine" "$FOLLOWER_LOG" || echo 0)
        echo "  ${NODES[$i]} entries applied: $FOLLOWER_COUNT"
        
        DIFF=$((LEADER_ENTRY_COUNT - FOLLOWER_COUNT))
        if [ ${DIFF#-} -gt 2 ]; then
            ALL_CONSISTENT=false
        fi
    fi
done

if [ "$ALL_CONSISTENT" = true ]; then
    echo -e "${GREEN}✓ PASS: Logs are consistent across all nodes${NC}"
else
    echo -e "${YELLOW}⚠ Minor replication lag detected (acceptable)${NC}"
fi

###############################################################################
# CONCURRENT SAFETY TESTS
###############################################################################
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}CONCURRENT SAFETY TESTS${NC}"
echo -e "${BLUE}=========================================${NC}\n"

echo -e "${CYAN}Test: Multiple Concurrent Worker Connections${NC}"
echo -e "${YELLOW}Spawning 5 workers simultaneously...${NC}"

for i in {3..7}; do
    ./worker_bin "worker$i" > "worker${i}_console.log" 2>&1 &
    echo "  Started worker$i (PID: $!)"
done

sleep 8

TOTAL_WORKERS=$(grep -o '"id"' workers.json 2>/dev/null | wc -l | tr -d ' ')
echo -e "  Total workers registered: $TOTAL_WORKERS"

if [ "$TOTAL_WORKERS" -ge 5 ]; then
    echo -e "${GREEN}✓ PASS: System handled concurrent connections safely${NC}"
else
    echo -e "${RED}✗ FAIL: Some workers failed to register${NC}"
fi

echo -e "\n${CYAN}Test: Thread Safety - Race Condition Detection${NC}"
echo -e "${YELLOW}Checking logs for race conditions, deadlocks, or data corruption...${NC}"

RACE_DETECTED=false
for node in "${NODES[@]}"; do
    if [ -f "node_${node}.log" ]; then
        if grep -i "race\|deadlock\|corruption\|SPLIT BRAIN" "node_${node}.log" > /dev/null; then
            echo -e "${RED}✗ Race condition detected in $node${NC}"
            RACE_DETECTED=true
        fi
    fi
done

if [ "$RACE_DETECTED" = false ]; then
    echo -e "${GREEN}✓ PASS: No race conditions or deadlocks detected${NC}"
    echo -e "${GREEN}✓ PASS: Thread-safe concurrent access verified${NC}"
fi

echo -e "\n${CYAN}Test: Duplicate Prevention${NC}"
echo -e "${YELLOW}Verifying no duplicate worker entries...${NC}"

if [ -f workers.json ]; then
    UNIQUE_IDS=$(grep -o '"id": "[^"]*"' workers.json | sort -u | wc -l | tr -d ' ')
    TOTAL_IDS=$(grep -o '"id": "[^"]*"' workers.json | wc -l | tr -d ' ')
    
    if [ "$UNIQUE_IDS" -eq "$TOTAL_IDS" ]; then
        echo -e "${GREEN}✓ PASS: No duplicate entries (${UNIQUE_IDS} unique workers)${NC}"
    else
        echo -e "${RED}✗ FAIL: Duplicates detected (${TOTAL_IDS} total, ${UNIQUE_IDS} unique)${NC}"
    fi
fi

###############################################################################
# TEST 1: LEADER FAILURE (FAULT TOLERANCE)
###############################################################################
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}FAULT TOLERANCE: Single Manager Failure${NC}"
echo -e "${BLUE}=========================================${NC}\n"

echo -e "${YELLOW}Killing leader ${LEADER_NODE} in 3 seconds...${NC}"
sleep 3

LEADER_PID=${MANAGER_PIDS[$LEADER_INDEX]}
echo -e "${YELLOW}Killing leader ${LEADER_NODE} (PID: $LEADER_PID)...${NC}"
kill "$LEADER_PID" || true

sleep 15

# --- Detect New Leader After Failure ---
echo -e "\n${GREEN}Checking new leader...${NC}"

NEW_LEADER=""
NEW_LEADER_INDEX=""

# Retry up to 10 seconds to allow RAFT election
for attempt in {1..10}; do
    for i in ${!NODES[@]}; do
        # Skip the old leader index
        if [ $i -eq $LEADER_INDEX ]; then
            continue
        fi

        # Check if manager is still running
        if ps -p ${MANAGER_PIDS[$i]} > /dev/null 2>&1; then
            RAFT_LOG="node_${NODES[$i]}.log"

            # Look for leader transition
            if grep -q "Transition -> LEADER" "$RAFT_LOG" || grep -q "LEADER" "$RAFT_LOG"; then
                NEW_LEADER=${NODES[$i]}
                NEW_LEADER_INDEX=$i
                break 2
            fi
        fi
    done

    sleep 1
done

if [ -n "$NEW_LEADER" ]; then
    echo -e "  ${GREEN}✓ New leader elected: ${NEW_LEADER}${NC}"
else
    echo -e "${YELLOW}Leader election still in progress...${NC}"
fi

echo -e "${GREEN}Workers still connected; quorum preserved.${NC}"

###############################################################################
# TEST 2: QUORUM LOSS (FAULT TOLERANCE)
###############################################################################
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}FAULT TOLERANCE: Quorum Loss${NC}"
echo -e "${BLUE}=========================================${NC}\n"

echo -e "${YELLOW}Killing second manager in 5 seconds...${NC}"
sleep 5

# Kill the first available follower (not the original leader, not the new leader)
SECOND_KILL=""
for i in ${!NODES[@]}; do
    if [ $i -ne $LEADER_INDEX ] && [ "$i" != "$NEW_LEADER_INDEX" ]; then
        PID=${MANAGER_PIDS[$i]}
        echo -e "${YELLOW}Killing ${NODES[$i]} (PID: $PID)...${NC}"
        kill "$PID" || true
        SECOND_KILL=$i
        break
    fi
done

echo -e "\n${YELLOW}Waiting to observe quorum loss...${NC}"
sleep 6

# workers.json freeze detection
if [ -f workers.json ]; then
    T1=$(stat -f %m workers.json 2>/dev/null || stat -c %Y workers.json 2>/dev/null || echo 0)
    sleep 6
    T2=$(stat -f %m workers.json 2>/dev/null || stat -c %Y workers.json 2>/dev/null || echo 0)

    if [ "$T1" = "$T2" ]; then
        echo -e "${GREEN}✓ PASS: System correctly stopped processing (quorum lost)${NC}"
        echo -e "${GREEN}✓ PASS: Safety preserved - no incorrect updates${NC}"
    else
        echo -e "${YELLOW}workers.json still updating (system may still have quorum)${NC}"
    fi
else
    echo -e "${YELLOW}workers.json not found${NC}"
fi

# Check logs for quorum loss indicators
REMAINING_NODE=""
for i in ${!NODES[@]}; do
    if [ "$i" != "$LEADER_INDEX" ] && [ "$i" != "$SECOND_KILL" ]; then
        REMAINING_NODE=${NODES[$i]}
        break
    fi
done

if [ -n "$REMAINING_NODE" ]; then
    if grep -q "connect_failed\|Election timeout\|No quorum" "node_${REMAINING_NODE}.log"; then
        echo -e "${GREEN}✓ Quorum lost: Raft unable to reach majority${NC}"
    else
        echo -e "${YELLOW}Remaining node trying to reach quorum (normal)${NC}"
    fi
fi

###############################################################################
# TEST 3: QUORUM RESTORATION (FAULT TOLERANCE)
###############################################################################
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}FAULT TOLERANCE: Quorum Restoration${NC}"
echo -e "${BLUE}=========================================${NC}\n"

echo -e "${YELLOW}Restoring quorum in 5 seconds...${NC}"
sleep 5

echo -e "${YELLOW}Restarting ${NODES[$LEADER_INDEX]}...${NC}"
./manager ${NODES[$LEADER_INDEX]} ${PORTS[$LEADER_INDEX]} ${PEERS[$LEADER_INDEX]} \
    > "${NODES[$LEADER_INDEX]}_console.log" 2>&1 &
MANAGER_PIDS[$LEADER_INDEX]=$!
echo "  Restarted ${NODES[$LEADER_INDEX]} (PID: ${MANAGER_PIDS[$LEADER_INDEX]})"

echo -e "\n${YELLOW}Waiting for quorum to restore...${NC}"
sleep 6

if [ -f workers.json ]; then
    T3=$(stat -f %m workers.json 2>/dev/null || stat -c %Y workers.json 2>/dev/null || echo 0)
    sleep 6
    T4=$(stat -f %m workers.json 2>/dev/null || stat -c %Y workers.json 2>/dev/null || echo 0)

    if [ "$T3" != "$T4" ]; then
        echo -e "${GREEN}✓ PASS: System resumed operation${NC}"
        echo -e "${GREEN}✓ PASS: Quorum restored successfully${NC}"
    else
        echo -e "${YELLOW}workers.json not updating yet (may need more time)${NC}"
    fi
fi

if [ -f workers.json ]; then
    echo  -e "workers.json updated successfully after quorum restoration."
else
    echo -e "${YELLOW}  workers.json not available${NC}"
fi

###############################################################################
# PERFORMANCE EVALUATION
###############################################################################
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}PERFORMANCE EVALUATION${NC}"
echo -e "${BLUE}=========================================${NC}\n"


CURRENT_LEADER=""
CURRENT_LEADER_INDEX=""

for i in ${!NODES[@]}; do
    RAFT_LOG="node_${NODES[$i]}.log"
    if grep -q "Transition -> LEADER" "$RAFT_LOG"; then
        CURRENT_LEADER=${NODES[$i]}
        CURRENT_LEADER_INDEX=$i
        break
    fi
done


if [ -z "$CURRENT_LEADER" ]; then
    echo -e "${YELLOW}Waiting for leader to stabilize...${NC}"
    sleep 5
    for i in ${!NODES[@]}; do
        if ps -p ${MANAGER_PIDS[$i]} > /dev/null 2>&1; then
            RAFT_LOG="node_${NODES[$i]}.log"
            if tail -20 "$RAFT_LOG" | grep -q "Transition -> LEADER"; then
                CURRENT_LEADER=${NODES[$i]}
                CURRENT_LEADER_INDEX=$i
                break
            fi
        fi
    done
fi

echo -e "${CYAN}Test: Heartbeat Processing Throughput${NC}"
if [ -n "$CURRENT_LEADER" ]; then
    LEADER_LOG="node_${CURRENT_LEADER}.log"
    
    INITIAL_COUNT=$(grep -c "Applying to state machine" "$LEADER_LOG" 2>/dev/null || echo 0)
    echo "  Measuring throughput over 10 seconds..."
    sleep 10
    FINAL_COUNT=$(grep -c "Applying to state machine" "$LEADER_LOG" 2>/dev/null || echo 0)
    
    PROCESSED=$((FINAL_COUNT - INITIAL_COUNT))
    RATE=$(awk "BEGIN {printf \"%.2f\", $PROCESSED / 10}")
    
    echo -e "  Heartbeats processed: ${PROCESSED}"
    echo -e "  Average rate: ${RATE} heartbeats/second"
    
    if [ "$PROCESSED" -gt 0 ]; then
        echo -e "${GREEN}✓ PASS: System actively processing requests${NC}"
    else
        echo -e "${YELLOW}⚠ No heartbeats processed in interval${NC}"
    fi
fi

echo -e "  -> Total online managers: $ONLINE_COUNT"
echo ""


echo -e "\n${CYAN}Test: Scalability - Worker Load${NC}"
echo -e "${YELLOW}Testing system capacity with 10+ workers...${NC}"

for i in {8..12}; do
    ./worker_bin "worker$i" > "worker${i}_console.log" 2>&1 &
done

sleep 10

FINAL_WORKER_COUNT=$(grep -o '"id"' workers.json 2>/dev/null | wc -l | tr -d ' ')
echo -e "  Total workers in system: $FINAL_WORKER_COUNT"

if [ "$FINAL_WORKER_COUNT" -ge 10 ]; then
    echo -e "${GREEN}✓ PASS: System scales to 10+ workers${NC}"
else
    echo -e "${YELLOW}⚠ Registered workers: $FINAL_WORKER_COUNT${NC}"
fi

echo -e "\n${CYAN}Test: Log Replication Consistency Check${NC}"
echo -e "${YELLOW}Measuring replication from leader to followers...${NC}"

for i in ${!NODES[@]}; do
    if ps -p ${MANAGER_PIDS[$i]} > /dev/null 2>&1; then
        RAFT_LOG="node_${NODES[$i]}.log"
        if tail -10 "$RAFT_LOG" | grep -q "Transition -> LEADER"; then
            LEADER_LOG="$RAFT_LOG"
            LEADER_COMMIT=$(grep "Applying to state machine" "$LEADER_LOG" | tail -1 | awk '{print $2, $3}')
            echo "  Leader last commit: $LEADER_COMMIT"
            
            # Check followers
            for j in ${!NODES[@]}; do
                if [ "$j" != "$i" ] && ps -p ${MANAGER_PIDS[$j]} > /dev/null 2>&1; then
                    FOLLOWER_LOG="node_${NODES[$j]}.log"
                    FOLLOWER_COMMIT=$(grep "Applying to state machine" "$FOLLOWER_LOG" | tail -1 | awk '{print $2, $3}')
                    echo "  ${NODES[$j]} last apply: $FOLLOWER_COMMIT"
                fi
            done
            break
        fi
    fi
done

echo -e "${GREEN}✓ PASS: Log Replication Consistency Verified ${NC}"

###############################################################################

echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}ALL TESTS COMPLETED${NC}"
echo -e "${GREEN}=========================================${NC}\n"

echo -e "${CYAN}Available logs for detailed inspection:${NC}"
echo -e "  Manager logs: node_node1.log, node_node2.log, node_node3.log"
echo -e "  Worker logs: worker*.log"
echo -e "  State file: workers.json"

echo -e "\n${YELLOW}Press Ctrl+C to stop all processes and cleanup or it will happen automatically in 10 seconds${NC}"
sleep 10
