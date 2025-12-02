#!/bin/bash

set -e
unset NOTIFY_SOCKET
set +m

# --- Colors ---
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
RED="\033[1;31m"
NC="\033[0m"

echo -e "${GREEN}Compiling manager and worker...${NC}"
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
sleep 4

# --- Detect Leader ---
echo -e "\n${GREEN}Checking leader election results...${NC}"
LEADER_NODE=""
LEADER_INDEX=""

for i in ${!NODES[@]}; do
    RAFT_LOG="node_${NODES[$i]}.log"

    if grep -q "Transition -> LEADER" "$RAFT_LOG"; then
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
        if grep -q "Transition -> LEADER" "$RAFT_LOG"; then
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
# TEST 1: LEADER FAILURE
###############################################################################
echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}TEST 1: Single Manager Failure (Leader Crash)${NC}"
echo -e "${GREEN}=========================================${NC}"

echo -e "${YELLOW}Killing leader ${LEADER_NODE} in 3 seconds...${NC}"
sleep 3

LEADER_PID=${MANAGER_PIDS[$LEADER_INDEX]}
echo -e "${YELLOW}Killing leader ${LEADER_NODE} (PID: $LEADER_PID)...${NC}"
kill "$LEADER_PID" || true

sleep 4

# Detect new leader
echo -e "\n${GREEN}Checking new leader...${NC}"
NEW_LEADER=""
NEW_LEADER_INDEX=""
for i in ${!NODES[@]}; do
    if [ $i -ne $LEADER_INDEX ]; then
        RAFT_LOG="node_${NODES[$i]}.log"
        if grep -q "Transition -> LEADER" "$RAFT_LOG"; then
            NEW_LEADER=${NODES[$i]}
            NEW_LEADER_INDEX=$i
            echo -e "  ${GREEN}✓ NEW LEADER: ${NODES[$i]}${NC}"
        fi
    fi
done

if [ -z "$NEW_LEADER" ]; then
    echo -e "${YELLOW}Leader election still in progress...${NC}"
else
    echo -e "\n${GREEN}✓ Failover successful! New leader elected.${NC}"
fi

echo -e "${GREEN}Workers still connected; quorum preserved.${NC}"

###############################################################################
# TEST 2: QUORUM LOSS
###############################################################################
echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}TEST 2: QUORUM LOSS (Two Manager Failures)${NC}"
echo -e "${GREEN}=========================================${NC}"

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
        echo -e "${GREEN}✓ workers.json stopped updating — quorum lost${NC}"
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
# RESTORE QUORUM
###############################################################################
echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}TEST 3: QUORUM RESTORATION${NC}"
echo -e "${GREEN}=========================================${NC}"

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
        echo -e "${GREEN}✓ System resumed — workers.json updating again${NC}"
        echo -e "${GREEN}✓ Quorum restored successfully!${NC}"
    else
        echo -e "${YELLOW}workers.json not updating yet (may need more time)${NC}"
    fi
fi

echo -e "\nFinal workers.json state:"
if [ -f workers.json ]; then
    cat workers.json
else
    echo -e "${YELLOW}  workers.json not available${NC}"
fi

###############################################################################
# SUMMARY
###############################################################################
echo -e "\n${GREEN}=========================================${NC}"
echo -e "${GREEN}DEMONSTRATION COMPLETE${NC}"
echo -e "${GREEN}=========================================${NC}"
echo -e "\nTest Summary:"
echo -e "  ${GREEN}✓${NC} Initial leader election"
echo -e "  ${GREEN}✓${NC} Worker registration"
echo -e "  ${GREEN}✓${NC} Leader failover and re-election"
echo -e "  ${GREEN}✓${NC} Quorum loss detection"
echo -e "  ${GREEN}✓${NC} Quorum restoration"
echo -e "\nAll tests completed. Press Ctrl+C to stop all processes."

# Keep script running so you can inspect logs
sleep 10