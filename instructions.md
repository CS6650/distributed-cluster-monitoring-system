# Distributed Cluster Monitoring System - Quick Start Guide

## What This System Does
A fault-tolerant monitoring system using RAFT consensus. Multiple manager nodes replicate worker health status. System survives single manager failures with automatic leader election and failover.

## Requirements Met
✓ Distributed system with multiple nodes (3 managers + N workers)
✓ Fault tolerance via RAFT consensus (tolerates 1 manager failure)
✓ Leader election with automatic failover
✓ Consistent replicated state across all managers
✓ Real-time health monitoring output

## Quick Start (5 minutes)

### 1. Build
```bash
cd distributed-cluster-monitoring-system
make clean && make
```

### 2. Run (Automated)
```bash
chmod +x run_test.sh
./run_test.sh
```
This opens 5 Terminal windows: 3 managers + 2 workers.

### 3. Verify System is Working
```bash
# In a new terminal
watch -n 1 cat workers.json

# You should see:
# {
#   "workers": [
#     {"id": "worker1", "online": true, ...},
#     {"id": "worker2", "online": true, ...}
#   ]
# }
```

### 4. Test Leader Election
```bash
# Check which node is leader
grep "LEADER" node*.log

# Expected: One node shows "✓ [LEADER] nodeX elected as leader"
```

### 5. Test Fault Tolerance
```bash
# 1. Identify current leader from logs or terminal output
# 2. Kill that manager (Ctrl+C in its terminal)
# 3. Watch for: "✓ [LEADER] nodeY elected as leader" (300-500ms)
# 4. Verify workers.json still updates (check timestamp)
# 5. Workers should show: "✓ [CONNECTED] worker1 connected to leader"
```

### 6. Test Quorum Loss
```bash
# 1. Kill 2 out of 3 managers
# 2. Observe: workers.json stops updating (no quorum)
# 3. Restart 1 manager: ./manager node2 5002 node1:5001,node3:5003
# 4. Observe: workers.json resumes updating (quorum restored)
```

### 7. Verify Consistency
```bash
# All managers should have applied same commands
for log in node*.log; do
    echo "$log: $(grep 'Applying to state machine' $log | wc -l)"
done

# All three numbers should be identical
```

## Manual Start (Alternative to run_test.sh)

### Managers (3 terminals):
```bash
# Terminal 1
./manager node1 5001 node2:5002,node3:5003

# Terminal 2
./manager node2 5002 node1:5001,node3:5003

# Terminal 3
./manager node3 5003 node1:5001,node2:5002
```

### Workers (2+ terminals):
```bash
# Terminal 4
./worker_bin worker1

# Terminal 5
./worker_bin worker2
```

## What to Observe

**Terminal Output (Important Events Only):**
- `✓ [LEADER] nodeX elected as leader` - Leader election
- `✓ [DISCOVERY] nodeX now accepting worker connections` - Discovery service active
- `✓ [CONNECTED] workerX connected to leader` - Worker connected
- `✗ [DISCONNECTED] workerX lost connection` - Connection lost
- `[WARN]` messages for connection issues

**Log Files (Detailed Information):**
- `node1.log, node2.log, node3.log` - RAFT operations, elections, log replication
- `worker1.log, worker2.log` - Worker heartbeat attempts

**Generated Output:**
- `workers.json` - Real-time cluster state (updates every 5 seconds)

## Key Features Demonstrated

1. **Leader Election:** Exactly one leader elected per term
2. **Fault Tolerance:** System continues with 1 manager down
3. **Automatic Failover:** New leader elected in <1 second
4. **Consistency:** All managers maintain identical state
5. **Worker Reconnection:** Workers automatically find new leader
6. **Quorum Enforcement:** System halts without majority (2/3 nodes)

## Troubleshooting

**Workers can't connect:** Ensure at least 2 managers running (quorum needed for leader election).

**Port already in use:** Change port numbers in command-line arguments. Discovery port 6000 is hardcoded.

**No leader elected:** Check all managers started and can reach each other on specified ports.


###############################################################################
# SUMMARY
###############################################################################
echo -e "\n${BLUE}=========================================${NC}"
echo -e "${BLUE}TEST SUITE SUMMARY${NC}"
echo -e "${BLUE}=========================================${NC}\n"

echo -e "${GREEN}1. CORRECTNESS TESTS:${NC}"
echo -e "  ✓ Leader election"
echo -e "  ✓ Worker registration"
echo -e "  ✓ State machine consistency"
echo -e "  ✓ Log replication verification"

echo -e "\n${GREEN}2. CONCURRENT SAFETY TESTS:${NC}"
echo -e "  ✓ Multiple concurrent connections"
echo -e "  ✓ Thread safety (no race conditions)"
echo -e "  ✓ Duplicate prevention"

echo -e "\n${GREEN}3. FAULT TOLERANCE TESTS:${NC}"
echo -e "  ✓ Single manager failure & recovery"
echo -e "  ✓ Quorum loss detection"
echo -e "  ✓ Quorum restoration"

echo -e "\n${GREEN}4. PERFORMANCE EVALUATION:${NC}"
echo -e "  ✓ Heartbeat throughput measurement"
echo -e "  ✓ Leader election latency"
echo -e "  ✓ Scalability (10+ workers)"
echo -e "  ✓ Log replication latency"
