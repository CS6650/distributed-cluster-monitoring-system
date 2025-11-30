#!/bin/bash

# Exit on any error
set -e

# Compile everything
echo "Compiling manager and worker..."
make

# Nodes configuration
NODES=("node1" "node2" "node3")
PORTS=(5001 5002 5003)
PEERS=("node2:5002,node3:5003" "node1:5001,node3:5003" "node1:5001,node2:5002")

# Workers
WORKERS=("worker1" "worker2")

# Start Managers
echo "Starting manager nodes..."
for i in ${!NODES[@]}; do
    # gnome-terminal -- bash -c "./manager ${NODES[$i]} ${PORTS[$i]} ${PEERS[$i]}; exec bash"
    osascript -e "tell application \"Terminal\" to do script \"cd $(pwd) && ./manager ${NODES[$i]} ${PORTS[$i]} ${PEERS[$i]}\""
done

# Give managers some time to start
sleep 2

# Start Workers
echo "Starting workers..."
for worker in "${WORKERS[@]}"; do
    # gnome-terminal -- bash -c "./worker $worker; exec bash"
    osascript -e "tell application \"Terminal\" to do script \"cd $(pwd) && ./worker_bin $worker\""
done

echo "All managers and workers started."
echo "Check node1.log, node2.log, node3.log for leader election and worker heartbeats."
