#!/bin/bash

# Define the process names for Namenode and Datanode
NAMENODE_PROCESS="namenode"
DATANODE_PROCESS="datanode"

# Define the test file directory for DataNodes
DATANODE_DIR="datanode_data"

# Define the logs directory
LOG_DIR="logs"

# Function to gracefully terminate processes
terminate_processes() {
    local process_name=$1

    echo "Terminating all processes named $process_name..."
    pkill -SIGTERM -f "$process_name"

    # Wait for the processes to terminate
    sleep 2

    # Check if any processes are still running and force kill them
    if pgrep -f "$process_name" > /dev/null; then
        echo "Forcing termination of remaining $process_name processes..."
        pkill -SIGKILL -f "$process_name"
    else
        echo "All $process_name processes terminated."
    fi
}

# Terminate Namenode and Datanode processes
terminate_processes "$NAMENODE_PROCESS"
terminate_processes "$DATANODE_PROCESS"

echo "All services stopped."

# Delete the DataNode test file directory
rm -rf "$DATANODE_DIR"
echo "Deleted DataNode test file directory: $DATANODE_DIR"

# Delete the logs directory
rm -rf "$LOG_DIR"
echo "Deleted logs directory: $LOG_DIR"

echo "Termination complete."