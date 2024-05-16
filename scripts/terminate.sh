#!/bin/bash

# Define the process names for Namenode and Datanode
NAMENODE_PROCESS="namenode"
DATANODE_PROCESS="datanode"

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
