#!/bin/bash

NAMENODE_CONFIG="config/namenode_endpoint.txt"
DATANODE_CONFIG="config/datanode_endpoints.txt"
DATANODE_DIR="datanode_data"
LOG_DIR="logs"

# Ensure configuration files exist
if [[ ! -f "$NAMENODE_CONFIG" ]]; then
  echo "Namenode configuration file not found: $NAMENODE_CONFIG"
  exit 1
fi

if [[ ! -f "$DATANODE_CONFIG" ]]; then
  echo "Datanode configuration file not found: $DATANODE_CONFIG"
  exit 1
fi

# Create datanodes subfolder
mkdir -p "$DATANODE_DIR"

# Create logs subfolder
mkdir -p "$LOG_DIR"

# Start Namenode
NAMENODE_ENDPOINT=$(cat "$NAMENODE_CONFIG")
./namenode/namenode.exe "$NAMENODE_ENDPOINT" "./$LOG_DIR/namenode.log" &

# Sleep for 1 second to allow Namenode to start
# sleep 1

# Start Datanodes
ID=1
while IFS= read -r DATANODE_ENDPOINT; do
  DATANODE_SUBDIR="$DATANODE_DIR/$ID"
  mkdir -p "$DATANODE_SUBDIR"
  pushd "$DATANODE_SUBDIR" > /dev/null

  # Create test files with blockID 0 and version 0
  echo "This is a test file for datanode with ID $ID" > "testfile{$ID}_0_0.txt"

  # Start Datanode
  ../../datanode/datanode.exe "$NAMENODE_ENDPOINT" "$DATANODE_ENDPOINT" "../../$LOG_DIR/datanode_$ID.log" &

  popd > /dev/null
  ID=$((ID + 1))
done < "$DATANODE_CONFIG"
