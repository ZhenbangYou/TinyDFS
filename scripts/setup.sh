#!/bin/bash

NAMENODE_CONFIG="config/namenode_endpoint.txt"
DATANODE_CONFIG="config/datanode_endpoints.txt"
DATANODE_DIR="datanode_data"
LOG_DIR="logs"
PID_DIR="pids"

# Ensure configuration files exist
if [[ ! -f "$NAMENODE_CONFIG" ]]; then
  echo "Namenode configuration file not found: $NAMENODE_CONFIG"
  exit 1
fi

if [[ ! -f "$DATANODE_CONFIG" ]]; then
  echo "Datanode configuration file not found: $DATANODE_CONFIG"
  exit 1
fi

# Create required directories
mkdir -p "$DATANODE_DIR" "$LOG_DIR" "$PID_DIR"

# Write Datanode test files
# test is under-replicated
python ./scripts/write_test_file.py --file_path test --block_num 1 --block_replication 1
python ./scripts/write_test_file.py --file_path test_dir/test2

# Start Namenode
NAMENODE_ENDPOINT=$(cat "$NAMENODE_CONFIG")
./namenode/namenode "$NAMENODE_ENDPOINT" "./$LOG_DIR/namenode.log" &
NAMENODE_PID=$!
if [[ -z "$NAMENODE_PID" ]]; then
  echo "Failed to start Namenode"
  exit 1
fi
echo $NAMENODE_PID > "$PID_DIR/namenode.pid"
echo "Namenode started with PID $NAMENODE_PID"

# Sleep for 1 second to allow Namenode to start
# sleep 1

# Start Datanodes
ID=1
while IFS= read -r DATANODE_ENDPOINT; do
  DATANODE_SUBDIR="$DATANODE_DIR/$ID"
  mkdir -p "$DATANODE_SUBDIR"
  pushd "$DATANODE_SUBDIR" > /dev/null

  # Start Datanode
  ../../datanode/datanode "$NAMENODE_ENDPOINT" "$DATANODE_ENDPOINT" "../../$LOG_DIR/datanode_$ID.log" &
  DATANODE_PID=$!
  if [[ -z "$DATANODE_PID" ]]; then
    echo "Failed to start Datanode $ID"
    exit 1
  fi
  echo $DATANODE_PID > "../../$PID_DIR/datanode_$ID.pid"
  echo "Datanode $ID started with PID $DATANODE_PID"

  popd > /dev/null
  ID=$((ID + 1))
done < "$DATANODE_CONFIG"


# Function to kill and restart the Namenode
restart_namenode() {
  NAMENODE_PID=$(cat "$PID_DIR/namenode.pid")
  kill $NAMENODE_PID
  wait $NAMENODE_PID 2>/dev/null

  sleep 5

  ./namenode/namenode "$NAMENODE_ENDPOINT" "./$LOG_DIR/namenode.log" &
  NAMENODE_PID=$!
  echo $NAMENODE_PID > "$PID_DIR/namenode.pid"
  echo "Namenode restarted with PID $NAMENODE_PID"
}

# Function to kill and restart a specific Datanode
restart_datanode() {
  local ID=$1
  local DATANODE_PID_FILE="$PID_DIR/datanode_$ID.pid"

  if [[ ! -f "$DATANODE_PID_FILE" ]]; then
    echo "Datanode PID file not found for ID $ID"
    return 1
  fi

  DATANODE_PID=$(cat "$DATANODE_PID_FILE")
  kill $DATANODE_PID
  wait $DATANODE_PID 2>/dev/null

  sleep 5

  DATANODE_SUBDIR="$DATANODE_DIR/$ID"
  DATANODE_ENDPOINT=$(sed -n "${ID}p" "$DATANODE_CONFIG")
  pushd "$DATANODE_SUBDIR" > /dev/null

  ../../datanode/datanode "$NAMENODE_ENDPOINT" "$DATANODE_ENDPOINT" "../../$LOG_DIR/datanode_$ID.log" &
  DATANODE_PID=$!
  echo $DATANODE_PID > "../../$DATANODE_PID_FILE"

  popd > /dev/null
  echo "Datanode $ID restarted with PID $DATANODE_PID"
}

# Begin experiment here
sleep 5
go run examples/write/main.go # write test-write file
sleep 3
restart_namenode
go run examples/read/main.go --path test-write # try to read test-write file immediately
sleep 3
go run examples/read/main.go --path test-write # try to read test-write file after 3 seconds

# # Example usage:
# # Restart Namenode
# restart_namenode

# # Restart Datanode 1
# restart_datanode 1