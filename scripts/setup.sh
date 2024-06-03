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

start_namenode() {
  NAMENODE_ENDPOINT=$(cat "$NAMENODE_CONFIG")
  NUM_DATANODES=$(wc -l < "$DATANODE_CONFIG")
  ./namenode/namenode "$NAMENODE_ENDPOINT" $NUM_DATANODES "./$LOG_DIR/namenode.log" &
  NAMENODE_PID=$!
  if [[ -z "$NAMENODE_PID" ]]; then
    echo "Failed to start Namenode"
    exit 1
  fi
  echo $NAMENODE_PID > "$PID_DIR/namenode.pid"
  echo "Namenode started with PID $NAMENODE_PID"
}

kill_namenode() {
  NAMENODE_PID=$(cat "$PID_DIR/namenode.pid")
  kill $NAMENODE_PID
  wait $NAMENODE_PID 2>/dev/null
  echo "Namenode killed"
}

start_datanode() {
  local ID=$1
  local DATANODE_PID_FILE="$PID_DIR/datanode_$ID.pid"
  DATANODE_ENDPOINT=$(sed -n "${ID}p" "$DATANODE_CONFIG")

  DATANODE_SUBDIR="$DATANODE_DIR/$ID"
  mkdir -p "$DATANODE_SUBDIR"
  pushd "$DATANODE_SUBDIR" > /dev/null

  ../../datanode/datanode "$NAMENODE_ENDPOINT" "$DATANODE_ENDPOINT" "../../$LOG_DIR/datanode_$ID.log" &
  DATANODE_PID=$!
  echo $DATANODE_PID > "../../$DATANODE_PID_FILE"

  popd > /dev/null
  echo "Datanode $ID started with PID $DATANODE_PID"
}

kill_datanode() {
  local ID=$1
  local DATANODE_PID_FILE="$PID_DIR/datanode_$ID.pid"

  if [[ ! -f "$DATANODE_PID_FILE" ]]; then
    echo "Datanode PID file not found for ID $ID"
    return 1
  fi

  DATANODE_PID=$(cat "$DATANODE_PID_FILE")
  kill $DATANODE_PID
  wait $DATANODE_PID 2>/dev/null
  echo "Datanode $ID killed"
}

#### Begin experiment here

## Write pre-defined test files
# `test`: under-replicated file of 1 block
python ./scripts/write_test_file.py --file_path test --block_num 1 --block_replication 1
# `test2`: normal file of 3 blocks
python ./scripts/write_test_file.py --file_path test_dir/test2

## Start system
# Start Namenode
start_namenode
# Start Datanodes
ID=1
while IFS= read -r DATANODE_ENDPOINT; do
  start_datanode $ID
  ID=$((ID + 1))
done < "$DATANODE_CONFIG"

## Run tests
sleep 2
go run examples/write/main.go --path test-write
go run examples/concurrent-write/main.go --path test-concurrency
kill_datanode 1
kill_datanode 2
# read after killing datanodes
sleep 15
go run examples/read/main.go --path test-write --offset 0 --length 10
go run examples/read/main.go --path test-concurrency --offset 0 --length 10
# write after killing datanodes
go run examples/write/main.go --path test-datanode-down
sleep 5
kill_namenode
start_datanode 1
start_namenode
# try to read test-write file immediately after restarting namenode
go run examples/read/main.go --path test-write --offset 0 --length 10
# wait for namenode to recover
start_datanode 2
sleep 6
# try to read files again
go run examples/read/main.go --path test-write --offset 0 --length 10
go run examples/read/main.go --path test-concurrency --offset 0 --length 10
go run examples/read/main.go --path test-datanode-down --offset 0 --length 10