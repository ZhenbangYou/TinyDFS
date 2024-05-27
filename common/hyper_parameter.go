package common

import "time"

const RPC_TIMEOUT = 1000 * time.Millisecond
const HEARTBEAT_INTERVAL = 3 * time.Second
const HEARTBEAT_MONITOR_INTERVAL = 10 * time.Second
const BLOCK_SIZE = 4
const BLOCK_REPLICATION = 3
const NETWORK_BANDWIDTH = 100 * 1024 * 1024 // 100MB/s
const READ_BLOCK_TIMEOUT = 3*time.Duration(BLOCK_SIZE/NETWORK_BANDWIDTH)*time.Second + RPC_TIMEOUT

const MIN_VALID_VERSION_NUMBER = 1
