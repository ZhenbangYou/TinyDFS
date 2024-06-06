package common

import "time"

const RPC_TIMEOUT = 1000 * time.Millisecond
const HEARTBEAT_INTERVAL = 3 * time.Second
const HEARTBEAT_MONITOR_INTERVAL = 10 * time.Second
const REPLICATION_MONITOR_INTERVAL = 60 * time.Second
const BLOCK_SIZE = 128 * 1024 * 1024 // 128MB
const BLOCK_REPLICATION = 3
const NETWORK_BANDWIDTH = 100 * 1024 * 1024 // 100MB/s

const MIN_VALID_VERSION_NUMBER = 1

const LEASE_TIMEOUT = 6 * time.Second
const LEASE_RENEWAL_INTERVAL = 3 * time.Second
