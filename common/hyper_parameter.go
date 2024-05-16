package common

import "time"

const RPC_TIMEOUT = 1000 * time.Millisecond
const HEARTBEAT_INTERVAL = 3 * time.Second
const HEARTBEAT_TIMEOUT = 6 * time.Second
const HEARTBEAT_MONITOR_INTERVAL = 15 * time.Second
const RETRY_MIN_INTERVAL = 1 * time.Second
const RETRY_MAX_INTERVAL = 5 * time.Second
