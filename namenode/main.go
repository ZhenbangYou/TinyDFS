package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

// Command Line Args:
// Args[1]: Namenode endpoint (IP:port)
// Args[2]: Optional log file path
func main() {
	// Check command line arguments
	if len(os.Args) < 2 || len(os.Args) > 3 {
		slog.Error("expect 2 or 3 command line arguments", "actual argument count", len(os.Args))
		os.Exit(1)
	}

	// Read and validate Namenode endpoint
	nameNodeEndpoint := os.Args[1]
	if !common.IsValidEndpoint(nameNodeEndpoint) {
		slog.Error("invalid namenode endpoint", "endpoint", nameNodeEndpoint)
		os.Exit(1)
	}

	// Set up slog to log into a file or terminal
	var logHandler slog.Handler
	var programLevel = new(slog.LevelVar) // Info by default

	if len(os.Args) == 3 {
		logFilePath := os.Args[2]
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			slog.Error("failed to open log file", "error", err)
			os.Exit(1)
		}
		defer logFile.Close()
		logHandler = slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: programLevel})
	} else {
		logHandler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})
	}

	slog.SetDefault(slog.New(logHandler))
	programLevel.Set(slog.LevelDebug)

	// Initialize NameNode
	server := NameNode{
		inodes:         make(map[string]iNode),
		inodeRWLock:    new(sync.RWMutex),
		datanodeRWLock: new(sync.RWMutex),
		datanodes:      make(map[string]DataNodeInfo),
	}
	slog.Info("Initialized namenode", "nameNodeEndpoint", nameNodeEndpoint)

	// Set up namenode RPC server
	port := strings.Split(nameNodeEndpoint, ":")[1]
	rpc.Register(&server)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+port) // Listen on all addresses
	if err != nil {
		slog.Error("listen error", "error", err)
	}
	http.Serve(listener, nil)

	// Start the heartbeat monitor
	go server.heartbeatMonitor()

}
