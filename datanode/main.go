package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

// Command Line Args:
// Args[1]: Namenode endpoint (IP:port)
// Args[2]: Datanode endpoint (IP:port)
// Args[3]: Optional log file path
func main() {
	// Check command line arguments
	if len(os.Args) < 3 || len(os.Args) > 4 {
		slog.Error("expect 3 or 4 command line arguments", "actual argument count", len(os.Args))
		os.Exit(1)
	}

	// Read and validate Namenode endpoint
	nameNodeEndpoint := os.Args[1]
	if !common.IsValidEndpoint(nameNodeEndpoint) {
		slog.Error("invalid namenode endpoint", "endpoint", nameNodeEndpoint)
		os.Exit(1)
	}

	// Read and validate Datanode endpoint
	dataNodeEndpoint := os.Args[2]
	if !common.IsValidEndpoint(dataNodeEndpoint) {
		slog.Error("invalid datanode endpoint", "endpoint", dataNodeEndpoint)
		os.Exit(1)
	}

	// Set up slog to log into a file or terminal
	var logHandler slog.Handler
	var programLevel = new(slog.LevelVar) // Info by default

	if len(os.Args) == 4 {
		logFilePath := os.Args[3]
		// Set file permissions to allow Read and Write for the owner
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
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

	// Initialize DataNode
	dataNode := DataNode{
		nameNodeEndpoint: nameNodeEndpoint,
		dataNodeEndpoint: dataNodeEndpoint,
	}
	slog.Info("Initialized datanode", "NameNode Endpoint", nameNodeEndpoint, "DataNode Endpoint", dataNodeEndpoint)

	// Register with Namenode
	if !dataNode.registerWithNameNode() {
		panic("Failed to register with Namenode")
	}

	// Send initial block report
	if !dataNode.sendBlockReport() {
		slog.Error("Failed to send initial block report")
		os.Exit(1)
	}

	// Start heartbeat loop
	go dataNode.heartbeatLoop()

	// Set up Datanode RPC server
	dataNodePort := strings.Split(dataNodeEndpoint, ":")[1]
	rpc.Register(&dataNode)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+dataNodePort) // Listen on all addresses
	if err != nil {
		slog.Error("listen error", "error", err)
		os.Exit(1)
	}
	http.Serve(listener, nil)
}
