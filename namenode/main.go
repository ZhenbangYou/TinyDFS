package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

// Command Line Args:
// Args[1]: Path to the config file specifying namenode endpoint
func main() {
	// Set up slog
	var programLevel = new(slog.LevelVar) // Info by default
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})
	slog.SetDefault(slog.New(h))
	programLevel.Set(slog.LevelDebug)

	// Get namenode endpoint
	if len(os.Args) != 2 {
		slog.Error("expect 2 command line arguments", "actual argument count", len(os.Args))
	}
	nameNodeEndpointBytes, err := os.ReadFile(os.Args[1])
	if err != nil {
		slog.Error("file read error", "error", err, "path", os.Args[1])
	}
	nameNodeEndpoint := string(nameNodeEndpointBytes)
	port := strings.Split(nameNodeEndpoint, ":")[1]

	// Set up namenode RPC server
	server := NameNode{
		inodes:       make(map[string]iNode),
		globalRWLock: new(sync.RWMutex),
	}
	rpc.Register(&server)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+port) // Listen on all addresses
	if err != nil {
		slog.Error("listen error", "error", err)
	}
	http.Serve(listener, nil)
}
