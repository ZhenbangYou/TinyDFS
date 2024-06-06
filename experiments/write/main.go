package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/client"
	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

func main() {
	// Define command line flags
	serverAddr := flag.String("server", "10.138.0.3:8000", "Address of the distributed file system server")
	nBlocks := flag.Int("n", 1, "Number of blocks to write")
	logFile := flag.String("log", "experiments/write_performance.log", "Log file to store the results")
	path := flag.String("path", "test-write", "Path of the file to write")

	// Parse command line flags
	flag.Parse()

	// Create a new DistributedFileSystem client
	dfs, err := client.ConnectDistributedFileSystem(*serverAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer dfs.Close()

	if dfs.Exists(*path) {
		dfs.Delete(*path)
	}

	err = dfs.Create(*path)
	if err != nil {
		log.Fatal(err)
	}

	writeHandle := dfs.OpenForWrite(*path)

	// Calculate the length of the file
	length := uint(common.BLOCK_SIZE * (*nBlocks))

	// Measure write performance
	startTime := time.Now()
	err = writeHandle.Write(make([]byte, length))
	if err != nil {
		log.Fatal(err)
	}

	duration := time.Since(startTime).Seconds()
	bandwidth := float64(length) / duration / (1024 * 1024) // in MB/s

	// Log the results
	logFileHandle, err := os.OpenFile(*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Failed to open log file: ", err)
	}
	defer logFileHandle.Close()

	logFileHandle.WriteString(fmt.Sprintf("%f\n", bandwidth))
}
