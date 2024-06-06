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
	nBlocks := flag.Int("n", 1, "Number of blocks to read")
	logFile := flag.String("log", "experiments/read_performance.log", "Log file to store the results")
	path := flag.String("path", "test-read", "Path of the file to read")

	// Parse command line flags
	flag.Parse()

	// Create a new DistributedFileSystem client
	dfs, err := client.ConnectDistributedFileSystem(*serverAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer dfs.Close()

	// Calculate the length of the file
	length := uint(common.BLOCK_SIZE * (*nBlocks))

	// Check if the large file exists, if not, create it
	if !dfs.Exists(*path) {
		err = dfs.Create(*path)
		if err != nil {
			log.Fatal(err)
		}
		writeHandle := dfs.OpenForWrite(*path)
		cur_offset := uint(0)
		for cur_offset < length {
			err = writeHandle.Write(make([]byte, common.BLOCK_SIZE))
			for err != nil {
				// log.Fatal(err)
				err = writeHandle.Write(make([]byte, common.BLOCK_SIZE))
			}
			cur_offset += common.BLOCK_SIZE
			writeHandle.Seek(cur_offset)
		}
	}

	// Measure read performance
	startTime := time.Now()

	readHandle := dfs.OpenForRead(*path)

	// Read the file
	if _, err := readHandle.Read(length); err != nil {
		log.Fatal("Read failed: ", err)
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
