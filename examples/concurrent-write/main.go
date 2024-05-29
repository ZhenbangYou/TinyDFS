package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/client"
)

func main() {
	// Define command line flags
	serverAddr := flag.String("server", "localhost:8000", "Address of the distributed file system server")
	path := flag.String("path", "test-concurrency", "file name")

	// Parse command line flags
	flag.Parse()
	fmt.Println("serverAddr", *serverAddr, "path", *path)

	// Read the file content to verify the writes
	dfs, err := client.ConnectDistributedFileSystem(*serverAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer dfs.Close()

	// Create a new file for clients
	if dfs.Exists(*path) {
		dfs.Delete(*path)
	}
	err = dfs.Create(*path)
	if err != nil {
		log.Fatal(err)
	}

	// Function to perform write operations by a client
	writeToFile := func(clientID int, wg *sync.WaitGroup) {
		defer wg.Done()

		// Create a new DistributedFileSystem client
		dfs, err := client.ConnectDistributedFileSystem(*serverAddr)
		if err != nil {
			log.Fatalf("Client %d: %v", clientID, err)
		}
		defer dfs.Close()

		writeHandle := dfs.OpenForWrite(*path)
		data := fmt.Sprintf("Client %d\n", clientID)
		err = writeHandle.Write([]byte(data))
		for {
			if err == nil {
				break
			}
			fmt.Printf("Client %d write failed: %v", clientID, err)
			// try again
			time.Sleep(1 * time.Second)
			err = writeHandle.Write([]byte(data))
		}

		fmt.Printf("Client %d: Write operations completed.\n", clientID)
	}

	var wg sync.WaitGroup

	// Launch two concurrent clients
	wg.Add(2)
	go writeToFile(1, &wg)
	go writeToFile(2, &wg)

	// Wait for both clients to finish
	wg.Wait()

	readHandle := dfs.OpenForRead(*path)
	time.Sleep(1 * time.Second) // Add a delay to ensure both clients have finished writing
	data, err := readHandle.Read(1024)
	if err == nil {
		fmt.Println("Final file content:\n", string(data))
	} else {
		fmt.Println("Read failed", err)
	}
}
