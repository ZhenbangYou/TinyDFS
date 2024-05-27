package main

import (
	"flag"
	"fmt"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/client"
)

func main() {
	// Define command line flags
	serverAddr := flag.String("server", "localhost:8000", "Address of the distributed file system server")
	path := flag.String("path", "test", "Path of the file to read")
	offset := flag.Uint("offset", 10, "Offset to start reading from")
	length := flag.Uint("length", 10, "Number of bytes to read")

	// Parse command line flags
	flag.Parse()
	fmt.Println("serverAddr", *serverAddr, "path", *path, "offset", *offset, "length", *length)

	// Create a new DistributedFileSystem client
	dfs := client.NewDistributedFileSystem(*serverAddr)

	// Check if the file exists
	exists := dfs.Exists(*path)
	fmt.Println("exists", exists)

	readHandle := dfs.OpenForRead(*path)
	readHandle.Seek(*offset)

	// Read the specified portion of the file
	data, err := readHandle.Read(*length)
	if err == nil {
		fmt.Println("read", string(data))
	} else {
		fmt.Println("read failed", err)
	}
}
