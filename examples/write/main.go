package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/client"
)

func main() {
	// Define command line flags
	serverAddr := flag.String("server", "localhost:8000", "Address of the distributed file system server")
	path := flag.String("path", "test-write", "file name")

	// Parse command line flags
	flag.Parse()
	fmt.Println("serverAddr", *serverAddr, "path", *path)

	// Create a new DistributedFileSystem client
	dfs := client.NewDistributedFileSystem(*serverAddr)

	if dfs.Exists(*path) {
		dfs.Delete(*path)
	}

	err := dfs.Create(*path)
	if err != nil {
		log.Fatal(err)
	}

	writeHandle := dfs.OpenForWrite(*path)
	err = writeHandle.Write([]byte("01234"))
	if err != nil {
		log.Fatal(err)
	}
	writeHandle.Seek(10)
	err = writeHandle.Write([]byte("56789"))
	if err != nil {
		log.Fatal(err)
	}
	writeHandle.Close()

	readHandle := dfs.OpenForRead(*path)

	// Read the specified portion of the file
	data, err := readHandle.Read(15)
	if err == nil {
		fmt.Println("read", string(data), "length", len(data))
	} else {
		fmt.Println("read failed", err)
	}
}
