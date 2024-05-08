package main

import (
	"fmt"
	"log"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/client"
)

func main() {
	dfs := client.NewDistributedFileSystem("localhost:8000")
	fileHandle, err := dfs.Create("/path/to/file")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(fileHandle)
}
