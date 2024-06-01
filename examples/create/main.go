package main

import (
	"fmt"
	"log"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/client"
)

func main() {
	dfs, err := client.ConnectDistributedFileSystem("localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	defer dfs.Close()
	path := "/path/to/file"
	err = dfs.Create(path)
	if err != nil {
		log.Fatal("Create fail", err)
	}
	fmt.Println("create succeeded")

	exists := dfs.Exists(path)
	fmt.Println("exists after create", exists)

	size, ok := dfs.GetSize(path)
	if ok {
		fmt.Println("file attributes", size)
	}

	dfs.Delete(path)
	exists = dfs.Exists(path)
	fmt.Println("exists after delete", exists)
}
