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
	success := dfs.Create(path)
	fmt.Println("create", success)

	exists := dfs.Exists(path)
	fmt.Println("exists after create", exists)

	attr, ok := dfs.GetAttributes(path)
	if ok {
		fmt.Println("file attributes", attr)
	}

	dfs.Delete(path)
	exists = dfs.Exists(path)
	fmt.Println("exists after delete", exists)
}
