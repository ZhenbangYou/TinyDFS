package main

import (
	"fmt"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/client"
)

func main() {
	dfs := client.NewDistributedFileSystem("localhost:8000")
	path := "/path/to/file"
	success := dfs.Create(path)
	fmt.Println("create", success)

	exists := dfs.Exists(path)
	fmt.Println("exists after create", exists)
	dfs.Delete(path)
	exists = dfs.Exists(path)
	fmt.Println("exists after delete", exists)
}
