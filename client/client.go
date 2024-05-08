package client

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"time"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

type DistributedFileSystem struct {
	endpoint string
}

func NewDistributedFileSystem(endpoint string) DistributedFileSystem {
	return DistributedFileSystem{endpoint: endpoint}
}

type OpenFile struct {
	FileHandle common.FileHandle
	FileOffset uint
}

func (dfs *DistributedFileSystem) Create(path string) (OpenFile, error) {
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)

	if err != nil {
		slog.Error("dialing error", "error:", err)
	}

	fileHandle := common.FileHandle{}

	asyncRpcCall := client.Go("NameNode.Create", path, &fileHandle, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			return OpenFile{}, asyncRpcCall.Error
		} else {
			return OpenFile{
				FileHandle: fileHandle,
				FileOffset: 0,
			}, nil
		}
	case <-time.After(time.Duration(time.Duration(common.RPC_TIMEOUT_MILLIS).Milliseconds())):
		return OpenFile{}, fmt.Errorf("Create timeout, DFS endpoint: %s, file path: %s", dfs.endpoint, path)
	}
}
