package client

import (
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

func (dfs *DistributedFileSystem) Create(path string) bool {
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)

	if err != nil {
		slog.Error("dialing error", "error", err)
		return false
	}

	var success bool

	asyncRpcCall := client.Go("NameNode.Create", path, &success, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("Create RPC error", "error", asyncRpcCall.Error)
			return false
		} else {
			return success
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("Create RPC timeout", "DFS endpoint", dfs.endpoint, "file path", path)
		return false
	}
}

func (dfs *DistributedFileSystem) Exists(path string) bool {
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)

	if err != nil {
		slog.Error("dialing error", "error", err)
		return false
	}

	var success bool

	asyncRpcCall := client.Go("NameNode.Exists", path, &success, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("Exists RPC error", "error", asyncRpcCall.Error)
			return false
		} else {
			return success
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("Exists RPC timeout", "DFS endpoint", dfs.endpoint, "file path", path)
		return false
	}
}

// Returns whether the request succeeds, which does nothing with whether the file specified
// by `path` exists when this request is issued
func (dfs *DistributedFileSystem) Delete(path string) bool {
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)

	if err != nil {
		slog.Error("dialing error", "error", err)
		return false
	}

	var success bool

	asyncRpcCall := client.Go("NameNode.Delete", path, &success, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("Delete RPC error", "error", asyncRpcCall.Error)
			return false
		} else {
			return true
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("Delete RPC timeout", "DFS endpoint", dfs.endpoint, "file path", path)
		return false
	}
}

func (dfs *DistributedFileSystem) GetAttributes(path string) (common.FileAttributes, bool) {
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)

	if err != nil {
		slog.Error("dialing error", "error", err)
		return common.FileAttributes{}, false
	}

	var fileAttributes common.FileAttributes

	asyncRpcCall := client.Go("NameNode.GetAttributes", path, &fileAttributes, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("GetAttributes RPC error", "error", asyncRpcCall.Error)
			return common.FileAttributes{}, false
		} else {
			return fileAttributes, true
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("GetAttributes RPC timeout", "DFS endpoint", dfs.endpoint, "file path", path)
		return common.FileAttributes{}, false
	}
}
