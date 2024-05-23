package client

import (
	"log/slog"
	"math/rand"
	"net/rpc"
	"sync"
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

// TODO: return error?
func (dfs *DistributedFileSystem) Read(path string, offset uint, length uint) ([]byte, bool) {
	// Step 1: Calculate the blocks that need to be read and their offsets
	startBlock := offset / common.BLOCK_SIZE
	endBlock := (offset + length) / common.BLOCK_SIZE
	startOffset := offset % common.BLOCK_SIZE
	endOffset := (offset + length) % common.BLOCK_SIZE

	// Step 2: Get the block metadata from the NameNode
	readRequest := common.ReadFileRequest{
		FileName:   path,
		BlockRange: [2]uint{uint(startBlock), uint(endBlock + 1)},
	}

	var readResponse common.ReadFileResponse
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)
	if err != nil {
		slog.Error("dialing error", "error", err)
		return nil, false
	}

	err = client.Call("NameNode.ReadRequest", readRequest, &readResponse)
	if err != nil {
		slog.Error("Error during Read Request", "error", err)
		return nil, false
	}
	if !readResponse.Succeeded {
		slog.Error("ReadRequest failed", "file", path)
		return nil, false
	}

	slog.Info("ReadRequest succeeded", "file", path, "BlockInfoList", readResponse.BlockInfoList)

	// Step 3: Read the blocks from the DataNodes in parallel
	var wg sync.WaitGroup
	dataBuffer := make([]byte, length)
	allSucceeded := true

	for i, blockInfo := range readResponse.BlockInfoList {
		wg.Add(1)

		var byteOffset [2]uint
		if i == 0 {
			byteOffset[0] = uint(startOffset)
		} else {
			byteOffset[0] = 0
		}
		if i == len(readResponse.BlockInfoList)-1 {
			byteOffset[1] = uint(endOffset)
		} else {
			byteOffset[1] = common.BLOCK_SIZE
		}

		go func(blockInfo common.BlockInfo, byteOffset [2]uint) {
			defer wg.Done()
			// Randomly choose one DataNode to read the block
			dataNodeEndpoint := blockInfo.DataNodeEndpoints[rand.Intn(len(blockInfo.DataNodeEndpoints))]
			dataNodeClient, err := rpc.DialHTTP("tcp", dataNodeEndpoint)
			if err != nil {
				slog.Error("dialing error", "error", err, "DataNode Endpoint", dataNodeEndpoint)
				allSucceeded = false
				return
			}

			readBlockRequest := common.ReadBlockRequest{
				BlockName:  blockInfo.BlockName,
				ByteOffset: byteOffset,
			}

			var readBlockResponse common.ReadBlockResponse

			asyncRpcCall := dataNodeClient.Go("DataNode.ReadBlock", readBlockRequest, &readBlockResponse, nil)

			select {
			case <-asyncRpcCall.Done:
				if asyncRpcCall.Error != nil {
					slog.Error("ReadBlock RPC error", "error", asyncRpcCall.Error)
					allSucceeded = false
				} else {
					_, blockID, _, err := common.ParseBlockName(blockInfo.BlockName)
					if err != nil {
						slog.Error("Error parsing block name", "error", err)
						allSucceeded = false
						return
					}
					bufferStart := (blockID*common.BLOCK_SIZE + byteOffset[0]) - offset
					slog.Debug("ReadBlock succeeded", "BlockName", blockInfo.BlockName, "Data", readBlockResponse.Data, "BufferStart", bufferStart)
					copy(dataBuffer[bufferStart:], readBlockResponse.Data)
				}
			case <-time.After(common.READ_BLOCK_TIMEOUT):
				slog.Error("ReadBlock RPC timeout", "DataNode Endpoint", dataNodeEndpoint)
				allSucceeded = false
			}

		}(blockInfo, byteOffset)
	}

	wg.Wait()

	if !allSucceeded {
		slog.Error("Failed to read all blocks")
		return nil, false
	}

	return dataBuffer, true
}
