package client

import (
	"errors"
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

type ReadHandle struct {
	dfs    *DistributedFileSystem
	path   string
	offset uint
}

func (dfs *DistributedFileSystem) OpenForRead(path string) ReadHandle {
	return ReadHandle{
		dfs:    dfs,
		path:   path,
		offset: 0,
	}
}

func (readHandle *ReadHandle) Seek(offset uint) {
	readHandle.offset = offset
}

func (readHandle *ReadHandle) Read(length uint) ([]byte, error) {
	data, err := readHandle.dfs.read(readHandle.path, readHandle.offset, length)
	if err == nil {
		readHandle.offset += uint(len(data))
	}
	return data, err
}

// read Operation. read the file specified by `path` from the offset `offset` with the length `length`
// Returns the data read and the error (if any)
func (dfs *DistributedFileSystem) read(path string, offset uint, length uint) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	// Step 1: Calculate the blocks that need to be read and their offsets
	beginBlock := offset / common.BLOCK_SIZE
	endBlock := (offset+length-1)/common.BLOCK_SIZE + 1
	beginBlockOffset := offset % common.BLOCK_SIZE
	endBlockOffset := (offset+length-1)%common.BLOCK_SIZE + 1

	// Step 2: Get the block metadata from the NameNode
	getBlockLocationsRequest := common.GetBlockLocationsRequest{
		FileName:   path,
		BeginBlock: uint(beginBlock),
		EndBlock:   uint(endBlock),
	}

	var getBlockLocationsResponse common.GetBlockLocationsResponse
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)
	if err != nil {
		slog.Error("dialing error", "error", err)
		return nil, err
	}

	err = client.Call("NameNode.GetBlockLocations", getBlockLocationsRequest, &getBlockLocationsResponse)
	if err != nil {
		slog.Error("Error during GetBlockLocations Request", "error", err)
		return nil, err
	}

	slog.Info("GetBlockLocations succeeded", "file", path, "BlockInfoList", getBlockLocationsResponse.BlockInfoList)

	// Step 3: Read the blocks from the DataNodes in parallel
	var wg sync.WaitGroup
	dataBuffer := make([]byte, length)
	allSucceeded := true

	for i, blockInfo := range getBlockLocationsResponse.BlockInfoList {
		wg.Add(1)

		beginOffset := uint(0)
		endOffset := uint(common.BLOCK_SIZE)
		if i == 0 {
			beginOffset = uint(beginBlockOffset)
		}
		if i == len(getBlockLocationsResponse.BlockInfoList)-1 {
			endOffset = uint(endBlockOffset)
		}

		go func(blockInfo common.BlockInfo, blockIndex uint, beginOffset uint, endOffset uint) {
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
				FileName:    path,
				BlockIndex:  blockIndex,
				Version:     blockInfo.Version,
				BeginOffset: beginOffset,
				Length:      endOffset - beginOffset,
			}

			var readBlockResponse common.ReadBlockResponse

			asyncRpcCall := dataNodeClient.Go("DataNode.ReadBlock", readBlockRequest, &readBlockResponse, nil)

			select {
			case <-asyncRpcCall.Done:
				if asyncRpcCall.Error != nil {
					slog.Error("ReadBlock RPC error", "error", asyncRpcCall.Error)
					allSucceeded = false
				} else {
					bufferStart := (blockIndex*common.BLOCK_SIZE + beginOffset) - offset
					slog.Debug("ReadBlock succeeded", "Block Index", readBlockRequest.BlockIndex, "Data", readBlockResponse.Data, "BufferStart", bufferStart)
					copy(dataBuffer[bufferStart:], readBlockResponse.Data)
				}
			case <-time.After(common.READ_BLOCK_TIMEOUT):
				slog.Error("ReadBlock RPC timeout", "DataNode Endpoint", dataNodeEndpoint)
				allSucceeded = false
			}

		}(blockInfo, beginBlock+uint(i), beginOffset, endOffset)
	}

	wg.Wait()

	if !allSucceeded {
		slog.Error("Failed to read all blocks")
		return nil, errors.New("failed to read all blocks")
	}

	return dataBuffer, nil
}

type WriteHandle struct {
	dfs    *DistributedFileSystem
	path   string
	offset uint
}

func (dfs *DistributedFileSystem) OpenForWrite(path string) WriteHandle {
	return WriteHandle{
		dfs:    dfs,
		path:   path,
		offset: 0,
	}
}

func (writeHandle *WriteHandle) Seek(offset uint) {
	writeHandle.offset = offset
}

func (writeHandle *WriteHandle) Write(data []byte) error {
	offset := writeHandle.offset
	length := uint(len(data))
	path := writeHandle.path
	dfs := writeHandle.dfs

	if length == 0 {
		return nil
	}

	// Step 1: Calculate the blocks that need to be read and their offsets
	beginBlock := offset / common.BLOCK_SIZE
	endBlock := (offset+length-1)/common.BLOCK_SIZE + 1
	beginBlockOffset := offset % common.BLOCK_SIZE
	endBlockOffset := (offset+length-1)%common.BLOCK_SIZE + 1

	// Step 2: Get the block metadata from the NameNode
	getBlockLocationsRequest := common.GetBlockLocationsRequest{
		FileName:   path,
		BeginBlock: uint(beginBlock),
		EndBlock:   uint(endBlock),
	}

	var getBlockLocationsResponse common.GetBlockLocationsResponse
	client, err := rpc.DialHTTP("tcp", dfs.endpoint)
	if err != nil {
		slog.Error("dialing error", "error", err)
		return err
	}

	err = client.Call("NameNode.GetBlockLocations", getBlockLocationsRequest, &getBlockLocationsResponse)
	if err != nil {
		slog.Error("Error during GetBlockLocations Request", "error", err)
		return err
	}

	slog.Info("GetBlockLocations succeeded", "file", path, "BlockInfoList", getBlockLocationsResponse.BlockInfoList)

	// Step 3: Read the blocks from the DataNodes in parallel
	var wg sync.WaitGroup
	allSucceeded := true

	for i, blockInfo := range getBlockLocationsResponse.BlockInfoList {
		wg.Add(1)

		beginOffset := uint(0)
		endOffset := uint(common.BLOCK_SIZE)
		if i == 0 {
			beginOffset = uint(beginBlockOffset)
		}
		if i == len(getBlockLocationsResponse.BlockInfoList)-1 {
			endOffset = uint(endBlockOffset)
		}

		go func(blockInfo common.BlockInfo, beginOffset uint, endOffset uint) {
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
				FileName:    path,
				BlockIndex:  beginBlock + uint(i),
				BeginOffset: beginOffset,
				Length:      endOffset - beginOffset,
			}

			var readBlockResponse common.ReadBlockResponse

			asyncRpcCall := dataNodeClient.Go("DataNode.ReadBlock", readBlockRequest, &readBlockResponse, nil)

			select {
			case <-asyncRpcCall.Done:
				if asyncRpcCall.Error != nil {
					slog.Error("ReadBlock RPC error", "error", asyncRpcCall.Error)
					allSucceeded = false
				} else {
					bufferStart := (beginBlock*common.BLOCK_SIZE + beginOffset) - offset
					slog.Debug("ReadBlock succeeded", "Block Index", readBlockRequest.BlockIndex, "Data", readBlockResponse.Data, "BufferStart", bufferStart)
				}
			case <-time.After(common.READ_BLOCK_TIMEOUT):
				slog.Error("ReadBlock RPC timeout", "DataNode Endpoint", dataNodeEndpoint)
				allSucceeded = false
			}

		}(blockInfo, beginOffset, endOffset)
	}

	wg.Wait()

	if !allSucceeded {
		slog.Error("Failed to write all blocks")
		return errors.New("failed to write all blocks")
	}

	return nil
}

func (writeHandle *WriteHandle) Close() {}
