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
	endpoint       string
	namenodeClient *rpc.Client
}

func ConnectDistributedFileSystem(endpoint string) (DistributedFileSystem, error) {
	namenodeClient, err := rpc.DialHTTP("tcp", endpoint)

	if err != nil {
		slog.Error("dialing error", "error", err)
		return DistributedFileSystem{}, err
	}

	return DistributedFileSystem{endpoint: endpoint, namenodeClient: namenodeClient}, nil
}

func (dfs *DistributedFileSystem) Close() {
	dfs.namenodeClient.Close()
}

func (dfs *DistributedFileSystem) Create(fileName string) error {
	var success bool
	asyncRpcCall := dfs.namenodeClient.Go("NameNode.Create", fileName, &success, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("Create RPC error", "error", asyncRpcCall.Error)
			return asyncRpcCall.Error
		} else {
			return nil
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("Create RPC timeout", "DFS endpoint", dfs.endpoint, "file name", fileName)
		return errors.New("create timeout")
	}
}

func (dfs *DistributedFileSystem) Exists(fileName string) bool {
	var success bool
	asyncRpcCall := dfs.namenodeClient.Go("NameNode.Exists", fileName, &success, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("Exists RPC", "error", asyncRpcCall.Error)
			return false
		} else {
			return success
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("Exists RPC timeout", "DFS endpoint", dfs.endpoint, "file name", fileName)
		return false
	}
}

// Returns whether the request succeeds, which does nothing with whether the file specified
// by `fileName` exists when this request is issued
func (dfs *DistributedFileSystem) Delete(fileName string) bool {
	var success bool
	asyncRpcCall := dfs.namenodeClient.Go("NameNode.Delete", fileName, &success, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("Delete RPC", "error", asyncRpcCall.Error)
			return false
		} else {
			return true
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("Delete RPC timeout", "DFS endpoint", dfs.endpoint, "file name", fileName)
		return false
	}
}

func (dfs *DistributedFileSystem) GetSize(fileName string) (uint, bool) {
	var size uint
	asyncRpcCall := dfs.namenodeClient.Go("NameNode.GetSize", fileName, &size, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("GetSize RPC", "error", asyncRpcCall.Error)
			return 0, false
		} else {
			return size, true
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("GetAttributes RPC timeout", "DFS endpoint", dfs.endpoint, "file name", fileName)
		return 0, false
	}
}

type ReadHandle struct {
	dfs      *DistributedFileSystem
	fileName string
	offset   uint
}

func (dfs *DistributedFileSystem) OpenForRead(fileName string) ReadHandle {
	return ReadHandle{
		dfs:      dfs,
		fileName: fileName,
		offset:   0,
	}
}

func (readHandle *ReadHandle) Seek(offset uint) {
	readHandle.offset = offset
}

// read Operation. read the file specified by `readHandle.fileName` from the offset `offset` with the length `length`
// Returns the data read and the error (if any)
func (readHandle *ReadHandle) Read(length uint) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	// Step 1: Calculate the blocks that need to be read and their offsets
	beginBlock := readHandle.offset / common.BLOCK_SIZE
	endBlock := (readHandle.offset+length-1)/common.BLOCK_SIZE + 1
	beginBlockOffset := readHandle.offset % common.BLOCK_SIZE
	endBlockOffset := (readHandle.offset+length-1)%common.BLOCK_SIZE + 1

	// Step 2: Get the block metadata from the NameNode
	getBlockLocationsRequest := common.GetBlockLocationsRequest{
		FileName:   readHandle.fileName,
		BeginBlock: uint(beginBlock),
		EndBlock:   uint(endBlock),
	}

	var getBlockLocationsResponse common.GetBlockLocationsResponse
	asyncRpcCall := readHandle.dfs.namenodeClient.Go("NameNode.GetBlockLocations", getBlockLocationsRequest, &getBlockLocationsResponse, nil)

	select {
	case <-asyncRpcCall.Done:
		if asyncRpcCall.Error != nil {
			slog.Error("Error during GetBlockLocations Request", "error", asyncRpcCall.Error)
			return nil, asyncRpcCall.Error
		} else {
			break
		}
	case <-time.After(common.RPC_TIMEOUT):
		slog.Error("GetBlockLocations Request timeout", "DFS endpoint", readHandle.dfs.endpoint, "file name", readHandle.fileName)
		return nil, errors.New("GetBlockLocations Request timeout")
	}

	slog.Info("GetBlockLocations succeeded", "file", readHandle.fileName, "BlockInfoList", getBlockLocationsResponse.BlockInfoList)

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
				BlockID: common.BlockIdentifier{
					FileName:   readHandle.fileName,
					BlockIndex: blockIndex,
					Version:    blockInfo.Version,
				},
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
					bufferStart := (blockIndex*common.BLOCK_SIZE + beginOffset) - readHandle.offset
					slog.Debug("ReadBlock succeeded",
						"Block Index", readBlockRequest.BlockID.BlockIndex,
						"Data", readBlockResponse.Data,
						"BufferStart", bufferStart)
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

	readHandle.offset += uint(len(dataBuffer))

	return dataBuffer, nil
}

type WriteHandle struct {
	dfs        *DistributedFileSystem
	fileName   string
	offset     uint
	leaseToken uint64
	closed     bool
}

func (dfs *DistributedFileSystem) OpenForWrite(fileName string) WriteHandle {
	return WriteHandle{
		dfs:      dfs,
		fileName: fileName,
		offset:   0,
		closed:   false,
	}
}

func (writeHandle *WriteHandle) Seek(offset uint) {
	writeHandle.offset = offset
}

func (writeHandle *WriteHandle) Write(data []byte) error {
	offset := writeHandle.offset
	length := uint(len(data))
	fileName := writeHandle.fileName

	if length == 0 {
		return nil
	}

	// Step 1: Calculate the blocks that need to be read and their offsets
	beginBlock := offset / common.BLOCK_SIZE
	endBlock := (offset+length-1)/common.BLOCK_SIZE + 1
	beginBlockOffset := offset % common.BLOCK_SIZE
	endBlockOffset := (offset+length-1)%common.BLOCK_SIZE + 1

	// Step 2: Get the block metadata from the NameNode
	for writeHandle.leaseToken == 0 {
		writeHandle.leaseToken = rand.Uint64()
	}

	getBlockLocationsRequest := common.GetBlockLocationsRequest{
		FileName:   fileName,
		BeginBlock: uint(beginBlock),
		EndBlock:   uint(endBlock),
		LeaseToken: writeHandle.leaseToken,
	}

	var getBlockLocationsResponse common.GetBlockLocationsResponse
	err := writeHandle.dfs.namenodeClient.Call("NameNode.GetBlockLocations",
		getBlockLocationsRequest, &getBlockLocationsResponse)
	if err != nil {
		slog.Error("Error during GetBlockLocations Request", "error", err)
		return err
	}

	slog.Info("GetBlockLocations succeeded", "file", fileName, "BlockInfoList", getBlockLocationsResponse.BlockInfoList)

	go func() {
		time.Sleep(common.LEASE_RENEWAL_INTERVAL)

		for !writeHandle.closed {
			var unused bool
			writeHandle.dfs.namenodeClient.Call("NameNode.RenewLease", common.Lease{
				FileName:   writeHandle.fileName,
				LeaseToken: writeHandle.leaseToken,
			}, &unused)
			time.Sleep(common.LEASE_RENEWAL_INTERVAL)
		}
	}()

	// Step 3: Write the blocks to the DataNodes in parallel
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

		go func(blockInfo common.BlockInfo, blockIndex uint, beginOffset uint, endOffset uint) {
			defer wg.Done()
			dataNodeEndpoint := blockInfo.DataNodeEndpoints[0]
			dataNodeClient, err := rpc.DialHTTP("tcp", dataNodeEndpoint)
			if err != nil {
				slog.Error("dialing error", "error", err, "DataNode Endpoint", dataNodeEndpoint)
				allSucceeded = false
				return
			}

			writeBlockRequest := common.WriteBlockRequest{
				BlockID: common.BlockIdentifier{
					FileName:   fileName,
					BlockIndex: blockIndex,
					Version:    blockInfo.Version + 1,
				},
				BeginOffset:      beginOffset,
				Data:             data[beginOffset+common.BLOCK_SIZE*blockIndex-writeHandle.offset : endOffset+common.BLOCK_SIZE*blockIndex-writeHandle.offset],
				ReplicaEndpoints: blockInfo.DataNodeEndpoints,
				IndexInChain:     0,
				LeaseToken:       writeHandle.leaseToken,
			}

			var unused bool
			asyncRpcCall := dataNodeClient.Go("DataNode.WriteBlock", writeBlockRequest, &unused, nil)

			writeBlockTimeout :=
				time.Millisecond*(common.BLOCK_SIZE*1024/common.NETWORK_BANDWIDTH)*3 +
					common.RPC_TIMEOUT*(common.BLOCK_REPLICATION+1)

			select {
			case <-asyncRpcCall.Done:
				if asyncRpcCall.Error != nil {
					slog.Error("WriteBlock RPC error", "error", asyncRpcCall.Error)
					allSucceeded = false
				} else {
					slog.Debug("WriteBlock succeeded", "Block Index", writeBlockRequest.BlockID.BlockIndex)
				}
			case <-time.After(writeBlockTimeout):
				slog.Error("WriteBlock RPC timeout", "DataNode Endpoint", dataNodeEndpoint)
				allSucceeded = false
			}

		}(blockInfo, beginBlock+uint(i), beginOffset, endOffset)
	}

	wg.Wait()

	if !allSucceeded {
		slog.Error("Failed to write all blocks")
		return errors.New("failed to write all blocks")
	}

	writeHandle.offset += uint(len(data))

	return nil
}

func (writeHandle *WriteHandle) Close() error {
	if writeHandle.leaseToken != 0 {
		writeHandle.closed = true
		var unused bool
		asyncRpcCall := writeHandle.dfs.namenodeClient.Go("NameNode.RevokeLease", common.Lease{
			FileName:   writeHandle.fileName,
			LeaseToken: writeHandle.leaseToken,
		}, &unused, nil)
		select {
		case <-asyncRpcCall.Done:
			if asyncRpcCall.Error != nil {
				slog.Error("RevokeLease RPC error", "error", asyncRpcCall.Error)
				return asyncRpcCall.Error
			} else {
				slog.Debug("RevokeLease succeeded", "file name", writeHandle.fileName)
			}
		case <-time.After(common.RPC_TIMEOUT):
			slog.Error("RevokeLease RPC timeout")
			return errors.New("RevokeLease RPC timeout")
		}
	}
	return nil
}
