package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

type iNode struct {
	fileAttributes common.FileAttributes
	rwlock         *sync.RWMutex // Per-file rwlock
	storageInfo    common.FileStorageInfo
}

type DataNodeInfo struct {
	heartbeatReceived bool // Whether received heartbeat during the last interval
	isAlive           bool
}

type NameNode struct {
	inodes      map[string]iNode
	inodeRWLock *sync.RWMutex // The RWLock for all the inodes

	datanodes      map[string]DataNodeInfo
	datanodeRWLock *sync.RWMutex // The RWLock for the datanodes
}

// `success` will be true iff the file doesn't exist
func (server *NameNode) Create(path string, success *bool) error {
	slog.Info("Create request", "path", path)

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

	_, exist := server.inodes[path]
	if exist {
		*success = false
		return errors.New("file exists")
	} else {
		metadata := common.FileAttributes{
			Size: 0,
		}
		server.inodes[path] = iNode{
			fileAttributes: metadata,
			rwlock:         new(sync.RWMutex),
			storageInfo:    make(common.FileStorageInfo),
		}
		*success = true
		return nil
	}
}

func (server *NameNode) Exists(path string, exists *bool) error {
	slog.Info("Exists request", "path", path)
	server.inodeRWLock.RLock()
	defer server.inodeRWLock.RUnlock()
	_, *exists = server.inodes[path]
	return nil
}

// TODO: 2pc, notify all the datanodes to delete the blocks before responding
// This RPC doesn't return anything
func (server *NameNode) Delete(path string, unused *bool) error {
	slog.Info("Delete request", "path", path)

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

	delete(server.inodes, path)
	return nil
}

func (server *NameNode) GetAttributes(path string, fileAttributes *common.FileAttributes) error {
	slog.Info("GetAttributes request", "path", path)
	server.inodeRWLock.RLock()
	defer server.inodeRWLock.RUnlock()
	*fileAttributes = server.inodes[path].fileAttributes
	return nil
}

func (server *NameNode) RegisterDataNode(dataNodeEndpoint string, success *bool) error {
	slog.Info("RegisterDataNode request", "dataNodeEndpoint", dataNodeEndpoint)

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

	// TODO: Check if the datanode is already registered
	server.datanodes[dataNodeEndpoint] = DataNodeInfo{
		heartbeatReceived: true,
		isAlive:           true,
	}

	*success = true
	return nil
}

// NameNode receives block report from DataNode,
// and updates all block storage info in the corresponding inode
func (server *NameNode) ReportBlock(blockReport common.BlockReport, success *bool) error {
	slog.Info("ReportBlock request", "dataNodeEndpoint", blockReport.Endpoint,
		slog.Any("Block MetaData", blockReport.BlockMetadata))

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

	// Delete all prior records of the reporting datanode's block storage info
	// so that we can update the latest info in the next step
	for _, inode := range server.inodes {
		inode.rwlock.Lock()
		// In each file, remove the reporting datanode's endpoint from the storage list
		for blockIndex, storageInfo := range inode.storageInfo {
			var newStorageNodes []string
			for _, storageNode := range storageInfo.DataNodes {
				if storageNode != blockReport.Endpoint {
					newStorageNodes = append(newStorageNodes, storageNode)
				}
			}
			storageInfo.DataNodes = newStorageNodes
			inode.storageInfo[blockIndex] = storageInfo
		}
		inode.rwlock.Unlock()
	}

	for _, blockMetadata := range blockReport.BlockMetadata {
		// If the file doesn't exist, create a new inode
		// TODO: check the semantics here
		inode, exists := server.inodes[blockMetadata.FileName]
		if !exists {
			inode = iNode{
				fileAttributes: common.FileAttributes{
					Size: 0,
				},
				rwlock:      new(sync.RWMutex),
				storageInfo: make(common.FileStorageInfo),
			}
			server.inodes[blockMetadata.FileName] = inode
			slog.Debug("Create new inode", "fileName", blockMetadata.FileName,
				"fileAttributes", inode.fileAttributes, "storageInfo", inode.storageInfo)
		}

		inode.rwlock.Lock()

		// Update the storage info
		if storageInfo, ok := inode.storageInfo[blockMetadata.BlockIndex]; ok {
			// Block info exists in namenode

			if blockMetadata.Version > inode.storageInfo[blockMetadata.BlockIndex].LatestVersion {
				// Find a newer version
				inode.storageInfo[blockMetadata.BlockIndex] = common.BlockStorageInfo{
					LatestVersion: blockMetadata.Version,
					Size:          blockMetadata.Size,
					DataNodes:     []string{blockReport.Endpoint},
				}
				slog.Debug("Update storage info",
					"file name", blockMetadata.FileName,
					"block index", blockMetadata.BlockIndex,
					slog.Any("storageInfo", inode.storageInfo[blockMetadata.BlockIndex]))
			} else if blockMetadata.Version == inode.storageInfo[blockMetadata.BlockIndex].LatestVersion {
				// Same version, find a new replica
				storageInfo.DataNodes = append(
					inode.storageInfo[blockMetadata.BlockIndex].DataNodes, blockReport.Endpoint)
				inode.storageInfo[blockMetadata.BlockIndex] = storageInfo
				slog.Debug("Append storage info",
					"file name", blockMetadata.FileName,
					"block index", blockMetadata.BlockIndex,
					slog.Any("storageInfo", inode.storageInfo[blockMetadata.BlockIndex]))
			} else {
				// TODO: mark as stale block and return the info to the datanode
			}
		} else {
			// Block info doesn't exist in namenode
			slog.Debug("Create new storage info",
				"file name", blockMetadata.FileName,
				"block index", blockMetadata.BlockIndex)
			inode.storageInfo[blockMetadata.BlockIndex] = common.BlockStorageInfo{
				LatestVersion: blockMetadata.Version,
				Size:          blockMetadata.Size,
				DataNodes:     []string{blockReport.Endpoint},
			}
		}

		inode.rwlock.Unlock()
	}

	// Update the file attributes
	for fileName, inode := range server.inodes {
		inode.rwlock.Lock()
		totalSize := uint(0)
		for _, storageInfo := range inode.storageInfo {
			totalSize += storageInfo.Size
		}
		inode.fileAttributes.Size = totalSize
		inode.rwlock.Unlock()

		slog.Debug("Updated inode", slog.String("fileName", fileName),
			"fileAttributes", inode.fileAttributes, slog.Any("storageInfo", inode.storageInfo))
	}

	slog.Info("ReportBlock success", "dataNodeEndpoint", blockReport.Endpoint)

	*success = true
	return nil
}

func (server *NameNode) Heartbeat(heartbeat common.Heartbeat, success *bool) error {
	slog.Info("Receive Heartbeat", "dataNodeEndpoint", heartbeat.Endpoint)

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

	// If the datanode is not registered, ignore the heartbeat
	if _, exists := server.datanodes[heartbeat.Endpoint]; !exists {
		*success = false
		slog.Error("Heartbeat datanode not registered", "dataNodeEndpoint", heartbeat.Endpoint)
		return errors.New("datanode not registered")
	}

	// Update the status of the datanode
	server.datanodes[heartbeat.Endpoint] = DataNodeInfo{
		heartbeatReceived: true,
		isAlive:           true,
	}

	*success = true
	return nil
}

func (server *NameNode) heartbeatMonitor() {
	for {
		time.Sleep(common.HEARTBEAT_MONITOR_INTERVAL) // Check periodically

		server.datanodeRWLock.Lock()
		for endpoint, dataNode := range server.datanodes {
			if !dataNode.heartbeatReceived {
				// The datanode hasn't sent a heartbeat
				server.datanodes[endpoint] = DataNodeInfo{
					heartbeatReceived: false,
					isAlive:           false,
				}
			} else {
				server.datanodes[endpoint] = DataNodeInfo{
					heartbeatReceived: false,
					isAlive:           true,
				}
			}
		}
		server.datanodeRWLock.Unlock()
	}
}

// GetBlockLocations handles the read request from the client,
// and returns (Block Name, DataNode Endpoint) pair for each block
// TODO: only return DataNode Endpoint ?
func (server *NameNode) GetBlockLocations(args *common.GetBlockLocationsRequest, reply *common.GetBlockLocationsResponse) error {
	slog.Info("GetBlockLocations", "file", args.FileName)

	server.inodeRWLock.RLock()
	inode, exists := server.inodes[args.FileName]
	server.inodeRWLock.RUnlock()

	if !exists {
		slog.Error("file not found", "file", args.FileName)
		return errors.New("file not found")
	}

	dataNodesStatus := server.getDataNodeLiveness()

	inode.rwlock.RLock()
	defer inode.rwlock.RUnlock()

	var blockInfoList []common.BlockInfo
	for i := args.BeginBlock; i < args.EndBlock; i++ {
		storageInfo, exist := inode.storageInfo[i]
		if !exist {
			slog.Error("block index does not exist", "block index", i)
			return errors.New("block index does not exist")
		}

		storageNodes := storageInfo.DataNodes
		// Filter out the alive storage nodes
		var hasDeadNode bool = false
		var aliveStorageNodes []string
		for _, storageNode := range storageNodes {
			if dataNodesStatus[storageNode] {
				aliveStorageNodes = append(aliveStorageNodes, storageNode)
			} else {
				hasDeadNode = true
			}
		}
		// Update the storage info if needed
		if hasDeadNode {
			inode.rwlock.RUnlock()
			inode.rwlock.Lock()
			storageInfo.DataNodes = aliveStorageNodes
			inode.storageInfo[i] = storageInfo
			inode.rwlock.Unlock()
			inode.rwlock.RLock()
		}
		// // Randomly select an alive storage node
		// if len(aliveStorageNodes) == 0 {
		// 	slog.Error("no alive DataNode")
		// 	return errors.New("no alive DataNode")
		// }
		// dataNode := aliveStorageNodes[rand.Intn(len(aliveStorageNodes))]

		// TODO: should we not disclose version number to client?
		blockInfoList = append(blockInfoList, common.BlockInfo{
			Version:           inode.storageInfo[i].LatestVersion,
			DataNodeEndpoints: aliveStorageNodes,
		})
	}

	reply.BlockInfoList = blockInfoList

	slog.Debug("GetBlockLocations success", "file", args.FileName, "blockInfoList", blockInfoList)

	return nil
}

// Returns the liveness status of all datanodes
func (server *NameNode) getDataNodeLiveness() map[string]bool {
	server.datanodeRWLock.RLock()
	defer server.datanodeRWLock.RUnlock()

	status := make(map[string]bool)
	for endpoint, dataNode := range server.datanodes {
		status[endpoint] = dataNode.isAlive
	}
	return status
}

// Command Line Args:
// Args[1]: Namenode endpoint (IP:port)
// Args[2]: Optional log file path
func main() {
	// Check command line arguments
	if len(os.Args) < 2 || len(os.Args) > 3 {
		panic(fmt.Sprintln("expect 2 or 3 command line arguments, actual argument count", len(os.Args)))
	}

	// Read and validate Namenode endpoint
	nameNodeEndpoint := os.Args[1]
	if !common.IsValidEndpoint(nameNodeEndpoint) {
		panic(fmt.Sprintln("invalid namenode endpoint", nameNodeEndpoint))
	}

	// Set up slog to log into a file or terminal
	var logHandler slog.Handler
	var programLevel = new(slog.LevelVar) // Info by default

	if len(os.Args) == 3 {
		logFilePath := os.Args[2]
		// Set file permissions to allow Read and Write for the owner
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			panic(fmt.Sprintln("failed to open log file", err))
		}
		defer logFile.Close()
		logHandler = slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: programLevel})
	} else {
		logHandler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel})
	}

	slog.SetDefault(slog.New(logHandler))
	programLevel.Set(slog.LevelDebug)

	// Initialize NameNode
	server := NameNode{
		inodes:         make(map[string]iNode),
		inodeRWLock:    new(sync.RWMutex),
		datanodeRWLock: new(sync.RWMutex),
		datanodes:      make(map[string]DataNodeInfo),
	}
	slog.Info("Initialized namenode", "nameNodeEndpoint", nameNodeEndpoint)

	// Set up namenode RPC server
	port := strings.Split(nameNodeEndpoint, ":")[1]
	rpc.Register(&server)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+port) // Listen on all addresses
	if err != nil {
		slog.Error("listen error", "error", err)
	}
	http.Serve(listener, nil)

	// Start the heartbeat monitor
	go server.heartbeatMonitor()

}
