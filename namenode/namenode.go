package main

import (
	"errors"
	"log/slog"
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
		for blockID, storageInfo := range inode.storageInfo {
			var newStorageNodes []string
			for _, storageNode := range storageInfo.DataNodes {
				if storageNode != blockReport.Endpoint {
					newStorageNodes = append(newStorageNodes, storageNode)
				}
			}
			storageInfo.DataNodes = newStorageNodes
			inode.storageInfo[blockID] = storageInfo
		}
		inode.rwlock.Unlock()
	}

	for blockName, metadata := range blockReport.BlockMetadata {
		// Parse the block name
		fileName, blockID, version, err := common.ParseBlockName(blockName)
		if err != nil {
			slog.Error("ParseBlockName error", "blockName", blockName)
			*success = false
			return errors.New("ParseBlockName error")
		}

		// If the file doesn't exist, create a new inode
		// TODO: check the semantics here
		inode, exists := server.inodes[fileName]
		if !exists {
			inode = iNode{
				fileAttributes: common.FileAttributes{
					Size: 0,
				},
				rwlock:      new(sync.RWMutex),
				storageInfo: make(common.FileStorageInfo),
			}
			server.inodes[fileName] = inode
			slog.Debug("Create new inode", "fileName", fileName,
				"fileAttributes", inode.fileAttributes, "storageInfo", inode.storageInfo)
		}

		inode.rwlock.Lock()

		// Update the storage info
		if storageInfo, ok := inode.storageInfo[blockID]; ok {
			if version > inode.storageInfo[blockID].LatestVersion {
				inode.storageInfo[blockID] = common.BlockStorageInfo{
					LatestVersion: version,
					Size:          metadata.Size,
					DataNodes:     []string{blockReport.Endpoint},
				}
				slog.Debug("Update storage info", "fileName", fileName, "blockID", blockID,
					slog.Any("storageInfo", inode.storageInfo[blockID]))
			} else if version == inode.storageInfo[blockID].LatestVersion {
				storageInfo.DataNodes = append(
					inode.storageInfo[blockID].DataNodes, blockReport.Endpoint)
				inode.storageInfo[blockID] = storageInfo
				slog.Debug("Append storage info", "fileName", fileName, "blockID", blockID,
					slog.Any("storageInfo", inode.storageInfo[blockID]))
			} else {
				// TODO: mark as stale block and return the info to the datanode
			}
		} else {
			slog.Debug("Create new storage info", "fileName", fileName, "blockID", blockID)
			inode.storageInfo[blockID] = common.BlockStorageInfo{
				LatestVersion: version,
				Size:          metadata.Size,
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

// ReadRequest handles the read request from the client,
// and returns (Block Name, DataNode Endpoint) pair for each block
// TODO: only return DataNode Endpoint ?
func (server *NameNode) ReadRequest(args *common.ReadFileRequest, reply *common.ReadFileResponse) error {
	slog.Info("ReadRequest", "file", args.FileName)

	server.inodeRWLock.RLock()
	inode, exists := server.inodes[args.FileName]
	server.inodeRWLock.RUnlock()

	if !exists {
		reply.Succeeded = false
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
			reply.Succeeded = false
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
		// 	reply.Succeeded = false
		// 	slog.Error("no alive DataNode")
		// 	return errors.New("no alive DataNode")
		// }
		// dataNode := aliveStorageNodes[rand.Intn(len(aliveStorageNodes))]

		blockname := common.ConstructBlockName(args.FileName, i, inode.storageInfo[i].LatestVersion)
		// TODO: should we not disclose version number to client?
		blockInfoList = append(blockInfoList, common.BlockInfo{
			BlockName:         blockname,
			DataNodeEndpoints: aliveStorageNodes,
		})
	}

	reply.Succeeded = true
	reply.BlockInfoList = blockInfoList

	slog.Debug("ReadRequest success", "file", args.FileName, "blockInfoList", blockInfoList)

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
