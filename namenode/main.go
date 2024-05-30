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

	"github.com/emirpasic/gods/maps/treemap"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

type BlockStorageInfo struct {
	LatestVersion uint     // The latest version of the block, version = 0 represents invalid
	Size          uint     // The size of the block
	DataNodes     []string // Endpoints of datanodes storing the block of the latest version
}

// Key: Block Index
type FileStorageInfo map[uint]BlockStorageInfo

type internalLeaseInfo struct {
	leaseToken           uint64
	lastRenewalTimestamp time.Time
}

type iNode struct {
	fileAttributes common.FileAttributes
	rwlock         *sync.RWMutex // Per-file rwlock
	storageInfo    FileStorageInfo
	lease          internalLeaseInfo
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

	datanodeLoadRank *treemap.Map   // block count (int) -> hashset of endpoints
	datanodeLoad     map[string]int // endpoint -> #blocks it has
	datanodeLoadLock *sync.Mutex
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
			storageInfo:    make(FileStorageInfo),
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
				storageInfo: make(FileStorageInfo),
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
				inode.storageInfo[blockMetadata.BlockIndex] = BlockStorageInfo{
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
			inode.storageInfo[blockMetadata.BlockIndex] = BlockStorageInfo{
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

	blockCount := len(blockReport.BlockMetadata)
	server.datanodeLoadLock.Lock()
	defer server.datanodeLoadLock.Unlock()

	server.datanodeLoad[blockReport.Endpoint] = blockCount

	datanodesOfSameLoad, exists := server.datanodeLoadRank.Get(blockCount)
	if exists {
		hashset := datanodesOfSameLoad.(map[string]bool)
		hashset[blockReport.Endpoint] = false // value is unused
		server.datanodeLoadRank.Put(blockCount, hashset)
	} else {
		server.datanodeLoadRank.Put(blockCount, map[string]bool{
			blockReport.Endpoint: false,
		})
	}

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

// Pick `num` datanodes to hold a new block
// TODO: optimize selection for load balancing
func (server *NameNode) pickDatanodes(num uint) []string {
	result := make([]string, 0, num)

	iter := server.datanodeLoadRank.Iterator()
	for iter.Next() {
		if uint(len(result)) == num {
			break
		}
		for endpoint := range iter.Value().(map[string]bool) {
			result = append(result, endpoint)
			if uint(len(result)) == num {
				break
			}
		}
	}

	return result
}

// GetBlockLocations handles the read request from the client,
// and returns (Block Name, DataNode Endpoint) pair for each block
// If the block doesn't exist but the file exist, the version of the block will be
// `common.MIN_VALID_VERSION_NUMBER - 1`
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

	if args.LeaseToken > 0 {
		if inode.lease.leaseToken == 0 ||
			inode.lease.leaseToken == args.LeaseToken ||
			time.Since(inode.lease.lastRenewalTimestamp) > common.LEASE_TIMEOUT {
			inode.lease.leaseToken = args.LeaseToken
			inode.lease.lastRenewalTimestamp = time.Now()

			server.inodes[args.FileName] = inode
		} else {
			slog.Error("Lease mismatch",
				"file name", args.FileName,
				"actual lease token", inode.lease.leaseToken,
				"got", args.LeaseToken)
			return errors.New("lease is owned by other client")
		}
	}

	var blockInfoList []common.BlockInfo
	for i := args.BeginBlock; i < args.EndBlock; i++ {
		storageInfo, exist := inode.storageInfo[i]
		if !exist {
			blockInfoList = append(blockInfoList, common.BlockInfo{
				Version:           common.MIN_VALID_VERSION_NUMBER - 1,
				DataNodeEndpoints: server.pickDatanodes(common.BLOCK_REPLICATION),
			})
		} else {
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

func (server *NameNode) BumpBlockVersion(args common.BlockVersionBump, unused *bool) error {
	// Ensure file exists
	server.inodeRWLock.RLock()
	oldInode, ok := server.inodes[args.FileName]
	if !ok {
		server.inodeRWLock.RUnlock()
		return errors.New(fmt.Sprint("file not found: ", args.FileName))
	}
	server.inodeRWLock.RUnlock()

	oldInode.rwlock.RLock()
	defer oldInode.rwlock.RUnlock()

	if args.LeaseToken != oldInode.lease.leaseToken {
		return errors.New(fmt.Sprint("Lease doesn't match",
			" actual lease token:", oldInode.lease.leaseToken, " got", args.LeaseToken))
	}

	newInode := oldInode
	newEntry := BlockStorageInfo{
		LatestVersion: args.Version,
		Size:          args.Size,
		DataNodes:     args.ReplicaEndpoints,
	}
	if oldEntry, ok := oldInode.storageInfo[args.BlockIndex]; ok {
		// Block exists
		if oldEntry.LatestVersion < args.Version {
			newInode.fileAttributes.Size -= oldEntry.Size
			newInode.fileAttributes.Size += newEntry.Size
			newInode.storageInfo[args.BlockIndex] = newEntry
		}
	} else {
		// New block
		newInode.fileAttributes.Size += newEntry.Size
		newInode.storageInfo[args.BlockIndex] = newEntry

		server.datanodeLoadLock.Lock()
		defer server.datanodeLoadLock.Unlock()

		for _, endpoint := range args.ReplicaEndpoints {
			// As long as the datanode reported its blocks before, the following two maps should contain it
			prevBlockCount := server.datanodeLoad[endpoint]
			prevDatanodesOfSameLoad, _ := server.datanodeLoadRank.Get(prevBlockCount)

			curBlockCount := prevBlockCount + 1
			server.datanodeLoad[endpoint] = curBlockCount

			prevHashset := prevDatanodesOfSameLoad.(map[string]bool)
			delete(prevHashset, endpoint)
			server.datanodeLoadRank.Put(prevBlockCount, prevHashset)

			curDatanodesOfSameLoad, exists := server.datanodeLoadRank.Get(curBlockCount)
			if exists {
				curHashset := curDatanodesOfSameLoad.(map[string]bool)
				curHashset[endpoint] = false // value is unused
				server.datanodeLoadRank.Put(curBlockCount, curHashset)
			} else {
				server.datanodeLoadRank.Put(curBlockCount, map[string]bool{
					endpoint: false,
				})
			}
		}
	}
	server.inodes[args.FileName] = newInode
	return nil
}

func (server *NameNode) RenewLease(args common.Lease, unused *bool) error {
	// Ensure file exists
	server.inodeRWLock.RLock()
	inode, ok := server.inodes[args.FileName]
	if !ok {
		server.inodeRWLock.RUnlock()
		return errors.New(fmt.Sprint("file not found: ", args.FileName))
	}
	server.inodeRWLock.RUnlock()

	inode.rwlock.RLock()
	defer inode.rwlock.RUnlock()

	if args.LeaseToken != inode.lease.leaseToken {
		return errors.New("lease doesn't match")
	}
	inode.lease.lastRenewalTimestamp = time.Now()
	return nil
}

func (server *NameNode) RevokeLease(args common.Lease, unused *bool) error {
	// Ensure file exists
	server.inodeRWLock.RLock()
	inode, ok := server.inodes[args.FileName]
	if !ok {
		server.inodeRWLock.RUnlock()
		return errors.New(fmt.Sprint("file not found: ", args.FileName))
	}
	server.inodeRWLock.RUnlock()

	inode.rwlock.RLock()
	defer inode.rwlock.RUnlock()

	if args.LeaseToken != inode.lease.leaseToken {
		return errors.New("lease doesn't match")
	}
	inode.lease.leaseToken = 0
	server.inodes[args.FileName] = inode

	slog.Info("Lease revoked", "file name", args.FileName)
	return nil
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
		inodes:           make(map[string]iNode),
		inodeRWLock:      new(sync.RWMutex),
		datanodeRWLock:   new(sync.RWMutex),
		datanodes:        make(map[string]DataNodeInfo),
		datanodeLoadRank: treemap.NewWithIntComparator(),
		datanodeLoad:     make(map[string]int),
		datanodeLoadLock: new(sync.Mutex),
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
