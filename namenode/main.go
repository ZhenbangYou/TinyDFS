package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/go-redis/redis/v8"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

var ctx = context.Background()

type blockStorageInfo struct {
	LatestVersion uint     // The latest version of the block, version = 0 represents invalid
	Size          uint     // The size of the block
	DataNodes     []string // Endpoints of datanodes storing the block of the latest version
}

type internalLeaseInfo struct {
	leaseToken           uint64
	lastRenewalTimestamp time.Time
}

type iNode struct {
	rwlock      *sync.RWMutex // Per-file rwlock
	storageInfo []blockStorageInfo
	lease       internalLeaseInfo
}

type dataNodeInfo struct {
	heartbeatReceived bool // Whether received heartbeat during the last interval
	isAlive           bool
}

type NameNode struct {
	inodes      map[string]iNode
	inodeRWLock *sync.RWMutex // The RWLock for all the inodes

	datanodes      map[string]dataNodeInfo
	datanodeRWLock *sync.RWMutex // The RWLock for the datanodes

	datanodeLoadRank *treemap.Map   // block count (int) -> hashset of endpoints
	datanodeLoad     map[string]int // endpoint -> #blocks it has
	datanodeLoadLock *sync.Mutex

	rdb *redis.Client
	// structure of the Redis DB:
	// One Redis set: filenames -- the set of all file names
	// For each filename, one Redis hash: block index -> version

	numDataNodes         uint
	blockReportsReceived map[string]bool // hashset of block reports received during initialization
}

const FILE_NAME_SET_KEY = "fileNames"

// `success` will be true iff the file doesn't exist
func (server *NameNode) Create(fileName string, success *bool) error {
	slog.Info("Create request", "path", fileName)

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

	_, exist := server.inodes[fileName]
	if exist {
		*success = false
		return errors.New("file exists")
	} else {
		server.inodes[fileName] = iNode{
			rwlock:      new(sync.RWMutex),
			storageInfo: nil,
		}
		err := server.rdb.SAdd(ctx, FILE_NAME_SET_KEY, fileName).Err()
		if err != nil {
			slog.Error("Redis SADD", "error", err)
			return err
		}
		*success = true
		return nil
	}
}

func (server *NameNode) Exists(fileName string, exists *bool) error {
	slog.Info("Exists request", "path", fileName)
	server.inodeRWLock.RLock()
	defer server.inodeRWLock.RUnlock()
	_, *exists = server.inodes[fileName]
	return nil
}

// TODO: notify all the datanodes to delete the blocks
// This RPC doesn't return anything
func (server *NameNode) Delete(fileName string, unused *bool) error {
	slog.Info("Delete request", "path", fileName)

	server.inodeRWLock.Lock()
	inode, exists := server.inodes[fileName]
	delete(server.inodes, fileName)
	defer server.inodeRWLock.Unlock()

	if exists {
		pipe := server.rdb.TxPipeline()
		err := pipe.SRem(ctx, FILE_NAME_SET_KEY, fileName).Err()
		if err != nil {
			slog.Error("Redis SREM", "error", err)
			return err
		}
		err = pipe.Del(ctx, fileName).Err()
		if err != nil {
			slog.Error("Redis DEL", "error", err)
			return err
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			slog.Error("Redis Transaction", "error", err)
		}

		for blockIndex, blockInfo := range inode.storageInfo {
			for _, endpoint := range blockInfo.DataNodes {
				datanodeConn, err := rpc.DialHTTP("tcp", endpoint)
				if err != nil {
					slog.Error("error dialing datanode", "endpoint", datanodeConn)
					// Even if it fails, don't let client know since the future block report
					// from the same datanode will resolve this
					continue
				}

				// No need to wait, just let them run
				go func(fileName string, blockIndex uint, version uint) {
					var unused bool
					datanodeConn.Call("DataNode.DeleteBlock", common.BlockIdentifier{
						FileName:   fileName,
						BlockIndex: blockIndex,
						Version:    version,
					}, &unused)
				}(fileName, uint(blockIndex), blockInfo.LatestVersion)
			}
		}
	}

	return nil
}

func (server *NameNode) GetSize(fileName string, size *uint) error {
	if len(server.blockReportsReceived)-1+common.BLOCK_REPLICATION < int(server.numDataNodes) {
		return errors.New("namenode waiting for more block reports for initialization")
	}

	slog.Info("GetSize request", "path", fileName)
	server.inodeRWLock.RLock()
	inode, ok := server.inodes[fileName]
	server.inodeRWLock.RUnlock()

	if !ok {
		return errors.New("file doesn't exist")
	}

	if len(inode.storageInfo) == 0 {
		*size = 0
	} else {
		*size = uint((len(inode.storageInfo)-1)*common.BLOCK_SIZE) +
			inode.storageInfo[len(inode.storageInfo)-1].Size
	}

	return nil
}

func (server *NameNode) RegisterDataNode(dataNodeEndpoint string, success *bool) error {
	slog.Info("RegisterDataNode request", "dataNodeEndpoint", dataNodeEndpoint)

	server.datanodeRWLock.Lock()
	defer server.datanodeRWLock.Unlock()

	server.datanodes[dataNodeEndpoint] = dataNodeInfo{
		heartbeatReceived: true,
		isAlive:           true,
	}

	*success = true
	return nil
}

// NameNode receives block report from DataNode,
// and updates all block storage info in the corresponding inode
func (server *NameNode) ReportBlock(
	args common.BlockReportRequest,
	reply *common.BlockReportResponse,
) error {
	slog.Info("ReportBlock request", "dataNodeEndpoint", args.Endpoint,
		slog.Any("Block MetaData", args.BlockMetadata))

	server.blockReportsReceived[args.Endpoint] = false

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
				if storageNode != args.Endpoint {
					newStorageNodes = append(newStorageNodes, storageNode)
				}
			}
			storageInfo.DataNodes = newStorageNodes
			inode.storageInfo[blockIndex] = storageInfo
		}
		inode.rwlock.Unlock()
	}

	reply = new(common.BlockReportResponse)

	for _, blockMetadata := range args.BlockMetadata {
		inode, exists := server.inodes[blockMetadata.BlockID.FileName]
		stale := true
		if exists {
			inode.rwlock.Lock()

			// Update the storage info
			if blockMetadata.BlockID.BlockIndex < uint(len(inode.storageInfo)) {
				// Block info exists in namenode

				storageInfo := inode.storageInfo[blockMetadata.BlockID.BlockIndex]
				if blockMetadata.BlockID.Version == storageInfo.LatestVersion {
					// Find a new replica
					storageInfo.DataNodes = append(storageInfo.DataNodes, args.Endpoint)
					inode.storageInfo[blockMetadata.BlockID.BlockIndex] = storageInfo
					slog.Debug("Append storage info",
						"file name", blockMetadata.BlockID.FileName,
						"block index", blockMetadata.BlockID.BlockIndex,
						slog.Any("storageInfo", inode.storageInfo[blockMetadata.BlockID.BlockIndex]))

					stale = false
				}
			}
			inode.rwlock.Unlock()
		}
		if stale {
			reply.BlockIDs = append(reply.BlockIDs, blockMetadata.BlockID)
		}
	}

	slog.Info("ReportBlock", "dataNodeEndpoint", args.Endpoint)

	blockCount := len(args.BlockMetadata)
	server.datanodeLoadLock.Lock()
	defer server.datanodeLoadLock.Unlock()

	server.datanodeLoad[args.Endpoint] = blockCount

	datanodesOfSameLoad, exists := server.datanodeLoadRank.Get(blockCount)
	if exists {
		hashset := datanodesOfSameLoad.(map[string]bool)
		hashset[args.Endpoint] = false // value is unused
		server.datanodeLoadRank.Put(blockCount, hashset)
	} else {
		server.datanodeLoadRank.Put(blockCount, map[string]bool{
			args.Endpoint: false,
		})
	}

	return nil
}

// NameNode checks if blocks are under-replicated and schedules replication
func (server *NameNode) replicationMonitor() {
	slog.Info("Replication monitor setup")

	for {
		time.Sleep(common.REPLICATION_MONITOR_INTERVAL) // Check periodically
		slog.Info("Replication monitor running")

		for fileName, inode := range server.inodes {
			inode.rwlock.RLock()
			for blockIndex, storageInfo := range inode.storageInfo {
				if len(storageInfo.DataNodes) < common.BLOCK_REPLICATION &&
					storageInfo.LatestVersion >= common.MIN_VALID_VERSION_NUMBER {

					// Under-replicated block
					slog.Info("Under-replicated block", "fileName", fileName, "blockIndex", blockIndex)

					// Pick an alive datanode holding the block to replicate
					var aliveStorageNodes []string
					for _, storageNode := range storageInfo.DataNodes {
						if server.datanodes[storageNode].isAlive {
							aliveStorageNodes = append(aliveStorageNodes, storageNode)
						}
					}
					if len(aliveStorageNodes) == 0 {
						slog.Error("No alive storage nodes available for replication", "fileName", fileName, "blockIndex", blockIndex)
						continue
					}
					dataNodeEndpoint := aliveStorageNodes[rand.Intn(len(aliveStorageNodes))]

					// Pick datanodes that don't hold the block to replicate
					replicaEndpoints := server.pickDatanodes(
						uint(common.BLOCK_REPLICATION-len(storageInfo.DataNodes)),
						storageInfo.DataNodes,
					)

					// Schedule replication: send a replication request to the datanode
					go func(fileName string, blockIndex uint, replicaEndpoints []string, storageInfo blockStorageInfo, dataNodeEndpoint string) {
						server.datanodeRWLock.RLock()
						defer server.datanodeRWLock.RUnlock()

						createReplicationRequest := common.CreateReplicationRequest{
							FileName:         fileName,
							BlockIndex:       blockIndex,
							Version:          storageInfo.LatestVersion,
							ReplicaEndpoints: replicaEndpoints,
						}

						dataNodeClient, err := rpc.DialHTTP("tcp", dataNodeEndpoint)
						if err != nil {
							slog.Error("dialing datanode failed", "datanode endpoint", dataNodeEndpoint, "error", err)
							return
						}

						replicateBlockTimeout :=
							time.Millisecond*(common.BLOCK_SIZE*1024/common.NETWORK_BANDWIDTH)*3 +
								common.RPC_TIMEOUT

						slog.Info("Send replication request", "fileName", fileName, "blockIndex", blockIndex, "replicaEndpoints", replicaEndpoints, "dataNodeEndpoint", dataNodeEndpoint)
						slog.Debug("create", slog.Any("createReplicationRequest", createReplicationRequest))

						var unused bool

						asyncRpcCall := dataNodeClient.Go("DataNode.CreateReplication", createReplicationRequest, &unused, nil)

						select {
						case <-asyncRpcCall.Done:
							if asyncRpcCall.Error != nil {
								slog.Error("Error during Create Replication request", "error", asyncRpcCall.Error)
							} else {
								// Update the storage info
								inode.rwlock.Lock()
								storageInfo.DataNodes = append(storageInfo.DataNodes, replicaEndpoints...)
								inode.storageInfo[blockIndex] = storageInfo
								inode.rwlock.Unlock()
								server.inodes[fileName] = inode
								slog.Info("Replication request success", "fileName", fileName, "blockIndex", blockIndex, "replicaEndpoints", replicaEndpoints)
							}
						case <-time.After(replicateBlockTimeout):
							slog.Error("Replication request timeout", "fileName", fileName, "blockIndex", blockIndex, "replicaEndpoints", replicaEndpoints)
						}

					}(fileName, uint(blockIndex), replicaEndpoints, storageInfo, dataNodeEndpoint)

				}
			}
			inode.rwlock.RUnlock()
		}
		slog.Info("Replication monitor finished")
	}
}

func (server *NameNode) Heartbeat(heartbeat common.Heartbeat, success *bool) error {
	slog.Info("Receive Heartbeat", "dataNodeEndpoint", heartbeat.Endpoint)

	server.datanodeRWLock.Lock()
	defer server.datanodeRWLock.Unlock()

	// If the datanode is not registered, ignore the heartbeat
	if _, exists := server.datanodes[heartbeat.Endpoint]; !exists {
		*success = false
		slog.Error("Heartbeat datanode not registered", "dataNodeEndpoint", heartbeat.Endpoint)
		return errors.New("datanode not registered")
	}

	// Update the status of the datanode
	server.datanodes[heartbeat.Endpoint] = dataNodeInfo{
		heartbeatReceived: true,
		isAlive:           true,
	}

	*success = true
	return nil
}

func (server *NameNode) heartbeatMonitor() {
	slog.Info("Heartbeat monitor setup")
	for {
		time.Sleep(common.HEARTBEAT_MONITOR_INTERVAL) // Check periodically

		server.datanodeRWLock.Lock()
		for endpoint, dataNode := range server.datanodes {
			if !dataNode.heartbeatReceived {
				// The datanode hasn't sent a heartbeat
				slog.Info("DataNode missed heartbeat", "dataNodeEndpoint", endpoint)
				server.datanodes[endpoint] = dataNodeInfo{
					heartbeatReceived: false,
					isAlive:           false,
				}
			} else {
				server.datanodes[endpoint] = dataNodeInfo{
					heartbeatReceived: false,
					isAlive:           true,
				}
			}
		}
		server.datanodeRWLock.Unlock()
	}
}

// Pick `num` datanodes to hold a new block
// Optimized selection for load balancing
func (server *NameNode) pickDatanodes(num uint, ignore []string) []string {
	result := make([]string, 0, num)

	iter := server.datanodeLoadRank.Iterator()
	for iter.Next() {
		if uint(len(result)) == num {
			break
		}
		for endpoint, _ := range iter.Value().(map[string]bool) {
			// ignore datanode that is down
			if !server.datanodes[endpoint].isAlive {
				continue
			}
			// if ignore list is provided, ignore datanode in the ignore list
			if ignore != nil && slices.Contains(ignore, endpoint) {
				continue
			}
			result = append(result, endpoint)
			if uint(len(result)) == num {
				break
			}
		}
	}

	if uint(len(result)) < num {
		slog.Warn("Not enough available DataNodes")
	}

	return result
}

// GetBlockLocations handles the read request from the client,
// and returns (Block Name, DataNode Endpoint) pair for each block
// If the block doesn't exist but the file exist, the version of the block will be
// `common.MIN_VALID_VERSION_NUMBER - 1`
func (server *NameNode) GetBlockLocations(args *common.GetBlockLocationsRequest, reply *common.GetBlockLocationsResponse) error {
	if len(server.blockReportsReceived)-1+common.BLOCK_REPLICATION < int(server.numDataNodes) {
		return errors.New("namenode waiting for more block reports for initialization")
	}

	slog.Info("GetBlockLocations", "file", args.FileName)

	server.inodeRWLock.RLock()
	inode, exists := server.inodes[args.FileName]
	server.inodeRWLock.RUnlock()

	if !exists {
		slog.Error("file not found", "file", args.FileName)
		return errors.New("file not found")
	}

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
		if !(i < uint(len(inode.storageInfo))) {
			blockInfoList = append(blockInfoList, common.BlockInfo{
				Version:           common.MIN_VALID_VERSION_NUMBER - 1,
				DataNodeEndpoints: server.pickDatanodes(common.BLOCK_REPLICATION, nil),
			})
		} else {
			// Filter out the alive storage nodes
			var hasDeadNode bool = false
			var aliveStorageNodes []string
			server.datanodeRWLock.RLock()
			for _, storageNode := range inode.storageInfo[i].DataNodes {
				if server.datanodes[storageNode].isAlive {
					aliveStorageNodes = append(aliveStorageNodes, storageNode)
				} else {
					hasDeadNode = true
				}
			}
			server.datanodeRWLock.RUnlock()

			// Update the storage info if needed
			if hasDeadNode {
				inode.rwlock.RUnlock()
				inode.rwlock.Lock()
				inode.storageInfo[i].DataNodes = aliveStorageNodes
				inode.rwlock.Unlock()
				inode.rwlock.RLock()
			}
			// // Randomly select an alive storage node
			// if len(aliveStorageNodes) == 0 {
			// 	slog.Error("no alive DataNode")
			// 	return errors.New("no alive DataNode")
			// }
			// dataNode := aliveStorageNodes[rand.Intn(len(aliveStorageNodes))]

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

func (server *NameNode) BumpBlockVersion(args common.BlockVersionBump, unused *bool) error {
	// Ensure file exists
	server.inodeRWLock.RLock()
	oldInode, ok := server.inodes[args.BlockID.FileName]
	if !ok {
		server.inodeRWLock.RUnlock()
		return errors.New(fmt.Sprint("file not found: ", args.BlockID.FileName))
	}
	server.inodeRWLock.RUnlock()

	oldInode.rwlock.RLock()
	defer oldInode.rwlock.RUnlock()

	if args.LeaseToken != oldInode.lease.leaseToken {
		return errors.New(fmt.Sprint("Lease doesn't match",
			" actual lease token:", oldInode.lease.leaseToken, " got", args.LeaseToken))
	}

	newInode := oldInode
	newEntry := blockStorageInfo{
		LatestVersion: args.BlockID.Version,
		Size:          args.Size,
		DataNodes:     args.ReplicaEndpoints,
	}
	if args.BlockID.BlockIndex < uint(len(oldInode.storageInfo)) {
		// Block exists
		if oldInode.storageInfo[args.BlockID.BlockIndex].LatestVersion < args.BlockID.Version {
			newInode.storageInfo[args.BlockID.BlockIndex] = newEntry
			err := server.rdb.HSet(ctx, args.BlockID.FileName, args.BlockID.BlockIndex, args.BlockID.Version).Err()
			if err != nil {
				slog.Error("Redis HSET", "error", err)
			}
		}
	} else {
		// New block
		for {
			newInode.storageInfo = append(newInode.storageInfo, blockStorageInfo{
				LatestVersion: common.MIN_VALID_VERSION_NUMBER - 1,
			})
			blockIndex := len(newInode.storageInfo) - 1
			if args.BlockID.BlockIndex < uint(len(newInode.storageInfo)) {
				// Last block to add
				err := server.rdb.HSet(ctx, args.BlockID.FileName, blockIndex, args.BlockID.Version).Err()
				if err != nil {
					slog.Error("Redis HSET", "error", err)
				}
				break
			} else {
				// Block in the middle, still in hollow state
				err := server.rdb.HSet(ctx, args.BlockID.FileName, blockIndex, common.MIN_VALID_VERSION_NUMBER-1).Err()
				if err != nil {
					slog.Error("Redis HSET", "error", err)
				}
			}
		}

		newInode.storageInfo[args.BlockID.BlockIndex] = newEntry

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
	server.inodes[args.BlockID.FileName] = newInode
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
// Args[2]: Number of DataNodes
// Args[3]: Optional log file path
func main() {
	// Check command line arguments
	if len(os.Args) < 3 || len(os.Args) > 4 {
		panic(fmt.Sprintln("expect 3 or 4 command line arguments, actual argument count", len(os.Args)))
	}

	// Read and validate Namenode endpoint
	nameNodeEndpoint := os.Args[1]
	if !common.IsValidEndpoint(nameNodeEndpoint) {
		panic(fmt.Sprintln("invalid namenode endpoint", nameNodeEndpoint))
	}

	// Set up slog to log into a file or terminal
	var logHandler slog.Handler
	var programLevel = new(slog.LevelVar) // Info by default

	if len(os.Args) == 4 {
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
		inodes:               make(map[string]iNode),
		inodeRWLock:          new(sync.RWMutex),
		datanodeRWLock:       new(sync.RWMutex),
		datanodes:            make(map[string]dataNodeInfo),
		datanodeLoadRank:     treemap.NewWithIntComparator(),
		datanodeLoad:         make(map[string]int),
		datanodeLoadLock:     new(sync.Mutex),
		rdb:                  redis.NewClient(&redis.Options{}),
		numDataNodes:         0,
		blockReportsReceived: make(map[string]bool),
	}
	slog.Info("Initialized namenode", "nameNodeEndpoint", nameNodeEndpoint)

	exists, err := server.rdb.Exists(ctx, FILE_NAME_SET_KEY).Result()
	if err != nil {
		panic(fmt.Sprintln("Redis EXISTS error", err))
	}

	// Read block version from Redis
	if exists > 0 {
		fileNames, err := server.rdb.SMembers(ctx, FILE_NAME_SET_KEY).Result()
		if err != nil {
			panic(fmt.Sprintln("Redis SMEMBERS error", err))
		}
		for _, fileName := range fileNames {
			inode := iNode{
				rwlock: new(sync.RWMutex),
				lease:  internalLeaseInfo{},
			}
			exists, err := server.rdb.Exists(ctx, fileName).Result()
			if err != nil {
				panic(fmt.Sprintln("Redis EXISTS", "error", err))
			}
			if exists > 0 {
				// if exists == 0, the file will be empty

				blocks, err := server.rdb.HGetAll(ctx, fileName).Result()
				if err != nil {
					panic(fmt.Sprintln("Redis HGETALL", "error", err))
				}
				inode.storageInfo = make([]blockStorageInfo, len(blocks))
				for blockIndexString, versionString := range blocks {
					blockIndex, err := strconv.ParseUint(blockIndexString, 10, 64)
					if err != nil {
						panic(fmt.Sprintln("block index string can't be parsed: ", blockIndexString))
					}
					version, err := strconv.ParseUint(versionString, 10, 64)
					if err != nil {
						panic(fmt.Sprintln("version string can't be parsed: ", versionString))
					}
					inode.storageInfo[blockIndex] = blockStorageInfo{
						LatestVersion: uint(version),
					}
				}
			}
			server.inodes[fileName] = inode
		}
	}

	numDataNodes, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		panic("Number of DataNodes argument can't be recognized")
	}
	server.numDataNodes = uint(numDataNodes)

	// Set up namenode RPC server
	port := strings.Split(nameNodeEndpoint, ":")[1]
	rpc.Register(&server)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+port) // Listen on all addresses
	if err != nil {
		panic(fmt.Sprintln("listen error", err))
	}
	go http.Serve(listener, nil)

	// Start the heartbeat monitor
	go server.heartbeatMonitor()

	// Start the replication monitor
	go server.replicationMonitor()

	select {}
}
