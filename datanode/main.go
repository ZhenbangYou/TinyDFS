package main

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

type DataNode struct {
	nameNodeEndpoint string
	dataNodeEndpoint string
	// added for now, only used in generateBlockReport (create locks),
	// you can remove it if you come up with a better way
	blockRWLock map[string]*sync.RWMutex
}

// Go over all the files in the subdir,
// Create locks and generate a block report for the namenode
func (datanode *DataNode) generateBlockReport() common.BlockReportRequest {
	var blockMetadata []common.BlockMetadata
	datanode.blockRWLock = make(map[string]*sync.RWMutex)

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		datanode.blockRWLock[path] = new(sync.RWMutex)

		fileName, blockIndex, version, err := parseBlockName(path)
		if err != nil {
			return err
		}

		blockMetadata = append(blockMetadata, common.BlockMetadata{
			BlockID: common.BlockIdentifier{
				FileName:   fileName,
				BlockIndex: blockIndex,
				Version:    version,
			},
			Size: uint(info.Size()),
		})
		return nil
	})

	if err != nil {
		slog.Error("error reading directory", "error", err)
	}

	return common.BlockReportRequest{
		Endpoint:      datanode.dataNodeEndpoint,
		BlockMetadata: blockMetadata,
	}
}

func (datanode *DataNode) sendBlockReport() bool {
	client, err := rpc.DialHTTP("tcp", datanode.nameNodeEndpoint)
	if err != nil {
		slog.Error("error dialing namenode", "error", err)
		return false
	}

	blockReport := datanode.generateBlockReport()
	var staleBlocks common.BlockReportResponse
	err = client.Call("NameNode.ReportBlock", blockReport, &staleBlocks)
	if err != nil {
		slog.Error("error sending block report to namenode", "error", err)
		return false
	}
	for _, blockID := range staleBlocks.BlockIDs {
		var unused bool
		datanode.DeleteBlock(blockID, &unused)
	}
	return true
}

// Register with the namenode. Return true if successful.
func (datanode *DataNode) registerWithNameNode() bool {
	slog.Info("Registering with namenode",
		"nameNodeEndpoint", datanode.nameNodeEndpoint,
		"dataNodeEndpoint", datanode.dataNodeEndpoint)

	client, err := rpc.DialHTTP("tcp", datanode.nameNodeEndpoint)
	if err != nil {
		slog.Error("error dialing namenode", "error", err)
		return false
	}

	var success bool
	err = client.Call("NameNode.RegisterDataNode", datanode.dataNodeEndpoint, &success)
	if err != nil {
		slog.Error("error registering with namenode", "error", err)
		return false
	} else if !success {
		slog.Error("registration with namenode failed")
		return false
	}
	return true
}

// This function periodically sends heartbeat to namenode.
// If heartbeat fails, retry after a random interval indefinitely,
// When starting initially or after a reconnection,
// the datanode registers with the namenode and send block report.
func (datanode *DataNode) heartbeatLoop() {
	var lastHeartbeatSucceeded bool = false
	for {
		client, err := rpc.DialHTTP("tcp", datanode.nameNodeEndpoint)
		// If failing, retry after a certain interval indefinitely
		if err != nil {
			slog.Error("error dialing namenode", "error", err)
			lastHeartbeatSucceeded = false
			time.Sleep(common.HEARTBEAT_INTERVAL) // TODO: adjust interval here
			continue
		}

		// After a reconnection, retry registration and block report
		if !lastHeartbeatSucceeded {
			if !datanode.registerWithNameNode() {
				slog.Error("Failed to register with Namenode")
				time.Sleep(common.HEARTBEAT_INTERVAL)
				continue
			} else if !datanode.sendBlockReport() {
				slog.Error("Failed to send block report")
				time.Sleep(common.HEARTBEAT_INTERVAL)
				continue
			} else {
				slog.Info("Heartbeat status prepared to change", "DataNode Endpoint", datanode.dataNodeEndpoint)
			}
		}

		heartbeat := common.Heartbeat{
			Endpoint: datanode.dataNodeEndpoint,
		}

		var success bool
		err = client.Call("NameNode.Heartbeat", heartbeat, &success)

		if err != nil {
			slog.Error("error sending heartbeat to namenode", "error", err)
			lastHeartbeatSucceeded = false
		} else if !success {
			slog.Error("heartbeat rejected by namenode")
			lastHeartbeatSucceeded = false
		} else {
			if !lastHeartbeatSucceeded {
				slog.Info("Heartbeat status changed to true", "DataNode Endpoint", datanode.dataNodeEndpoint)
			}
			lastHeartbeatSucceeded = true
		}

		time.Sleep(common.HEARTBEAT_INTERVAL)
	}
}

// BlockName format: fileName_blockID_versionID
func constructBlockName(fileName string, blockID uint, versionID uint) string {
	return fmt.Sprintf("%s_%d_%d", fileName, blockID, versionID)
}

// Parse blockName and returns fileName, blockID, versionID, and potential error
func parseBlockName(blockName string) (string, uint, uint, error) {
	parts := strings.Split(blockName, "_")
	// the last part is the versionID
	versionID, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return "", 0, 0, err
	}
	// the second last part is the blockID
	blockID, err := strconv.Atoi(parts[len(parts)-2])
	if err != nil {
		return "", 0, 0, err
	}
	// the other parts is the fileName
	fileName := strings.Join(parts[:len(parts)-2], "_")
	return fileName, uint(blockID), uint(versionID), nil
}

func (datanode *DataNode) ReadBlock(args common.ReadBlockRequest, response *common.ReadBlockResponse) error {
	slog.Info("ReadBlock request", "file name", args.BlockID.FileName, "block index", args.BlockID.BlockIndex)

	// TODO: check if lock is needed here
	// datanode.blockRWLock[args.BlockName].RLock()
	// defer datanode.blockRWLock[args.BlockName].RUnlock()

	// Open the block file
	completePath := constructBlockName(args.BlockID.FileName, args.BlockID.BlockIndex, args.BlockID.Version)
	file, err := os.Open(completePath)
	if err != nil {
		slog.Error("error opening block file", "error", err)
		return err
	}
	defer file.Close()

	// Ensure response.Data is of sufficient size
	response.Data = make([]byte, args.Length)

	// Read the block data into response.Data
	n, err := file.ReadAt(response.Data, int64(args.BeginOffset))
	if err != nil && err != io.EOF {
		slog.Error("error reading block data", "error", err)
		return err
	}
	if err == io.EOF {
		response.Data = response.Data[:n]
	}

	slog.Debug("ReadBlock succeeded", "file name", args.BlockID.FileName,
		"block index", args.BlockID.BlockIndex, "bytes read", n)

	return nil
}

// If this RPC returns nil (i.e., no error, meaning success), the block must have been persistently
// written to the distributed file system. However, even if this RPC returns an error, the write
// may still be successful, e.g., the error may be due to the lost of the reply from datanode to client
func (datanode *DataNode) WriteBlock(args common.WriteBlockRequest, unused *bool) error {
	slog.Info("WriteBlock request", "block info", args)

	curPath := constructBlockName(args.BlockID.FileName, args.BlockID.BlockIndex, args.BlockID.Version)
	tmpPath := curPath + ".tmp"
	prevPath := constructBlockName(args.BlockID.FileName, args.BlockID.BlockIndex, args.BlockID.Version-1)

	curFile, err := os.Create(tmpPath)
	if err != nil {
		slog.Error("error creating file", "error", err)
		return err
	}

	// Write data
	_, err = curFile.WriteAt(args.Data, int64(args.BeginOffset))
	if err != nil {
		slog.Error("Write block error", "error", err)
		os.Remove(tmpPath)
		return err
	}

	blockSize := args.BeginOffset + uint(len(args.Data))

	// TODO: Significant write amplification in random writes, can be improved a lot
	// Copy file from the previous version
	if args.BlockID.Version > common.MIN_VALID_VERSION_NUMBER {
		// If a previous version exists

		prevFile, err := os.Open(prevPath)
		if err != nil {
			prevFile.Close()
			curFile.Close()
			os.Remove(tmpPath)
			return err
		}

		if args.BeginOffset > 0 {
			buffer := make([]byte, args.BeginOffset)
			_, err := prevFile.Read(buffer)
			if err != nil {
				prevFile.Close()
				curFile.Close()
				os.Remove(tmpPath)
				return err
			}
			_, err = curFile.WriteAt(buffer, 0)
			if err != nil {
				prevFile.Close()
				curFile.Close()
				os.Remove(tmpPath)
				return err
			}
		}

		prevStat, err := prevFile.Stat()
		if err != nil {
			prevFile.Close()
			curFile.Close()
			os.Remove(tmpPath)
			return err
		}
		prevSize := prevStat.Size()
		CurEndOffset := args.BeginOffset + uint(len(args.Data))
		if CurEndOffset < uint(prevSize) {
			blockSize = uint(prevSize)

			lengthDiff := uint(prevSize) - CurEndOffset
			buffer := make([]byte, lengthDiff)
			_, err := prevFile.ReadAt(buffer, int64(CurEndOffset))
			if err != nil {
				prevFile.Close()
				curFile.Close()
				os.Remove(tmpPath)
				return err
			}
			_, err = curFile.WriteAt(buffer, int64(CurEndOffset))
			if err != nil {
				prevFile.Close()
				curFile.Close()
				os.Remove(tmpPath)
				return err
			}
		}
		prevFile.Close()
	}
	curFile.Close()
	os.Rename(tmpPath, curPath)

	if args.IndexInChain+1 == uint(len(args.ReplicaEndpoints)) {
		// Last one in chain, report to namenode

		client, err := rpc.DialHTTP("tcp", datanode.nameNodeEndpoint)
		if err != nil {
			slog.Error("dialing namanode error", "error", err)
			return err
		}

		bumpBlockVersionRequest := common.BlockVersionBump{
			BlockID:          args.BlockID,
			Size:             blockSize,
			ReplicaEndpoints: args.ReplicaEndpoints,
			LeaseToken:       args.LeaseToken,
		}

		var unused bool
		// Should be blocking to avoid the case where the namenode successfully update block info
		// but the reply gets lost
		err = client.Call("NameNode.BumpBlockVersion", bumpBlockVersionRequest, &unused)

		if err != nil {
			// The latest block shouldn't be removed, since the reply from the namenode may be lost,
			// i.e., even if this RPC fails, the update in the namenode may still be successful
			slog.Error("BumpBlockVersion RPC error", "error", err)
			return errors.New(
				fmt.Sprintln(
					"BumpBlockVersion RPC error", "error", err))
		} else {
			slog.Info("bump block version succeeded")
			os.Remove(prevPath)
			return nil
		}
	} else {
		// Call the next node in chain

		client, err := rpc.DialHTTP("tcp", args.ReplicaEndpoints[args.IndexInChain+1])
		if err != nil {
			slog.Error("dialing next datanode error",
				"error", err,
				"next datanode endpoint", args.ReplicaEndpoints[args.IndexInChain+1])
			return err
		}

		writeBlockRequest := args
		writeBlockRequest.IndexInChain += 1
		var unused bool
		// Should be blocking to avoid the case where the namenode and some datanodes
		// successfully update block info but the reply gets lost
		err = client.Call("DataNode.WriteBlock", &writeBlockRequest, &unused)

		if err != nil {
			// The latest block shouldn't be removed, because it's possible that the updates
			// in the namenode and some datanodes are successful but the reply doesn't come
			// back successfully
			slog.Error("Write block to next datanode error",
				"error", err,
				"next datanode endpoint", args.ReplicaEndpoints[writeBlockRequest.IndexInChain])
			return errors.New(
				fmt.Sprintln(
					"Write block to next datanode error",
					"error", err,
					"next datanode endpoint", args.ReplicaEndpoints[writeBlockRequest.IndexInChain]))
		} else {
			slog.Info("Write block to next datanode succeeded")
			os.Remove(prevPath)
			return nil
		}
	}
}

func (datanode *DataNode) CreateReplication(args *common.CreateReplicationRequest, unused *bool) error {
	slog.Info("CreateReplication request")

	curPath := constructBlockName(args.FileName, args.BlockIndex, args.Version)

	// Open the existing block file
	srcFile, err := os.Open(curPath)
	if err != nil {
		slog.Error("error opening source block file for replication", "error", err)
		return err
	}
	defer srcFile.Close()

	// Read the entire block file
	srcData, err := io.ReadAll(srcFile)
	if err != nil {
		slog.Error("error reading source block file for replication", "error", err)
		return err
	}

	// Construct the ReplicateBlockRequest
	createReplicationRequest := common.ReplicateBlockRequest{
		FileName:         args.FileName,
		BlockIndex:       args.BlockIndex,
		Version:          args.Version,
		Data:             srcData,
		ReplicaEndpoints: args.ReplicaEndpoints,
		IndexInChain:     0,
	}

	// Send the block to the first datanode in the list
	client, err := rpc.DialHTTP("tcp", args.ReplicaEndpoints[0])
	if err != nil {
		slog.Error("dialing the first datanode error",
			"error", err,
			"first datanode endpoint", args.ReplicaEndpoints[0])
		return err
	}

	err = client.Call("DataNode.ReplicateBlock", &createReplicationRequest, &unused)
	if err != nil {
		slog.Error("ReplicateBlock RPC error", "error", err)
		return err
	}

	return nil
}

func (datanode *DataNode) ReplicateBlock(args *common.ReplicateBlockRequest, unused *bool) error {
	slog.Info("ReplicateBlock request", "block info", args)

	curPath := constructBlockName(args.FileName, args.BlockIndex, args.Version)
	curFile, err := os.Create(curPath)
	if err != nil {
		slog.Error("error creating file", "error", err)
		return err
	}

	// Write data
	_, err = curFile.WriteAt(args.Data, 0)
	if err != nil {
		slog.Error("Write block error", "error", err)
		os.Remove(curPath)
		return err
	}
	curFile.Close()

	if args.IndexInChain+1 == uint(len(args.ReplicaEndpoints)) {
		slog.Info("Last datanode in chain, replication succeeded")
		return nil
	} else {
		// Call the next node in chain

		client, err := rpc.DialHTTP("tcp", args.ReplicaEndpoints[args.IndexInChain+1])
		if err != nil {
			slog.Error("dialing next datanode error",
				"error", err,
				"next datanode endpoint", args.ReplicaEndpoints[args.IndexInChain+1])
			return err
		}

		replicateBlockRequest := args
		replicateBlockRequest.IndexInChain += 1
		var unused bool
		err = client.Call("DataNode.ReplicateBlock", &replicateBlockRequest, &unused)

		if err != nil {
			slog.Error("Replicate block to next datanode error",
				"error", err,
				"next datanode endpoint", args.ReplicaEndpoints[replicateBlockRequest.IndexInChain])
			return errors.New(
				fmt.Sprintln(
					"Replicate block to next datanode error",
					"error", err,
					"next datanode endpoint", args.ReplicaEndpoints[replicateBlockRequest.IndexInChain]))
		} else {
			slog.Info("Replicate block to next datanode succeeded")
			return nil
		}
	}
}

func (datanode *DataNode) DeleteBlock(args common.BlockIdentifier, unused *bool) error {
	slog.Info("Delete Block request", "block", args)
	path := constructBlockName(args.FileName, args.BlockIndex, args.Version)
	err := os.Remove(path)
	return err
}

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return destFile.Sync()
}

func (datanode *DataNode) TruncateBlock(args common.TruncateBlockRequest, unused *bool) error {
	slog.Info("Truncate Block request", "block", args, "new block length", args.NewBlockLength)
	oldPath := constructBlockName(args.BlockID.FileName, args.BlockID.BlockIndex, args.BlockID.Version)
	newPath := constructBlockName(args.BlockID.FileName, args.BlockID.BlockIndex, args.BlockID.Version+1)
	err := copyFile(oldPath, newPath)
	if err != nil {
		return err
	}
	err = os.Truncate(newPath, int64(args.NewBlockLength))
	return err
}

// Command Line Args:
// Args[1]: Namenode endpoint (IP:port)
// Args[2]: Datanode endpoint (IP:port)
// Args[3]: Optional log file path
func main() {
	// Check command line arguments
	if len(os.Args) < 3 || len(os.Args) > 4 {
		panic(fmt.Sprintln("expect 3 or 4 command line arguments, actual argument count", len(os.Args)))
	}

	// Read and validate Namenode endpoint
	nameNodeEndpoint := os.Args[1]
	if !common.IsValidEndpoint(nameNodeEndpoint) {
		panic(fmt.Sprintln("invalid namenode endpoint, endpoint", nameNodeEndpoint))
	}

	// Read and validate Datanode endpoint
	dataNodeEndpoint := os.Args[2]
	if !common.IsValidEndpoint(dataNodeEndpoint) {
		panic(fmt.Sprintln("invalid datanode endpoint, endpoint", dataNodeEndpoint))
	}

	// Set up slog to log into a file or terminal
	var logHandler slog.Handler
	var programLevel = new(slog.LevelVar) // Info by default

	if len(os.Args) == 4 {
		logFilePath := os.Args[3]
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

	// Initialize DataNode
	dataNode := DataNode{
		nameNodeEndpoint: nameNodeEndpoint,
		dataNodeEndpoint: dataNodeEndpoint,
	}
	slog.Info("Initialized datanode", "NameNode Endpoint", nameNodeEndpoint, "DataNode Endpoint", dataNodeEndpoint)

	// Start heartbeat loop,
	// this also register the datanode with the namenode and send block report
	go dataNode.heartbeatLoop()

	// Set up Datanode RPC server
	dataNodePort := strings.Split(dataNodeEndpoint, ":")[1]
	rpc.Register(&dataNode)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+dataNodePort) // Listen on all addresses
	if err != nil {
		panic(fmt.Sprintln("Failed to start DataNode RPC server, listen error", err))
	}
	go http.Serve(listener, nil)

	select {}
}
