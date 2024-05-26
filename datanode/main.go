package main

import (
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
func (datanode *DataNode) generateBlockReport() common.BlockReport {
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
			FileName:   fileName,
			BlockIndex: blockIndex,
			Version:    version,
			Size:       uint(info.Size()),
		})
		return nil
	})

	if err != nil {
		slog.Error("error reading directory", "error", err)
	}

	return common.BlockReport{
		Endpoint:      datanode.dataNodeEndpoint,
		BlockMetadata: blockMetadata,
	}
}

// TODO: receive response to deal with stale blocks
func (datanode *DataNode) sendBlockReport() bool {
	client, err := rpc.DialHTTP("tcp", datanode.nameNodeEndpoint)
	if err != nil {
		slog.Error("error dialing namenode", "error", err)
		return false
	}

	blockReport := datanode.generateBlockReport()
	var success bool
	err = client.Call("NameNode.ReportBlock", blockReport, &success)
	if err != nil {
		slog.Error("error sending block report to namenode", "error", err)
		return false
	} else if !success {
		slog.Error("block report rejected by namenode")
		return false
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

func (datanode *DataNode) ReadBlock(args *common.ReadBlockRequest, response *common.ReadBlockResponse) error {
	slog.Info("ReadBlock request", "file name", args.FileName, "block index", args.BlockIndex)

	// TODO: check if lock is needed here
	// datanode.blockRWLock[args.BlockName].RLock()
	// defer datanode.blockRWLock[args.BlockName].RUnlock()

	// Open the block file
	completePath := constructBlockName(args.FileName, args.BlockIndex, args.Version)
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

	slog.Debug("ReadBlock succeeded", "file name", args.FileName,
		"block index", args.BlockIndex, "bytes read", n)

	return nil
}

func (datanode *DataNode) WriteBlock(args *common.WriteBlockRequest, unused *bool) error {
	slog.Info("WriteBlock request", "block info", args)

	const TEMP_SUFFIX = ".temp"

	completePath := constructBlockName(args.FileName, args.BlockIndex, args.Version)
	tempPath := completePath + TEMP_SUFFIX
	// Write to this temp file first. After successfully writing all data, rename the file.
	file, err := os.Create(tempPath)
	if err != nil {
		slog.Error("error creating file", "error", err)
		return err
	}
	defer file.Close()

	// Seek to the start offset
	_, err = file.Seek(int64(args.BeginOffset), 0)
	if err != nil {
		slog.Error("error when seek to the start offset", "error", err)
		return err
	}

	// Write data
	_, err = file.WriteAt(args.Data, int64(args.BeginOffset))
	if err != nil {
		slog.Error("Write block error", "error", err)
		os.Remove(tempPath)
		return err
	} else {
		os.Rename(tempPath, completePath)
	}

	if len(args.RemainingEndpointsInChain) > 0 {
		// Call next node
	}

	if args.ReportToNameNode {

	}

	return nil
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
	http.Serve(listener, nil)
}
