package main

import (
	"io"
	"log/slog"
	"net/rpc"
	"os"
	"path/filepath"
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
	blockMetadata := make(map[string]common.BlockMetadata)
	datanode.blockRWLock = make(map[string]*sync.RWMutex)

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		datanode.blockRWLock[path] = new(sync.RWMutex)
		blockMetadata[path] = common.BlockMetadata{
			Size: uint(info.Size()),
		}
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

func (datanode *DataNode) ReadBlock(args *common.ReadBlockRequest, response *common.ReadBlockResponse) error {
	slog.Info("ReadBlock request", "blockName", args.BlockName)

	// TODO: check if lock is needed here
	// datanode.blockRWLock[args.BlockName].RLock()
	// defer datanode.blockRWLock[args.BlockName].RUnlock()

	// Open the block file
	file, err := os.Open(args.BlockName)
	if err != nil {
		slog.Error("error opening block file", "error", err)
		return err
	}
	defer file.Close()

	// Seek to the start offset
	_, err = file.Seek(int64(args.BeginOffset), 0)
	if err != nil {
		slog.Error("error when seek to the start offset", "error", err)
		return err
	}

	// Ensure response.Data is of sufficient size
	numBytes := args.EndOffset - args.BeginOffset
	if len(response.Data) < int(numBytes) {
		response.Data = make([]byte, numBytes)
	} else {
		response.Data = response.Data[:numBytes]
	}

	// Read the block data into response.Data
	n, err := file.Read(response.Data)
	if err != nil && err != io.EOF {
		slog.Error("error reading block data", "error", err)
		return err
	}

	// Check if the number of bytes read is correct
	if uint(n) != numBytes {
		slog.Error("incorrect number of bytes read", "expected", numBytes, "actual", n)
		return io.ErrUnexpectedEOF
	}

	slog.Debug("ReadBlock succeeded", "blockName", args.BlockName, "numBytes", n)

	return nil
}
