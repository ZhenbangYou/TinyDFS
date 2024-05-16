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
}

type DataNodeInfo struct {
	LastTimestamp time.Time
	isAlive       bool
}

type NameNode struct {
	inodes       map[string]iNode
	globalRWLock *sync.RWMutex // The RWLock for the entire namenode
	datanodes    map[string]DataNodeInfo
}

// `success` will be true iff the file doesn't exist
func (server *NameNode) Create(path string, success *bool) error {
	slog.Info("Create request", "path", path)

	server.globalRWLock.Lock()
	defer server.globalRWLock.Unlock()

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
		}
		*success = true
		return nil
	}
}

func (server *NameNode) Exists(path string, exists *bool) error {
	slog.Info("Exists request", "path", path)
	server.globalRWLock.RLock()
	defer server.globalRWLock.RUnlock()
	_, *exists = server.inodes[path]
	return nil
}

// This RPC doesn't return anything
func (server *NameNode) Delete(path string, unused *bool) error {
	slog.Info("Delete request", "path", path)

	server.globalRWLock.Lock()
	defer server.globalRWLock.Unlock()

	delete(server.inodes, path)
	return nil
}

func (server *NameNode) GetAttributes(path string, fileAttributes *common.FileAttributes) error {
	slog.Info("GetAttributes request", "path", path)
	server.globalRWLock.RLock()
	defer server.globalRWLock.RUnlock()
	*fileAttributes = server.inodes[path].fileAttributes
	return nil
}

func (server *NameNode) RegisterDataNode(dataNodeEndpoint string, success *bool) error {
	slog.Info("RegisterDataNode request", "dataNodeEndpoint", dataNodeEndpoint)

	server.globalRWLock.Lock()
	defer server.globalRWLock.Unlock()

	// TODO: Check if the datanode is already registered
	server.datanodes[dataNodeEndpoint] = DataNodeInfo{
		LastTimestamp: time.Now(),
		isAlive:       true,
	}

	*success = true
	return nil
}

func (server *NameNode) ReceiveBlockReport(blockReport common.BlockReport, success *bool) error {
	slog.Info("ReceiveBlockReport request", "dataNodeEndpoint", blockReport.Endpoint)

	server.globalRWLock.Lock()
	defer server.globalRWLock.Unlock()

	// TODO: handle block report

	*success = true
	return nil
}

func (server *NameNode) ReceiveHeartBeat(heartbeat common.Heartbeat, success *bool) error {
	slog.Info("ReceiveHeartBeat request", "dataNodeEndpoint", heartbeat.Endpoint)

	server.globalRWLock.Lock()
	defer server.globalRWLock.Unlock()

	if _, exists := server.datanodes[heartbeat.Endpoint]; !exists {
		*success = false
		slog.Error("ReceiveHeartBeat datanode not registered", "dataNodeEndpoint", heartbeat.Endpoint)
		return errors.New("datanode not registered")
	}

	// Update the last timestamp and status of the datanode
	server.datanodes[heartbeat.Endpoint] = DataNodeInfo{
		LastTimestamp: time.Now(),
		isAlive:       true,
	}

	*success = true
	return nil
}

func (server *NameNode) HeartbeatMonitor() {
	for {
		time.Sleep(common.HEARTBEAT_MONITOR_INTERVAL) // Check periodically

		server.globalRWLock.Lock()
		for endpoint, dataNode := range server.datanodes {
			if time.Since(dataNode.LastTimestamp) > common.HEARTBEAT_TIMEOUT {
				dataNode.isAlive = false
				server.datanodes[endpoint] = dataNode
				slog.Warn("DataNode timed out", "endpoint", endpoint)
			}
		}
		server.globalRWLock.Unlock()
	}
}
