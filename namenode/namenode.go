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
	heartbeatReceived bool // Whether received heartbeat during the last interval
	isAlive           bool
}

type NameNode struct {
	inodes         map[string]iNode
	inodeRWLock    *sync.RWMutex // The RWLock for the inodes
	datanodeRWLock *sync.RWMutex // The RWLock for the datanodes
	datanodes      map[string]DataNodeInfo
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

func (server *NameNode) ReportBlock(blockReport common.BlockReport, success *bool) error {
	slog.Info("ReportBlock request", "dataNodeEndpoint", blockReport.Endpoint)

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

	// TODO: handle block report

	*success = true
	return nil
}

func (server *NameNode) Heartbeat(heartbeat common.Heartbeat, success *bool) error {
	slog.Info("Receive Heartbeat", "dataNodeEndpoint", heartbeat.Endpoint)

	server.inodeRWLock.Lock()
	defer server.inodeRWLock.Unlock()

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
