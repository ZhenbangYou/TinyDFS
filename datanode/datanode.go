package main

import (
	"log/slog"
	"math/rand"
	"net/rpc"
	"time"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

type DataNode struct {
	nameNodeEndpoint string
	dataNodeEndpoint string
}

func (datanode *DataNode) registerWithNameNode() bool {
	slog.Info("Registering with namenode",
		"nameNodeEndpoint", datanode.nameNodeEndpoint,
		"dataNodeEndpoint", datanode.dataNodeEndpoint)

	// retry if failed
	for {
		client, err := rpc.DialHTTP("tcp", datanode.nameNodeEndpoint)
		if err != nil {
			slog.Error("error dialing namenode", "error", err)
			// retry after a random interval
			delay := common.RETRY_MIN_INTERVAL + time.Duration(rand.Int63n(int64(common.RETRY_MAX_INTERVAL-common.RETRY_MIN_INTERVAL)))
			time.Sleep(delay)
			continue
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
}

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

func (datanode *DataNode) heartbeatLoop() {
	var lastHeartbeatSucceeded bool = true
	for {
		client, err := rpc.DialHTTP("tcp", datanode.nameNodeEndpoint)
		if err != nil {
			slog.Error("error dialing namenode", "error", err)
			lastHeartbeatSucceeded = false
			time.Sleep(common.HEARTBEAT_INTERVAL) // TODO: adjust interval here
			continue
		}

		heartbeat := common.Heartbeat{
			Endpoint: datanode.dataNodeEndpoint,
		}

		var success bool
		err = client.Call("NameNode.Heartbeat", heartbeat, &success)

		if err != nil || !success {
			slog.Error("error sending heartbeat to namenode", "error", err)
			lastHeartbeatSucceeded = false
		} else {
			if !lastHeartbeatSucceeded {
				slog.Info("HeartBeat status prepared to change, sending block report")
				if !datanode.sendBlockReport() {
					slog.Error("Failed to send block report")
				} else {
					lastHeartbeatSucceeded = true
					slog.Info("HeartBeat status changed to true")
				}
			}
		}

		time.Sleep(common.HEARTBEAT_INTERVAL)
	}
}

func (datanode *DataNode) generateBlockReport() common.BlockReport {
	// TODO: go over all the files in the subdir
	// generate (block_name, version_number) pairs
	// return the map
	return common.BlockReport{
		Endpoint:      datanode.dataNodeEndpoint,
		BlockMetadata: make(map[string]common.BlockMetadata),
	}
}
