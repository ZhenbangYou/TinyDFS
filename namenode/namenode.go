package main

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

type iNode struct {
	fileAttributes common.FileAttributes
	rwlock         *sync.RWMutex // Per-file rwlock
}

type NameNode struct {
	inodes       map[string]iNode
	globalRWLock *sync.RWMutex // The RWLock for the entire namenode
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
