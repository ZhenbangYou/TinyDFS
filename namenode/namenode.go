package main

import (
	"errors"
	"log/slog"
	"sync"

	"github.com/ZhenbangYou/TinyDFS/tiny-dfs/common"
)

type iNode struct {
	metadata common.FileMetadata
	rwlock   *sync.RWMutex // Per-file rwlock
}

type NameNode struct {
	inodes       map[string]iNode
	globalRWLock *sync.RWMutex // The RWLock for the entire namenode
}

func (server *NameNode) Create(path string, fileHandle *common.FileHandle) error {
	slog.Info("Create request", "path", path)

	server.globalRWLock.Lock()
	defer server.globalRWLock.Unlock()

	_, exist := server.inodes[path]
	if exist {
		return errors.New("file exists")
	} else {
		metadata := common.FileMetadata{
			Size: 0,
		}
		server.inodes[path] = iNode{
			metadata: metadata,
			rwlock:   new(sync.RWMutex),
		}
		*fileHandle = common.FileHandle{
			Path:     path,
			MetaData: metadata,
		}
		return nil
	}
}
