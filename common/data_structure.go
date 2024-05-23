package common

type FileAttributes struct {
	Size uint
}

type BlockMetadata struct {
	Size uint
}

type BlockReport struct {
	Endpoint      string
	BlockMetadata map[string]BlockMetadata
}

type Heartbeat struct {
	Endpoint string
}

// Request to read a file with the specified name and block range [BeginBlock, EndBlock)
type ReadFileRequest struct {
	FileName   string
	BeginBlock uint
	EndBlock   uint
}

type ReadFileResponse struct {
	Succeeded     bool
	BlockInfoList []BlockInfo
}

type BlockInfo struct {
	BlockName         string
	DataNodeEndpoints []string
}

// Request to read a block with the specified name and byte range [BeginOffset, EndOffset)
type ReadBlockRequest struct {
	BlockName   string
	BeginOffset uint
	EndOffset   uint
}

type ReadBlockResponse struct {
	Data []byte
}

type BlockStorageInfo struct {
	LatestVersion uint     // The latest version of the block, version = 0 represents invalid
	Size          uint     // The size of the block
	DataNodes     []string // DataNodes that store the latest block
}

// maps the BlockID to BlockStorageInfo
type FileStorageInfo map[uint]BlockStorageInfo
