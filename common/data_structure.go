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

type ReadFileRequest struct {
	FileName   string
	BlockRange [2]uint
}

type ReadFileResponse struct {
	Succeeded     bool
	BlockInfoList []BlockInfo
}

type BlockInfo struct {
	BlockName         string
	DataNodeEndpoints []string
}

type ReadBlockRequest struct {
	BlockName  string
	ByteOffset [2]uint
}

type ReadBlockResponse struct {
	Data []byte
}

// The latest version of the block, the size of the block,
// and the DataNodes that store the latest block
// Version = 0 represents invalid
type BlockStorageInfo struct {
	LatestVersion uint
	Size          uint
	DataNodes     []string
}

// maps the BlockID to BlockStorageInfo
type FileStorageInfo map[uint]BlockStorageInfo
