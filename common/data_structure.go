package common

type FileAttributes struct {
	Size uint
}

type BlockMetadata struct {
	FileName   string
	BlockIndex uint
	Version    uint
	Size       uint
}

type BlockReport struct {
	Endpoint      string
	BlockMetadata []BlockMetadata
}

type Heartbeat struct {
	Endpoint string
}

// Request to read a file with the specified name and block range [BeginBlock, EndBlock)
type GetBlockLocationsRequest struct {
	FileName   string
	BeginBlock uint
	EndBlock   uint
}

type GetBlockLocationsResponse struct {
	BlockInfoList []BlockInfo
}

type BlockInfo struct {
	Version           uint
	DataNodeEndpoints []string
}

// Request to read a block with the specified name and byte range [BeginOffset, EndOffset)
type ReadBlockRequest struct {
	FileName    string
	BlockIndex  uint
	Version     uint
	BeginOffset uint
	EndOffset   uint
}

type ReadBlockResponse struct {
	Data []byte
}

type WriteBlockRequest struct {
	FileName    string
	BlockIndex  uint
	Version     uint
	BeginOffset uint
	EndOffset   uint

	// All datanodes holding a block form a Chain Replication, hence the name
	RemainingEndpointsInChain []string

	// If true, the datanode receiving this request should report to the namenode on success
	ReportToNameNode bool
}
